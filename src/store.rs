//! The store.
//!
//! Four keyspaces:
//!
//! ```text
//! cas    32B hash                -> chunk bytes    kv-separated, Lz4
//! blobs  16B id ++ 4B seq (BE)   -> see below      plain
//! refs   32B hash ++ 16B id      -> ()             plain
//! roots  32B root ++ 16B id      -> ()             plain
//! ```
//!
//! `blobs` holds one entry per chunk plus two markers. Seq 0 is the header,
//! carrying the content type, which is all that is known when ingest opens.
//! Seq 1..n are chunk hashes in order. Seq u32::MAX is the trailer, holding
//! what only EOF can tell you: total size, chunk count, and the BLAKE3 root
//! of the content. A blob is complete exactly when its trailer exists, and
//! because u32::MAX sorts last, a prefix scan finds out by reading to the end
//! of the blob it was already reading.
//!
//! `refs` is why deletion is possible. Chunks are shared between blobs, so
//! removing a blob may not remove its chunks. One key per (chunk, blob) pair
//! turns "is this chunk still wanted" into a prefix scan.
//!
//! `roots` maps content back to the handles holding it. It is a multimap on
//! purpose: ingesting the same bytes twice is legitimate and cheap, since
//! every chunk already hits in `cas`.

use anyhow::{bail, Context, Result};
use fjall::{
    config::PinningPolicy, CompressionType, Database, Keyspace, KeyspaceCreateOptions,
    KvSeparationOptions,
};
use scru128::Scru128Id;
use serde::{Deserialize, Serialize};
use std::io::{Read, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use crate::ident::{self, Hash, Ref};

pub const CHUNK_SIZE: usize = 64 * 1024;
const TRAILER_SEQ: u32 = u32::MAX;
pub const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";

/// Written once, at EOF. Everything here is unknowable when the blob id is
/// handed out, which is the whole reason it is a trailer and not a header.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trailer {
    pub size: u64,
    pub chunks: u32,
    /// BLAKE3 of the content, base32. Plain BLAKE3 of the bytes, not a hash
    /// over the chunk list, so it does not move if CHUNK_SIZE ever changes
    /// and anything else can recompute it.
    pub root: String,
}

/// A blob being written. Holds the partial chunk, the running BLAKE3 of the
/// content, and where we are in the sequence.
pub struct Ingest {
    pub id: Scru128Id,
    hasher: blake3::Hasher,
    pending: Vec<u8>,
    seq: u32,
    size: u64,
}

#[derive(Debug, Clone)]
pub struct Blob {
    pub id: Scru128Id,
    pub content_type: String,
    pub chunks: Vec<Hash>,
    pub trailer: Option<Trailer>,
}

impl Blob {
    pub fn complete(&self) -> bool {
        self.trailer.is_some()
    }
    pub fn size(&self) -> Option<u64> {
        self.trailer.as_ref().map(|t| t.size)
    }
    pub fn status(&self) -> &'static str {
        if self.complete() {
            "complete"
        } else {
            "ingesting"
        }
    }
}

pub struct Store {
    path: std::path::PathBuf,
    db: Database,
    cas: Keyspace,
    blobs: Keyspace,
    refs: Keyspace,
    roots: Keyspace,
}

fn entry_key(id: &Scru128Id, seq: u32) -> [u8; 20] {
    let mut k = [0u8; 20];
    k[..16].copy_from_slice(&id.to_bytes());
    k[16..].copy_from_slice(&seq.to_be_bytes());
    k
}

fn pair_key(hash: &Hash, id: &Scru128Id) -> [u8; 48] {
    let mut k = [0u8; 48];
    k[..32].copy_from_slice(hash);
    k[32..].copy_from_slice(&id.to_bytes());
    k
}

impl Store {
    pub fn open(path: impl AsRef<Path>, cache_mb: u64) -> Result<Self> {
        let db = Database::builder(path.as_ref())
            .cache_size(cache_mb * 1024 * 1024)
            .open()
            .map_err(|e| match e {
                fjall::Error::Locked => anyhow::anyhow!(
                    "store {} is locked by another klump process.\n\
                     fjall gives one process exclusive access, so a reader cannot \
                     attach while a writer holds it.",
                    path.as_ref().display()
                ),
                other => anyhow::Error::new(other).context("opening store"),
            })?;

        // Chunks are the only large values, so this is the only keyspace that
        // wants key-value separation: the LSM holds 32-byte keys and a value
        // handle, and compaction moves those rather than the payload.
        //
        // Filters are pinned at every level, not just L0. The default leaves
        // deeper filters in the shared block cache, and once the tree
        // compacts down to one big table its filter is too large to survive
        // there. Measured on fjall 3.1.10 at 1M chunks that turns a 271ns
        // "do I have this chunk" into 72us, which is worse than the write it
        // exists to avoid. Pinning costs ~1.25 MB per million chunks and
        // lowers total RSS, because the filter stops evicting everything else.
        let cas = db.keyspace("cas", || {
            KeyspaceCreateOptions::default()
                .with_kv_separation(Some(
                    KvSeparationOptions::default().compression(CompressionType::Lz4),
                ))
                .filter_block_pinning_policy(PinningPolicy::all(true))
        })?;

        let small = || KeyspaceCreateOptions::default();
        let blobs = db.keyspace("blobs", small)?;
        let refs = db.keyspace("refs", small)?;
        let roots = db.keyspace("roots", small)?;

        Ok(Self { path: path.as_ref().to_path_buf(), db, cas, blobs, refs, roots })
    }

    // --- ingest ---------------------------------------------------------

    /// Open a blob and hand back its handle before any content exists.
    ///
    /// This is the whole point of the design: the id is knowable at open, the
    /// root is not, so a subscriber can be told where to look while the bytes
    /// are still arriving.
    pub fn begin(&self, content_type: &str) -> Result<Ingest> {
        let id = scru128::new();
        let ct = if content_type.trim().is_empty() {
            DEFAULT_CONTENT_TYPE
        } else {
            content_type
        };
        self.blobs.insert(entry_key(&id, 0), ct.as_bytes())?;
        Ok(Ingest {
            id,
            hasher: blake3::Hasher::new(),
            pending: Vec::with_capacity(CHUNK_SIZE),
            seq: 1,
            size: 0,
        })
    }

    /// Take more bytes. Chunk boundaries are fixed offsets into the content,
    /// never wherever a read happened to land, so the same bytes always chunk
    /// the same way whether they arrived in one write or a thousand.
    pub fn append(&self, ing: &mut Ingest, mut bytes: &[u8]) -> Result<()> {
        while !bytes.is_empty() {
            let want = CHUNK_SIZE - ing.pending.len();
            let take = want.min(bytes.len());
            ing.pending.extend_from_slice(&bytes[..take]);
            bytes = &bytes[take..];
            if ing.pending.len() == CHUNK_SIZE {
                self.commit_chunk(ing)?;
            }
        }
        Ok(())
    }

    /// Seal the blob: flush any partial chunk, then write the trailer holding
    /// what only EOF can tell you.
    pub fn finish(&self, mut ing: Ingest) -> Result<Trailer> {
        if !ing.pending.is_empty() {
            self.commit_chunk(&mut ing)?;
        }
        let root: Hash = ing.hasher.finalize().into();
        let trailer = Trailer {
            size: ing.size,
            chunks: ing.seq - 1,
            root: ident::encode(&root),
        };
        let mut batch = self.db.batch();
        batch.insert(
            &self.blobs,
            entry_key(&ing.id, TRAILER_SEQ),
            serde_json::to_vec(&trailer)?,
        );
        batch.insert(&self.roots, pair_key(&root, &ing.id), []);
        batch.commit()?;
        Ok(trailer)
    }

    /// One chunk, one batch. Committing per chunk rather than per blob is what
    /// lets a follower see chunk n land before chunk n+1 is even read.
    fn commit_chunk(&self, ing: &mut Ingest) -> Result<()> {
        let chunk = std::mem::replace(&mut ing.pending, Vec::with_capacity(CHUNK_SIZE));
        ing.hasher.update(&chunk);
        ing.size += chunk.len() as u64;
        let hash: Hash = blake3::hash(&chunk).into();

        // Check before write. The read costs about 0.5% of the write it may
        // avoid, so it pays for itself above roughly that dedup rate, and a
        // blind duplicate write would burn space until blob GC reclaims it.
        let mut batch = self.db.batch();
        if !self.cas.contains_key(hash)? {
            batch.insert(&self.cas, hash, chunk);
        }
        batch.insert(&self.blobs, entry_key(&ing.id, ing.seq), hash);
        batch.insert(&self.refs, pair_key(&hash, &ing.id), []);
        batch.commit()?;
        ing.seq += 1;
        Ok(())
    }

    /// Stream a reader in, optionally forwarding each chunk onward as it is
    /// stored so a pipeline captures and consumes in one pass.
    pub fn put_tee<R: Read, W: Write>(
        &self,
        content_type: &str,
        mut reader: R,
        mut tee: Option<W>,
    ) -> Result<(Scru128Id, Trailer)> {
        let mut ing = self.begin(content_type)?;
        let id = ing.id;
        let mut buf = vec![0u8; CHUNK_SIZE];
        loop {
            let n = match reader.read(&mut buf) {
                Ok(0) => break,
                Ok(n) => n,
                Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            };
            if let Some(w) = tee.as_mut() {
                w.write_all(&buf[..n])?;
                w.flush()?;
            }
            self.append(&mut ing, &buf[..n])?;
        }
        if let Some(mut w) = tee {
            w.flush()?;
        }
        let trailer = self.finish(ing)?;
        Ok((id, trailer))
    }

    // --- lookup ---------------------------------------------------------

    pub fn load(&self, id: &Scru128Id) -> Result<Option<Blob>> {
        let mut content_type = String::new();
        let mut chunks = Vec::new();
        let mut trailer = None;
        let mut seen = false;

        for item in self.blobs.prefix(id.to_bytes()) {
            let (k, v) = item.into_inner()?;
            seen = true;
            let seq = u32::from_be_bytes(k[16..20].try_into().unwrap());
            match seq {
                0 => content_type = String::from_utf8_lossy(&v).into_owned(),
                TRAILER_SEQ => trailer = Some(serde_json::from_slice(&v)?),
                _ => {
                    if v.len() == 32 {
                        chunks.push(<Hash>::try_from(v.as_ref()).unwrap());
                    }
                }
            }
        }
        Ok(if seen {
            Some(Blob { id: *id, content_type, chunks, trailer })
        } else {
            None
        })
    }

    /// Every handle holding this content, oldest first.
    pub fn by_root(&self, root: &Hash) -> Result<Vec<Scru128Id>> {
        let mut out = Vec::new();
        for item in self.roots.prefix(root) {
            let k = item.key()?;
            out.push(Scru128Id::from_bytes(k[32..48].try_into().unwrap()));
        }
        Ok(out)
    }

    pub fn resolve(&self, r: &Ref) -> Result<Scru128Id> {
        match r {
            Ref::Id(id) => Ok(*id),
            Ref::Root(h) => match self.by_root(h)?.first() {
                Some(id) => Ok(*id),
                None => bail!("no blob has root {}", ident::encode(h)),
            },
            Ref::IdPrefix(p) => {
                let mut hits: Vec<Scru128Id> = Vec::new();
                for id in self.ids()? {
                    if id.to_string().starts_with(p) {
                        hits.push(id);
                        if hits.len() > 1 {
                            break;
                        }
                    }
                }
                match hits.len() {
                    0 => bail!("no blob id starts with {p}"),
                    1 => Ok(hits[0]),
                    _ => bail!("{p} is ambiguous, give more characters"),
                }
            }
        }
    }

    /// Blob ids, oldest first. scru128 sorts by creation time, and the
    /// keyspace is sorted by id, so this is a single ordered scan.
    pub fn ids(&self) -> Result<Vec<Scru128Id>> {
        let mut out = Vec::new();
        let mut last: Option<[u8; 16]> = None;
        for item in self.blobs.iter() {
            let k = item.key()?;
            let raw: [u8; 16] = k[..16].try_into().unwrap();
            if last != Some(raw) {
                out.push(Scru128Id::from_bytes(raw));
                last = Some(raw);
            }
        }
        Ok(out)
    }

    // --- read -----------------------------------------------------------

    /// Write a blob's bytes to `out`.
    ///
    /// With `follow`, keep going while the blob is still being ingested,
    /// returning when its trailer lands. fjall has no watch, so this polls;
    /// the interval is the latency a follower sees, not a busy loop.
    pub fn get<W: Write>(&self, id: &Scru128Id, mut out: W, follow: bool) -> Result<u64> {
        let mut next: u32 = 1;
        let mut written = 0u64;
        let idle = Duration::from_millis(25);

        loop {
            let blob = self.load(id)?.context("blob not found")?;
            while (next as usize) <= blob.chunks.len() {
                let hash = blob.chunks[next as usize - 1];
                let chunk = self
                    .cas
                    .get(hash)?
                    .with_context(|| format!("chunk {} missing: {}", next, ident::encode(&hash)))?;
                out.write_all(&chunk)?;
                written += chunk.len() as u64;
                next += 1;
            }
            if blob.complete() || !follow {
                out.flush()?;
                return Ok(written);
            }
            std::thread::sleep(idle);
        }
    }

    /// Re-read the content and check it against the trailer's root. This is
    /// what the root buys today: whole-blob verification after the fact.
    /// Verifying a partial stream needs the BLAKE3 tree's interior nodes,
    /// which are not stored.
    pub fn verify(&self, id: &Scru128Id) -> Result<(bool, u64)> {
        let blob = self.load(id)?.context("blob not found")?;
        let trailer = blob.trailer.as_ref().context("blob is still ingesting")?;
        let mut hasher = blake3::Hasher::new();
        let mut size = 0u64;
        for (i, hash) in blob.chunks.iter().enumerate() {
            let chunk = self
                .cas
                .get(hash)?
                .with_context(|| format!("chunk {} missing", i + 1))?;
            // Each chunk is content-addressed, so a corrupt one is detectable
            // on its own, before the root is even assembled.
            let actual: Hash = blake3::hash(&chunk).into();
            if actual != *hash {
                bail!("chunk {} does not match its hash", i + 1);
            }
            hasher.update(&chunk);
            size += chunk.len() as u64;
        }
        let root: Hash = hasher.finalize().into();
        Ok((ident::encode(&root) == trailer.root && size == trailer.size, size))
    }

    // --- delete ---------------------------------------------------------

    /// Remove a blob and any chunk that nothing else wants.
    ///
    /// Two steps on purpose. The batch drops the blob's entries and its refs
    /// atomically; only then is it safe to ask whether a chunk is orphaned,
    /// because this blob's own claim on it is gone. Deleting a chunk twice is
    /// harmless, so concurrent removes race benignly. A remove racing an
    /// ingest of the same chunk does not: see README, "Concurrency".
    pub fn remove(&self, id: &Scru128Id) -> Result<(usize, u64)> {
        let hashes = self.unlink(id)?;
        self.reclaim(&hashes)
    }

    /// Sweep chunks nothing references. `remove` already reclaims as it goes,
    /// so a healthy store finds nothing here. It exists because an
    /// interrupted `remove` can leave a chunk orphaned after its refs are
    /// gone, and because a safety net you can run is worth more than one you
    /// have to trust.
    pub fn gc(&self) -> Result<(usize, u64)> {
        let mut orphans: Vec<(Hash, u64)> = Vec::new();
        for item in self.cas.iter() {
            let k = item.key()?;
            let hash: Hash = k[..32].try_into().unwrap();
            if self.refs.prefix(hash).next().is_none() {
                let size = self.cas.get(hash)?.map(|v| v.len() as u64).unwrap_or(0);
                orphans.push((hash, size));
            }
        }
        let mut bytes = 0u64;
        for (hash, size) in &orphans {
            self.cas.remove(hash)?;
            bytes += size;
        }
        Ok((orphans.len(), bytes))
    }

    // --- introspection --------------------------------------------------

    pub fn stats(&self) -> Result<Stats> {
        let mut blobs = 0usize;
        let mut ingesting = 0usize;
        let mut logical = 0u64;
        for id in self.ids()? {
            blobs += 1;
            match self.load(&id)?.and_then(|b| b.trailer) {
                Some(t) => logical += t.size,
                None => ingesting += 1,
            }
        }
        let unique_chunks = self.cas.len()?;
        let chunk_refs = self.refs.len()?;
        let mut stored = 0u64;
        for item in self.cas.iter() {
            stored += item.size()? as u64;
        }
        // `roots` is a multimap keyed by root ++ id, so len() counts holders,
        // not distinct content. Fold on the 32-byte prefix.
        let mut distinct_roots = 0usize;
        let mut last: Option<Hash> = None;
        for item in self.roots.iter() {
            let k = item.key()?;
            let r: Hash = k[..32].try_into().unwrap();
            if last != Some(r) {
                distinct_roots += 1;
                last = Some(r);
            }
        }

        Ok(Stats {
            blobs,
            ingesting,
            roots: distinct_roots,
            unique_chunks,
            chunk_refs,
            logical,
            stored,
            disk: dir_size(&self.path),
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[derive(Debug)]
pub struct Stats {
    pub blobs: usize,
    pub ingesting: usize,
    pub roots: usize,
    pub unique_chunks: usize,
    pub chunk_refs: usize,
    /// Bytes the blobs contain, counting shared chunks once per blob.
    pub logical: u64,
    /// Bytes the chunks actually occupy, counting each chunk once.
    pub stored: u64,
    pub disk: u64,
}

fn dir_size(p: &Path) -> u64 {
    fn walk(p: &Path, acc: &mut u64) {
        if let Ok(rd) = std::fs::read_dir(p) {
            for e in rd.flatten() {
                match e.file_type() {
                    Ok(t) if t.is_dir() => walk(&e.path(), acc),
                    Ok(_) => *acc += e.metadata().map(|m| m.len()).unwrap_or(0),
                    _ => {}
                }
            }
        }
    }
    let mut acc = 0;
    walk(p, &mut acc);
    acc
}

/// Wall time helper for the CLI's timing lines.
pub fn since(t: Instant) -> String {
    let s = t.elapsed().as_secs_f64();
    if s < 1.0 {
        format!("{:.0}ms", s * 1000.0)
    } else {
        format!("{s:.1}s")
    }
}

impl Store {
    /// Remove a blob from the namespace without reclaiming its chunks.
    ///
    /// The Unix model: unlink drops the name, and the content survives while
    /// anyone still holds it open. Returns the chunks the blob referenced so
    /// the caller can reclaim them once the last handle closes.
    pub fn unlink(&self, id: &Scru128Id) -> Result<Vec<Hash>> {
        let blob = self.load(id)?.context("blob not found")?;
        let mut distinct = blob.chunks.clone();
        distinct.sort_unstable();
        distinct.dedup();

        let mut batch = self.db.batch();
        for item in self.blobs.prefix(id.to_bytes()) {
            batch.remove(&self.blobs, item.key()?);
        }
        for hash in &distinct {
            batch.remove(&self.refs, pair_key(hash, id));
        }
        if let Some(t) = &blob.trailer {
            batch.remove(&self.roots, pair_key(&ident::decode(&t.root)?, id));
        }
        batch.commit()?;
        Ok(distinct)
    }

    /// Drop any of these chunks that nothing references any more.
    pub fn reclaim(&self, hashes: &[Hash]) -> Result<(usize, u64)> {
        let mut n = 0;
        let mut bytes = 0u64;
        for hash in hashes {
            if self.refs.prefix(hash).next().is_none() {
                if let Some(v) = self.cas.get(hash)? {
                    bytes += v.len() as u64;
                }
                self.cas.remove(hash)?;
                n += 1;
            }
        }
        Ok((n, bytes))
    }

    /// Chunk hashes from `from` onward, without rescanning the whole blob.
    /// A follower calls this every time it wakes, so it must cost what
    /// arrived rather than what exists.
    pub fn chunks_from(&self, id: &Scru128Id, from: u32) -> Result<(Vec<Hash>, bool)> {
        let mut start = id.to_bytes().to_vec();
        start.extend_from_slice(&from.to_be_bytes());
        let mut end = id.to_bytes().to_vec();
        end.extend_from_slice(&u32::MAX.to_be_bytes());

        let mut out = Vec::new();
        let mut complete = false;
        for item in self.blobs.range(start..=end) {
            let (k, v) = item.into_inner()?;
            let seq = u32::from_be_bytes(k[16..20].try_into().unwrap());
            if seq == TRAILER_SEQ {
                complete = true;
            } else if v.len() == 32 {
                out.push(<Hash>::try_from(v.as_ref()).unwrap());
            }
        }
        Ok((out, complete))
    }

    pub fn chunk(&self, hash: &Hash) -> Result<Option<Vec<u8>>> {
        Ok(self.cas.get(hash)?.map(|v| v.to_vec()))
    }
}
