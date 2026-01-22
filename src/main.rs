use clap::{Parser, Subcommand};
use fjall::{CompressionType, Database, Keyspace, KeyspaceCreateOptions, KvSeparationOptions, Slice};
use scru128::Scru128Id;
use std::io::{Read, Write};
use std::path::PathBuf;

const CHUNK_SIZE: usize = 64 * 1024; // 64KB

#[derive(Parser)]
#[command(name = "klump")]
#[command(about = "Chunked blob storage experiment")]
struct Cli {
    /// Path to the store directory
    #[arg(short, long, default_value = ".klump")]
    store: PathBuf,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Store content from stdin, returns blob ID
    Put {
        /// Content type (e.g., text/plain, application/json)
        #[arg(short = 't', long, default_value = "")]
        content_type: String,
    },
    /// Retrieve content by ID, writes to stdout
    Get { id: String },
    /// List all stored blobs
    List,
    /// Show info about a blob
    Info { id: String },
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum BlobStatus {
    Ingesting,
    Complete,
}

struct BlobInfo {
    content_type: String,
    chunks: Vec<[u8; 32]>,
    is_complete: bool,
}

struct Store {
    #[allow(dead_code)]
    db: Database,
    cas: Keyspace,   // hash (32 bytes) -> chunk bytes
    blobs: Keyspace, // blob_id + seq -> content_type (seq 0) | hash (32B) | empty (EOF)
}

fn entry_key(id: &Scru128Id, seq: u32) -> [u8; 20] {
    let mut key = [0u8; 20];
    key[0..16].copy_from_slice(&id.to_bytes());
    key[16..20].copy_from_slice(&seq.to_be_bytes());
    key
}

impl Store {
    fn open(path: PathBuf) -> fjall::Result<Self> {
        let db = Database::builder(path).open()?;

        // CAS: large values (64KB chunks), use KV-separation with LZ4
        let cas = db.keyspace("cas", || {
            KeyspaceCreateOptions::default().with_kv_separation(Some(
                KvSeparationOptions::default().compression(CompressionType::Lz4),
            ))
        })?;

        // Blobs: small keys/values (20B -> 32B or empty), defaults fine
        let blobs = db.keyspace("blobs", || KeyspaceCreateOptions::default())?;

        Ok(Self { db, cas, blobs })
    }

    fn put<R: Read>(&self, content_type: &str, mut reader: R) -> fjall::Result<Scru128Id> {
        let id = scru128::new();
        let mut seq = 0u32;
        let mut buffer = vec![0u8; CHUNK_SIZE];

        // Seq 0: content-type
        self.blobs.insert(entry_key(&id, seq), content_type.as_bytes())?;
        seq += 1;

        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            let chunk = &buffer[..bytes_read];
            let hash = self.store_chunk(chunk)?;

            // Write chunk entry: key = blob_id + seq, value = hash
            self.blobs.insert(entry_key(&id, seq), hash)?;
            seq += 1;
        }

        // Write EOF marker: empty value
        self.blobs.insert(entry_key(&id, seq), [])?;

        Ok(id)
    }

    fn store_chunk(&self, data: &[u8]) -> fjall::Result<[u8; 32]> {
        let hash: [u8; 32] = blake3::hash(data).into();

        // Only write if not already present (content-addressed)
        // TOCTOU race is fine: same hash means same content
        if !self.cas.contains_key(hash)? {
            self.cas.insert(hash, data)?;
        }

        Ok(hash)
    }

    fn get_blob_info(&self, id: &Scru128Id) -> fjall::Result<Option<BlobInfo>> {
        let mut content_type = String::new();
        let mut chunks = Vec::new();
        let mut is_complete = false;
        let mut seq = 0u32;
        let prefix = id.to_bytes();

        for item in self.blobs.prefix(Slice::from(prefix.as_slice())) {
            let (_, value) = item.into_inner()?;

            if seq == 0 {
                // Content-type
                content_type = String::from_utf8_lossy(&value).into_owned();
            } else if value.is_empty() {
                // EOF
                is_complete = true;
            } else if value.len() == 32 {
                // Chunk hash
                let hash: [u8; 32] = value.as_ref().try_into().unwrap();
                chunks.push(hash);
            }
            seq += 1;
        }

        if seq == 0 {
            return Ok(None); // Blob doesn't exist
        }

        Ok(Some(BlobInfo {
            content_type,
            chunks,
            is_complete,
        }))
    }

    fn get<W: Write>(&self, id: Scru128Id, mut writer: W) -> fjall::Result<Option<u64>> {
        let Some(info) = self.get_blob_info(&id)? else {
            return Ok(None);
        };

        let mut written = 0u64;
        for hash in info.chunks {
            let chunk = self.cas.get(hash)?.expect("missing chunk");
            writer.write_all(&chunk)?;
            written += chunk.len() as u64;
        }

        Ok(Some(written))
    }

    fn list(&self) -> fjall::Result<Vec<(Scru128Id, BlobStatus, String, usize, u64)>> {
        let mut results = Vec::new();
        let mut current_id: Option<Scru128Id> = None;
        let mut content_type = String::new();
        let mut chunk_count = 0usize;
        let mut is_complete = false;
        let mut size = 0u64;
        let mut seq = 0u32;

        for item in self.blobs.iter() {
            let (key, value) = item.into_inner()?;
            let id = Scru128Id::from_bytes(key[0..16].try_into().unwrap());

            // New blob?
            if current_id != Some(id) {
                // Flush previous
                if let Some(prev_id) = current_id {
                    let status = if is_complete { BlobStatus::Complete } else { BlobStatus::Ingesting };
                    results.push((prev_id, status, content_type.clone(), chunk_count, size));
                }
                current_id = Some(id);
                content_type = String::new();
                chunk_count = 0;
                is_complete = false;
                size = 0;
                seq = 0;
            }

            if seq == 0 {
                // Content-type
                content_type = String::from_utf8_lossy(&value).into_owned();
            } else if value.is_empty() {
                is_complete = true;
            } else if value.len() == 32 {
                let hash: [u8; 32] = value.as_ref().try_into().unwrap();
                if let Some(chunk) = self.cas.get(hash)? {
                    size += chunk.len() as u64;
                }
                chunk_count += 1;
            }
            seq += 1;
        }

        // Flush last
        if let Some(id) = current_id {
            let status = if is_complete { BlobStatus::Complete } else { BlobStatus::Ingesting };
            results.push((id, status, content_type, chunk_count, size));
        }

        Ok(results)
    }

    fn info(&self, id: Scru128Id) -> fjall::Result<Option<(BlobStatus, String, Vec<[u8; 32]>, u64)>> {
        let Some(info) = self.get_blob_info(&id)? else {
            return Ok(None);
        };

        let status = if info.is_complete { BlobStatus::Complete } else { BlobStatus::Ingesting };

        let mut size = 0u64;
        for hash in &info.chunks {
            if let Some(chunk) = self.cas.get(hash)? {
                size += chunk.len() as u64;
            }
        }

        Ok(Some((status, info.content_type, info.chunks, size)))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let store = Store::open(cli.store)?;

    match cli.command {
        Commands::Put { content_type } => {
            let stdin = std::io::stdin();
            let id = store.put(&content_type, stdin.lock())?;
            println!("{}", id);
        }
        Commands::Get { id } => {
            let id: Scru128Id = id.parse().map_err(|_| "invalid scru128 id")?;
            let stdout = std::io::stdout();
            match store.get(id, stdout.lock())? {
                Some(_) => {}
                None => {
                    eprintln!("blob not found: {}", id);
                    std::process::exit(1);
                }
            }
        }
        Commands::List => {
            for (id, status, content_type, chunk_count, size) in store.list()? {
                let ct = if content_type.is_empty() { "-" } else { &content_type };
                println!(
                    "{}\t{:?}\t{}\t{} chunks\t{} bytes",
                    id, status, ct, chunk_count, size,
                );
            }
        }
        Commands::Info { id } => {
            let id: Scru128Id = id.parse().map_err(|_| "invalid scru128 id")?;
            match store.info(id)? {
                Some((status, content_type, chunks, size)) => {
                    println!("Status: {:?}", status);
                    println!("Content-Type: {}", if content_type.is_empty() { "-" } else { &content_type });
                    println!("Size: {} bytes", size);
                    println!("Chunks: {}", chunks.len());
                    for (i, hash) in chunks.iter().enumerate() {
                        println!("  {}: {}", i, data_encoding::HEXLOWER.encode(hash));
                    }
                }
                None => {
                    eprintln!("blob not found: {}", id);
                    std::process::exit(1);
                }
            }
        }
    }

    Ok(())
}
