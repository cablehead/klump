use clap::{Parser, Subcommand};
use fjall::{Database, Keyspace, KeyspaceCreateOptions, Slice};
use scru128::Scru128Id;
use sha2::{Digest, Sha256};
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
    Put,
    /// Retrieve content by ID, writes to stdout
    Get { id: String },
    /// List all stored blobs
    List,
    /// Show info about a blob
    Info { id: String },
}

#[derive(Debug, Clone, Copy, PartialEq)]
#[repr(u8)]
enum BlobStatus {
    Ingesting = 0,
    Complete = 1,
}

impl BlobStatus {
    fn from_byte(b: u8) -> Self {
        match b {
            0 => BlobStatus::Ingesting,
            _ => BlobStatus::Complete,
        }
    }
}

struct BlobMeta {
    status: BlobStatus,
    size: u64,
}

impl BlobMeta {
    fn to_bytes(&self) -> [u8; 9] {
        let mut buf = [0u8; 9];
        buf[0] = self.status as u8;
        buf[1..9].copy_from_slice(&self.size.to_be_bytes());
        buf
    }

    fn from_bytes(bytes: &[u8]) -> Self {
        let status = BlobStatus::from_byte(bytes[0]);
        let size = u64::from_be_bytes(bytes[1..9].try_into().unwrap());
        BlobMeta { status, size }
    }
}

struct Store {
    #[allow(dead_code)]
    db: Database,
    cas: Keyspace,   // hash (32 bytes) -> chunk bytes
    blobs: Keyspace, // blob_id (16 bytes) -> meta OR blob_id (16) + seq (4) -> hash (32)
}

// Key helpers
fn meta_key(id: &Scru128Id) -> [u8; 16] {
    id.to_bytes()
}

fn chunk_key(id: &Scru128Id, seq: u32) -> [u8; 20] {
    let mut key = [0u8; 20];
    key[0..16].copy_from_slice(&id.to_bytes());
    key[16..20].copy_from_slice(&seq.to_be_bytes());
    key
}

impl Store {
    fn open(path: PathBuf) -> fjall::Result<Self> {
        let db = Database::builder(path).open()?;
        let cas = db.keyspace("cas", || KeyspaceCreateOptions::default())?;
        let blobs = db.keyspace("blobs", || KeyspaceCreateOptions::default())?;
        Ok(Self { db, cas, blobs })
    }

    fn put<R: Read>(&self, mut reader: R) -> fjall::Result<Scru128Id> {
        let id = scru128::new();
        let mut seq = 0u32;
        let mut total_size = 0u64;
        let mut buffer = vec![0u8; CHUNK_SIZE];

        // Write initial meta (ingesting)
        let meta = BlobMeta {
            status: BlobStatus::Ingesting,
            size: 0,
        };
        self.blobs.insert(meta_key(&id), meta.to_bytes())?;

        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            let chunk = &buffer[..bytes_read];
            let hash = self.store_chunk(chunk)?;
            total_size += bytes_read as u64;

            // Write chunk index entry: key = blob_id + seq, value = hash
            self.blobs.insert(chunk_key(&id, seq), hash)?;
            seq += 1;

            // Update meta with current size
            let meta = BlobMeta {
                status: BlobStatus::Ingesting,
                size: total_size,
            };
            self.blobs.insert(meta_key(&id), meta.to_bytes())?;
        }

        // Finalize meta
        let meta = BlobMeta {
            status: BlobStatus::Complete,
            size: total_size,
        };
        self.blobs.insert(meta_key(&id), meta.to_bytes())?;

        Ok(id)
    }

    fn store_chunk(&self, data: &[u8]) -> fjall::Result<[u8; 32]> {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash: [u8; 32] = hasher.finalize().into();

        // Only write if not already present (content-addressed)
        if self.cas.get(hash)?.is_none() {
            self.cas.insert(hash, data)?;
        }

        Ok(hash)
    }

    fn get_chunks(&self, id: &Scru128Id) -> fjall::Result<Vec<[u8; 32]>> {
        let mut chunks = Vec::new();
        let prefix = id.to_bytes();

        for item in self.blobs.prefix(Slice::from(prefix.as_slice())) {
            let (key, value) = item.into_inner()?;
            // Skip meta entry (16 bytes), only process chunk entries (20 bytes)
            if key.len() == 20 {
                let hash: [u8; 32] = value.as_ref().try_into().unwrap();
                chunks.push(hash);
            }
        }
        Ok(chunks)
    }

    fn get<W: Write>(&self, id: Scru128Id, mut writer: W) -> fjall::Result<Option<u64>> {
        // Check if blob exists
        if self.blobs.get(meta_key(&id))?.is_none() {
            return Ok(None);
        }

        let mut written = 0u64;
        for hash in self.get_chunks(&id)? {
            let chunk = self.cas.get(hash)?.expect("missing chunk");
            writer.write_all(&chunk)?;
            written += chunk.len() as u64;
        }

        Ok(Some(written))
    }

    fn list(&self) -> fjall::Result<Vec<(Scru128Id, BlobMeta, usize)>> {
        let mut results = Vec::new();
        let mut current_id: Option<Scru128Id> = None;
        let mut chunk_count = 0usize;
        let mut current_meta: Option<BlobMeta> = None;

        for item in self.blobs.iter() {
            let (key, value) = item.into_inner()?;

            if key.len() == 16 {
                // Meta entry - flush previous if any
                if let (Some(id), Some(meta)) = (current_id, current_meta.take()) {
                    results.push((id, meta, chunk_count));
                }

                let id = Scru128Id::from_bytes(key.as_ref().try_into().unwrap());
                let meta = BlobMeta::from_bytes(&value);
                current_id = Some(id);
                current_meta = Some(meta);
                chunk_count = 0;
            } else if key.len() == 20 {
                // Chunk entry
                chunk_count += 1;
            }
        }

        // Flush last
        if let (Some(id), Some(meta)) = (current_id, current_meta) {
            results.push((id, meta, chunk_count));
        }

        Ok(results)
    }

    fn info(&self, id: Scru128Id) -> fjall::Result<Option<(BlobMeta, Vec<[u8; 32]>)>> {
        let Some(meta_bytes) = self.blobs.get(meta_key(&id))? else {
            return Ok(None);
        };
        let meta = BlobMeta::from_bytes(&meta_bytes);
        let chunks = self.get_chunks(&id)?;
        Ok(Some((meta, chunks)))
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let cli = Cli::parse();
    let store = Store::open(cli.store)?;

    match cli.command {
        Commands::Put => {
            let stdin = std::io::stdin();
            let id = store.put(stdin.lock())?;
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
            for (id, meta, chunk_count) in store.list()? {
                println!(
                    "{}\t{:?}\t{} chunks\t{} bytes",
                    id, meta.status, chunk_count, meta.size,
                );
            }
        }
        Commands::Info { id } => {
            let id: Scru128Id = id.parse().map_err(|_| "invalid scru128 id")?;
            match store.info(id)? {
                Some((meta, chunks)) => {
                    println!("Status: {:?}", meta.status);
                    println!("Size: {} bytes", meta.size);
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
