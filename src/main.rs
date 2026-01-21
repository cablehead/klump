use clap::{Parser, Subcommand};
use fjall::{Database, Keyspace, KeyspaceCreateOptions};
use scru128::Scru128Id;
use serde::{Deserialize, Serialize};
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

#[derive(Serialize, Deserialize, Debug)]
struct BlobManifest {
    status: BlobStatus,
    chunks: Vec<String>, // hex-encoded chunk hashes
    size: u64,
}

#[derive(Serialize, Deserialize, Debug, PartialEq)]
enum BlobStatus {
    Ingesting,
    Complete,
}

struct Store {
    #[allow(dead_code)]
    db: Database,
    cas: Keyspace,   // chunk_hash -> chunk_bytes
    blobs: Keyspace, // scru128_id -> BlobManifest (JSON)
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
        let mut chunks = Vec::new();
        let mut total_size = 0u64;
        let mut buffer = vec![0u8; CHUNK_SIZE];

        // Write initial manifest (ingesting)
        let manifest = BlobManifest {
            status: BlobStatus::Ingesting,
            chunks: vec![],
            size: 0,
        };
        self.blobs.insert(
            id.to_bytes(),
            serde_json::to_vec(&manifest).unwrap(),
        )?;

        loop {
            let bytes_read = reader.read(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            let chunk = &buffer[..bytes_read];
            let hash = self.store_chunk(chunk)?;
            chunks.push(hash);
            total_size += bytes_read as u64;

            // Update manifest with new chunk
            let manifest = BlobManifest {
                status: BlobStatus::Ingesting,
                chunks: chunks.clone(),
                size: total_size,
            };
            self.blobs.insert(
                id.to_bytes(),
                serde_json::to_vec(&manifest).unwrap(),
            )?;
        }

        // Finalize manifest
        let manifest = BlobManifest {
            status: BlobStatus::Complete,
            chunks,
            size: total_size,
        };
        self.blobs.insert(
            id.to_bytes(),
            serde_json::to_vec(&manifest).unwrap(),
        )?;

        Ok(id)
    }

    fn store_chunk(&self, data: &[u8]) -> fjall::Result<String> {
        let mut hasher = Sha256::new();
        hasher.update(data);
        let hash = hasher.finalize();
        let hash_hex = data_encoding::HEXLOWER.encode(&hash);

        // Only write if not already present (content-addressed)
        if self.cas.get(&hash_hex)?.is_none() {
            self.cas.insert(&hash_hex, data)?;
        }

        Ok(hash_hex)
    }

    fn get<W: Write>(&self, id: Scru128Id, mut writer: W) -> fjall::Result<Option<u64>> {
        let Some(manifest_bytes) = self.blobs.get(id.to_bytes())? else {
            return Ok(None);
        };

        let manifest: BlobManifest = serde_json::from_slice(&manifest_bytes)
            .expect("invalid manifest");

        let mut written = 0u64;
        for chunk_hash in &manifest.chunks {
            let chunk = self.cas.get(chunk_hash)?
                .expect("missing chunk");
            writer.write_all(&chunk)?;
            written += chunk.len() as u64;
        }

        Ok(Some(written))
    }

    fn list(&self) -> fjall::Result<Vec<(Scru128Id, BlobManifest)>> {
        let mut results = Vec::new();
        for item in self.blobs.iter() {
            let (key, value) = item.into_inner()?;
            let id = Scru128Id::from_bytes(key.as_ref().try_into().unwrap());
            let manifest: BlobManifest = serde_json::from_slice(&value).unwrap();
            results.push((id, manifest));
        }
        Ok(results)
    }

    fn info(&self, id: Scru128Id) -> fjall::Result<Option<BlobManifest>> {
        let Some(manifest_bytes) = self.blobs.get(id.to_bytes())? else {
            return Ok(None);
        };
        let manifest: BlobManifest = serde_json::from_slice(&manifest_bytes).unwrap();
        Ok(Some(manifest))
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
            let id: Scru128Id = id.parse()
                .map_err(|_| "invalid scru128 id")?;
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
            for (id, manifest) in store.list()? {
                println!("{}\t{:?}\t{} chunks\t{} bytes",
                    id,
                    manifest.status,
                    manifest.chunks.len(),
                    manifest.size,
                );
            }
        }
        Commands::Info { id } => {
            let id: Scru128Id = id.parse()
                .map_err(|_| "invalid scru128 id")?;
            match store.info(id)? {
                Some(manifest) => {
                    println!("Status: {:?}", manifest.status);
                    println!("Size: {} bytes", manifest.size);
                    println!("Chunks: {}", manifest.chunks.len());
                    for (i, hash) in manifest.chunks.iter().enumerate() {
                        println!("  {}: {}", i, hash);
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
