//! klump: streaming content-addressed blob storage on fjall.

mod ident;
mod serve;
mod store;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use ident::{human, parse_ref};
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Instant;
use store::{Store, DEFAULT_CONTENT_TYPE};

#[derive(Parser)]
#[command(
    name = "klump",
    version,
    about = "Streaming content-addressed blob storage",
    long_about = "Streaming content-addressed blob storage.\n\n\
        Content is split into 64K chunks, each stored under its BLAKE3 hash, so \
        identical chunks are stored once however many blobs share them. Nothing is \
        buffered whole: chunks are committed as they arrive and streamed back one \
        at a time.\n\n\
        Blobs are named twice. The id is a scru128 handle, minted when ingest opens. \
        The root is the BLAKE3 hash of the content, known only at EOF, and shared by \
        any two blobs holding the same bytes. Commands take either, or an unambiguous \
        prefix of an id."
)]
struct Cli {
    /// Store directory
    #[arg(long, env = "KLUMP_STORE", default_value = "~/.klump", global = true)]
    store: String,
    /// Block cache, in MiB
    #[arg(long, env = "KLUMP_CACHE_MB", default_value_t = 64, global = true)]
    cache_mb: u64,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Store content from a file or stdin, and print its blob id
    Put {
        /// File to read; omit or use - for stdin
        file: Option<PathBuf>,
        /// Content type, as an HTTP Content-Type value
        #[arg(short = 't', long)]
        content_type: Option<String>,
        /// Print the root hash instead of the blob id
        #[arg(long)]
        root: bool,
        /// Report size, chunks and rate on stderr
        #[arg(short, long)]
        verbose: bool,
        /// Write the content onward to stdout as it is stored, so a pipeline
        /// consumes it in the same pass. The name goes to stderr instead.
        #[arg(long)]
        tee: bool,
    },
    /// Write a blob's content to stdout or a file
    Get {
        /// Blob id, id prefix, or root hash
        reference: String,
        /// Write here instead of stdout
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Keep streaming while the blob is still being ingested
        #[arg(short, long)]
        follow: bool,
    },
    /// List blobs, oldest first
    #[command(alias = "list")]
    Ls {
        /// Show root hashes as well
        #[arg(short, long)]
        long: bool,
    },
    /// Show one blob in detail
    Info {
        reference: String,
        /// List every chunk
        #[arg(short, long)]
        chunks: bool,
    },
    /// Delete blobs, reclaiming chunks nothing else references
    #[command(alias = "remove")]
    Rm {
        #[arg(required = true)]
        references: Vec<String>,
    },
    /// Re-read a blob and check it against its root hash
    Verify { reference: String },
    /// Delete chunks nothing references
    Gc,
    /// Summarise the store
    Stats,
    /// Run the daemon
    Serve {
        /// Unix socket to listen on
        #[arg(long, env = "KLUMP_SOCKET", default_value = "~/.klump.sock")]
        socket: String,
    },
}

fn expand(p: &str) -> PathBuf {
    if let Some(rest) = p.strip_prefix("~/") {
        if let Ok(home) = std::env::var("HOME") {
            return PathBuf::from(home).join(rest);
        }
    }
    PathBuf::from(p)
}

fn ratio(a: u64, b: u64) -> f64 {
    if b > 0 {
        a as f64 / b as f64
    } else {
        1.0
    }
}

fn main() {
    if let Err(e) = run() {
        eprintln!("klump: {e:#}");
        std::process::exit(1);
    }
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    let store = Store::open(expand(&cli.store), cli.cache_mb)?;

    match cli.cmd {
        Cmd::Put { file, content_type, root, verbose, tee } => {
            let ct = content_type.unwrap_or_else(|| DEFAULT_CONTENT_TYPE.into());
            let started = Instant::now();
            let sink = if tee { Some(io::stdout().lock()) } else { None };
            let (id, trailer) = match file.as_deref() {
                Some(p) if p.as_os_str() != "-" => {
                    let f = std::fs::File::open(p)
                        .with_context(|| format!("opening {}", p.display()))?;
                    store.put_tee(&ct, io::BufReader::new(f), sink)?
                }
                _ => store.put_tee(&ct, io::stdin().lock(), sink)?,
            };
            let name = if root { trailer.root.clone() } else { id.to_string() };
            // With --tee stdout carries the content, so the name goes to stderr.
            if tee {
                eprintln!("{name}");
            } else {
                println!("{name}");
            }
            if verbose {
                let secs = started.elapsed().as_secs_f64().max(1e-9);
                eprintln!(
                    "{} in {} chunks, {} at {}/s, root {}",
                    human(trailer.size),
                    trailer.chunks,
                    store::since(started),
                    human((trailer.size as f64 / secs) as u64),
                    trailer.root
                );
            }
        }

        Cmd::Get { reference, output, follow } => {
            let id = store.resolve(&parse_ref(&reference)?)?;
            match output {
                Some(p) => {
                    let f = std::fs::File::create(&p)
                        .with_context(|| format!("creating {}", p.display()))?;
                    store.get(&id, io::BufWriter::new(f), follow)?;
                }
                None => {
                    let out = io::stdout();
                    store.get(&id, io::BufWriter::new(out.lock()), follow)?;
                }
            }
        }

        Cmd::Ls { long } => {
            for id in store.ids()? {
                let Some(b) = store.load(&id)? else { continue };
                let size = b.size().map(human).unwrap_or_else(|| "-".into());
                if long {
                    println!(
                        "{id}  {size:>7}  {:<9}  {:<24}  {}",
                        b.status(),
                        b.content_type,
                        b.trailer.as_ref().map(|t| t.root.as_str()).unwrap_or("-")
                    );
                } else {
                    println!("{id}  {size:>7}  {:<9}  {}", b.status(), b.content_type);
                }
            }
        }

        Cmd::Info { reference, chunks } => {
            let id = store.resolve(&parse_ref(&reference)?)?;
            let b = store.load(&id)?.context("blob not found")?;
            println!("id            {}", b.id);
            println!("status        {}", b.status());
            println!("content-type  {}", b.content_type);
            match &b.trailer {
                Some(t) => {
                    println!("size          {} ({} bytes)", human(t.size), t.size);
                    println!("chunks        {}", t.chunks);
                    println!("root          {}", t.root);
                    let holders = store.by_root(&ident::decode(&t.root)?)?;
                    let others: Vec<String> = holders
                        .iter()
                        .filter(|h| **h != b.id)
                        .map(|h| h.to_string())
                        .collect();
                    if !others.is_empty() {
                        println!("also held by  {}", others.join(", "));
                    }
                }
                None => println!("chunks        {} so far", b.chunks.len()),
            }
            if chunks {
                println!();
                for (i, h) in b.chunks.iter().enumerate() {
                    println!("  {:>5}  {}", i + 1, ident::encode(h));
                }
            }
        }

        Cmd::Rm { references } => {
            let mut chunks = 0;
            let mut bytes = 0u64;
            for r in &references {
                let id = store.resolve(&parse_ref(r)?)?;
                let (c, b) = store.remove(&id)?;
                chunks += c;
                bytes += b;
                println!("removed {id}");
            }
            eprintln!("freed {chunks} chunks, {}", human(bytes));
        }

        Cmd::Verify { reference } => {
            let id = store.resolve(&parse_ref(&reference)?)?;
            let started = Instant::now();
            let (ok, size) = store.verify(&id)?;
            if !ok {
                println!("FAILED  {id}");
                std::process::exit(1);
            }
            println!("ok  {id}  {} in {}", human(size), store::since(started));
        }

        Cmd::Serve { socket } => {
            serve::serve(store, &expand(&socket))?;
        }

        Cmd::Gc => {
            let (n, bytes) = store.gc()?;
            println!("removed {n} unreferenced chunks, {}", human(bytes));
        }

        Cmd::Stats => {
            let s = store.stats()?;
            println!("store          {}", store.path().display());
            let ing = match s.ingesting {
                0 => String::new(),
                n => format!(" ({n} ingesting)"),
            };
            println!("blobs          {}{ing}", s.blobs);
            println!("distinct roots {}", s.roots);
            println!("chunks         {} unique, {} referenced", s.unique_chunks, s.chunk_refs);
            let dedup = if s.chunk_refs > 0 {
                100.0 * (s.chunk_refs - s.unique_chunks) as f64 / s.chunk_refs as f64
            } else {
                0.0
            };
            println!("dedup          {dedup:.1}%");
            println!("logical        {:>7}  what the blobs contain", human(s.logical));
            println!(
                "stored         {:>7}  unique chunk bytes, {:.1}x from dedup",
                human(s.stored),
                ratio(s.logical, s.stored)
            );
            // On-disk covers the journal and anything compaction has not
            // reclaimed yet, so it is not a compression ratio, and after a
            // large delete it exceeds the live bytes until compaction runs.
            if s.disk > s.stored {
                println!(
                    "on disk        {:>7}  includes journal and space pending compaction",
                    human(s.disk)
                );
            } else {
                println!(
                    "on disk        {:>7}  {:.1}x smaller again after Lz4, {:.1}x overall",
                    human(s.disk),
                    ratio(s.stored, s.disk),
                    ratio(s.logical, s.disk)
                );
            }
        }
    }

    let _ = io::stdout().flush();
    Ok(())
}
