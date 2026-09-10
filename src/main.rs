//! klump: streaming content-addressed blob storage on fjall.

mod client;
mod ident;
mod serve;
mod store;

use anyhow::{bail, Context, Result};
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
    /// Block cache, in MiB. Ignored when a daemon is serving.
    #[arg(long, env = "KLUMP_CACHE_MB", default_value_t = 64, global = true)]
    cache_mb: u64,
    /// Daemon socket. Used automatically when something is listening on it.
    #[arg(long, env = "KLUMP_SOCKET", default_value = "~/.klump.sock", global = true)]
    socket: String,
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
    Serve {},
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
    let socket = expand(&cli.socket);

    // serve always takes the store itself; everything else prefers a daemon
    // if one is holding it, because fjall allows only one process at a time.
    if !matches!(cli.cmd, Cmd::Serve { }) {
        if let Some(c) = client::Client::connect(&socket) {
            return run_remote(cli.cmd, c);
        }
    }

    let store = Store::open(expand(&cli.store), cli.cache_mb)?;
    if let Cmd::Serve { } = cli.cmd {
        return serve::serve(store, &socket);
    }

    match cli.cmd {
        Cmd::Serve { } => unreachable!(),
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

/// The same commands, against a running daemon.
///
/// The store allows one process at a time, so a daemon holding it would make
/// every other klump invocation fail. Instead the CLI notices the socket and
/// speaks to it. Nothing about the command surface changes.
fn run_remote(cmd: Cmd, mut c: client::Client) -> Result<()> {
    match cmd {
        Cmd::Serve { .. } => unreachable!("serve is always local"),

        Cmd::Put { file, content_type, root, verbose, tee } => {
            let ct = content_type.unwrap_or_else(|| DEFAULT_CONTENT_TYPE.into());
            let started = Instant::now();
            let mut stdout = io::stdout();
            let mut sink: Option<&mut dyn Write> = if tee { Some(&mut stdout) } else { None };
            let mut f;
            let mut stdin;
            let body: &mut dyn io::Read = match file.as_deref() {
                Some(p) if p.as_os_str() != "-" => {
                    f = io::BufReader::new(
                        std::fs::File::open(p)
                            .with_context(|| format!("opening {}", p.display()))?,
                    );
                    &mut f
                }
                _ => {
                    stdin = io::stdin().lock();
                    &mut stdin
                }
            };
            let reply = c.send("POST", "/blobs", Some(&ct), Some(body), sink.take())?;
            let mut out = Vec::new();
            c.read_body(&mut out)?;
            if reply.status >= 400 {
                bail!("{}", String::from_utf8_lossy(&out).trim());
            }
            // NDJSON: the id landed first, the trailer closes it.
            let last = out
                .split(|b| *b == b'\n')
                .filter(|l| !l.is_empty())
                .next_back()
                .unwrap_or_default();
            let v: serde_json::Value = serde_json::from_slice(last)?;
            let id = v["id"].as_str().unwrap_or_default();
            let rt = v["root"].as_str().unwrap_or_default();
            let name = if root { rt } else { id };
            if tee {
                eprintln!("{name}");
            } else {
                println!("{name}");
            }
            if verbose {
                let size = v["size"].as_u64().unwrap_or(0);
                let secs = started.elapsed().as_secs_f64().max(1e-9);
                eprintln!(
                    "{} in {} chunks, {} at {}/s, root {rt}",
                    human(size),
                    v["chunks"].as_u64().unwrap_or(0),
                    store::since(started),
                    human((size as f64 / secs) as u64),
                );
            }
        }

        Cmd::Get { reference, output, follow } => {
            let path = if follow {
                format!("/blobs/{reference}?follow=1")
            } else {
                format!("/blobs/{reference}")
            };
            let reply = c.send("GET", &path, None, None, None)?;
            if reply.status >= 400 {
                let mut e = Vec::new();
                c.read_body(&mut e)?;
                bail!("{}", String::from_utf8_lossy(&e).trim());
            }
            match output {
                Some(p) => {
                    let f = std::fs::File::create(&p)
                        .with_context(|| format!("creating {}", p.display()))?;
                    c.read_body(&mut io::BufWriter::new(f))?;
                }
                None => {
                    let out = io::stdout();
                    c.read_body(&mut io::BufWriter::new(out.lock()))?;
                }
            }
        }

        Cmd::Ls { long } => {
            let v = c.json("GET", "/blobs")?;
            for b in v.as_array().cloned().unwrap_or_default() {
                let size = b["size"].as_u64().map(human).unwrap_or_else(|| "-".into());
                let status = b["status"].as_str().unwrap_or("?");
                let ct = b["content_type"].as_str().unwrap_or("");
                let id = b["id"].as_str().unwrap_or("");
                if long {
                    println!(
                        "{id}  {size:>7}  {status:<9}  {ct:<24}  {}",
                        b["root"].as_str().unwrap_or("-")
                    );
                } else {
                    println!("{id}  {size:>7}  {status:<9}  {ct}");
                }
            }
        }

        Cmd::Info { reference, chunks } => {
            let v = c.json("GET", &format!("/blobs/{reference}?meta=1"))?;
            println!("id            {}", v["id"].as_str().unwrap_or(""));
            println!("status        {}", v["status"].as_str().unwrap_or(""));
            println!("content-type  {}", v["content_type"].as_str().unwrap_or(""));
            if let Some(size) = v["size"].as_u64() {
                println!("size          {} ({size} bytes)", human(size));
                println!("chunks        {}", v["chunks"].as_u64().unwrap_or(0));
                println!("root          {}", v["root"].as_str().unwrap_or(""));
                let held: Vec<String> = v["also_held_by"]
                    .as_array()
                    .map(|a| a.iter().filter_map(|s| s.as_str().map(String::from)).collect())
                    .unwrap_or_default();
                if !held.is_empty() {
                    println!("also held by  {}", held.join(", "));
                }
            }
            if chunks {
                eprintln!("klump: --chunks needs direct store access; stop the daemon to use it");
            }
        }

        Cmd::Rm { references } => {
            let mut n = 0u64;
            let mut bytes = 0u64;
            for r in &references {
                let v = c.json("DELETE", &format!("/blobs/{r}"))?;
                println!("removed {}", v["id"].as_str().unwrap_or(r));
                n += v["freed_chunks"].as_u64().unwrap_or(0);
                bytes += v["freed_bytes"].as_u64().unwrap_or(0);
            }
            eprintln!("freed {n} chunks, {}", human(bytes));
        }

        Cmd::Verify { reference } => {
            let started = Instant::now();
            let v = c.json("POST", &format!("/blobs/{reference}/verify"))?;
            let id = v["id"].as_str().unwrap_or(&reference);
            if v["ok"].as_bool().unwrap_or(false) {
                println!(
                    "ok  {id}  {} in {}",
                    human(v["size"].as_u64().unwrap_or(0)),
                    store::since(started)
                );
            } else {
                println!("FAILED  {id}");
                std::process::exit(1);
            }
        }

        Cmd::Gc => {
            let v = c.json("POST", "/gc")?;
            println!(
                "removed {} unreferenced chunks, {}",
                v["chunks"].as_u64().unwrap_or(0),
                human(v["bytes"].as_u64().unwrap_or(0))
            );
        }

        Cmd::Stats => {
            let v = c.json("GET", "/stats")?;
            let g = |k: &str| v[k].as_u64().unwrap_or(0);
            let ing = match g("ingesting") {
                0 => String::new(),
                n => format!(" ({n} ingesting)"),
            };
            println!("blobs          {}{ing}", g("blobs"));
            println!("distinct roots {}", g("roots"));
            println!(
                "chunks         {} unique, {} referenced",
                g("unique_chunks"),
                g("chunk_refs")
            );
            let (u, r) = (g("unique_chunks"), g("chunk_refs"));
            let dedup = if r > 0 { 100.0 * (r - u) as f64 / r as f64 } else { 0.0 };
            println!("dedup          {dedup:.1}%");
            println!("logical        {:>7}  what the blobs contain", human(g("logical")));
            println!(
                "stored         {:>7}  unique chunk bytes, {:.1}x from dedup",
                human(g("stored")),
                ratio(g("logical"), g("stored"))
            );
            if g("disk") > g("stored") {
                println!(
                    "on disk        {:>7}  includes journal and space pending compaction",
                    human(g("disk"))
                );
            } else {
                println!(
                    "on disk        {:>7}  {:.1}x smaller again after Lz4, {:.1}x overall",
                    human(g("disk")),
                    ratio(g("stored"), g("disk")),
                    ratio(g("logical"), g("disk"))
                );
            }
        }
    }
    Ok(())
}
