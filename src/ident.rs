//! Identities. Two of them, answering different questions.
//!
//! A blob id is a scru128: 25 Base36 digits, case-insensitive, sortable by
//! creation time. It is a handle, minted when ingest opens, before anyone
//! knows what the content will be.
//!
//! A root is the BLAKE3 hash of the content: 32 bytes, rendered as 52
//! lowercase base32 digits. It is an identity, known only at EOF, and two
//! blobs holding the same bytes share it.
//!
//! Both render as lowercase alphanumerics that survive a URL, a filename and
//! a shell without escaping. Hashes are stored raw and encoded only at the
//! edges.

use anyhow::{bail, Result};
use data_encoding::BASE32_NOPAD_NOCASE;
use scru128::Scru128Id;

pub type Hash = [u8; 32];

/// 52 lowercase base32 digits. No padding, no algorithm prefix: the hash
/// function is a property of the store, not of every identity it hands out.
pub fn encode(hash: &Hash) -> String {
    BASE32_NOPAD_NOCASE.encode(hash).to_lowercase()
}

pub fn decode(s: &str) -> Result<Hash> {
    let raw = BASE32_NOPAD_NOCASE
        .decode(s.as_bytes())
        .map_err(|_| anyhow::anyhow!("not a base32 hash: {s}"))?;
    let n = raw.len();
    raw.try_into()
        .map_err(|_| anyhow::anyhow!("hash is {n} bytes, want 32"))
}

/// What the user typed. Every command that names a blob takes any of these,
/// so `klump get` works with a handle, a short prefix of one, or the
/// content's own name.
#[derive(Debug, Clone)]
pub enum Ref {
    Id(Scru128Id),
    /// A prefix of a blob id, resolved against the store and required to be
    /// unambiguous.
    IdPrefix(String),
    Root(Hash),
}

pub fn parse_ref(s: &str) -> Result<Ref> {
    let s = s.trim();
    if s.is_empty() {
        bail!("empty reference");
    }
    // Lengths make the two full forms unambiguous: 52 for a hash, 25 for an id.
    if s.len() == 52 {
        return Ok(Ref::Root(decode(s)?));
    }
    if let Ok(id) = s.parse::<Scru128Id>() {
        return Ok(Ref::Id(id));
    }
    if s.len() < 25 && s.chars().all(|c| c.is_ascii_alphanumeric()) {
        return Ok(Ref::IdPrefix(s.to_lowercase()));
    }
    bail!("not a blob id or hash: {s}")
}

pub fn human(bytes: u64) -> String {
    const U: [&str; 5] = ["B", "K", "M", "G", "T"];
    let mut v = bytes as f64;
    let mut i = 0;
    while v >= 1024.0 && i < U.len() - 1 {
        v /= 1024.0;
        i += 1;
    }
    if i == 0 {
        format!("{bytes}B")
    } else if v < 10.0 {
        format!("{v:.1}{}", U[i])
    } else {
        format!("{v:.0}{}", U[i])
    }
}
