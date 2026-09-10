//! The daemon.
//!
//! One thread, one loop, no runtime. `polling` reports which sockets are ready
//! (epoll, kqueue, or wepoll over IOCP on Windows), and a sans-IO codec turns
//! bytes into events and events back into bytes. Neither performs any IO.
//! Everything that touches a socket is in this file.
//!
//! Two protocols, chosen per connection by sniffing the first 24 bytes against
//! HTTP/2's fixed connection preface. HTTP/1.1 is what every client already
//! speaks; HTTP/2 is what makes one connection carry an upload and several
//! followers at once, gives per-stream flow control, and lets a response start
//! before the request body ends, which is how `POST /blobs` can hand back an id
//! a subscriber can follow immediately.

use anyhow::{Context, Result};
use polling::{Event as PollEvent, Events, PollMode, Poller};
use std::collections::HashMap;
use std::io::{ErrorKind, Read, Write};
use std::path::Path;
use std::time::Duration;

use h11r::{Event as H1Event, NextEvent, Response as H1Response, StatusCode, Version};
use shiguredo_http2::{Event as H2Event, HeaderField, StreamId, CONNECTION_PREFACE};

#[cfg(unix)]
use std::os::unix::net::{UnixListener, UnixStream};
#[cfg(windows)]
use win_uds::net::{UnixListener, UnixStream};

use crate::store::Store;
use scru128::Scru128Id;

const LISTENER_KEY: usize = 0;
/// How much unwritten response we are willing to hold per connection before
/// pausing the reader that is producing it.
const OUT_HIGH_WATER: usize = 1 << 20;

/// Which request a response belongs to. HTTP/1.1 has one in flight per
/// connection, HTTP/2 has many, so only one of these carries an id.
#[derive(Clone, Copy, Debug)]
enum ReqId {
    H1,
    H2(StreamId),
}

/// What a codec told us happened, lifted out of the codec's borrow so we can
/// turn round and write to it.
enum Incoming {
    Request { id: ReqId, method: String, path: String, ctype: String },
    Body { id: ReqId, data: Vec<u8>, end: bool },
    End(ReqId),
    Closed,
    /// An event we do not act on. Keep pulling: the queue may hold more.
    Ignored,
    /// The codec has nothing left. Stop pulling.
    Idle,
}

enum Codec {
    /// Fewer than 24 bytes seen; still deciding.
    Sniffing(Vec<u8>),
    H1(Box<h11r::Connection>),
    H2(Box<shiguredo_http2::Connection>),
}

/// Everything in flight on one connection. HTTP/1.1 keys on 0 because it has
/// one request at a time; HTTP/2 keys on the stream id.
enum Req {
    Ingest(crate::store::Ingest),
}

pub struct Server {
    store: Store,
    handles: Handles,
}

struct Conn {
    sock: UnixStream,
    codec: Codec,
    out: Vec<u8>,
    dead: bool,
    reqs: HashMap<u32, Req>,
    outs: Vec<Out>,
}

impl Conn {
    fn new(sock: UnixStream) -> Self {
        Self {
            sock,
            codec: Codec::Sniffing(Vec::new()),
            out: Vec::new(),
            dead: false,
            reqs: HashMap::new(),
            outs: Vec::new(),
        }
    }

    /// Decide the protocol once there is enough evidence, then replay the
    /// bytes we held back into the codec we picked.
    fn feed(&mut self, bytes: &[u8]) -> Result<()> {
        if let Codec::Sniffing(held) = &mut self.codec {
            held.extend_from_slice(bytes);
            let n = held.len().min(CONNECTION_PREFACE.len());
            if held[..n] == CONNECTION_PREFACE[..n] {
                if held.len() < CONNECTION_PREFACE.len() {
                    return Ok(()); // still a possible preface; wait for more
                }
                let replay = std::mem::take(held);
                let mut h2 = shiguredo_http2::Connection::new(
                    shiguredo_http2::Role::Server,
                    shiguredo_http2::Limits::default(),
                );
                h2.initiate()?;
                self.codec = Codec::H2(Box::new(h2));
                return self.feed_codec(&replay);
            }
            let replay = std::mem::take(held);
            self.codec = Codec::H1(Box::new(h11r::Connection::new(
                h11r::Role::Server,
                h11r::Limits::default(),
            )));
            return self.feed_codec(&replay);
        }
        self.feed_codec(bytes)
    }

    fn feed_codec(&mut self, bytes: &[u8]) -> Result<()> {
        match &mut self.codec {
            Codec::Sniffing(_) => Ok(()),
            Codec::H1(c) => c
                .receive_data(bytes)
                .map_err(|e| anyhow::anyhow!("h1: {e:?}")),
            Codec::H2(c) => {
                let mut fed = 0;
                while fed < bytes.len() {
                    let n = c.feed(&bytes[fed..])?;
                    c.process()?;
                    // feed() consuming nothing means it cannot make progress
                    // with what it has; spinning here would wedge the loop.
                    if n == 0 {
                        break;
                    }
                    fed += n;
                }
                Ok(())
            }
        }
    }

    fn next(&mut self) -> Result<Incoming> {
        match &mut self.codec {
            Codec::Sniffing(_) => Ok(Incoming::Idle),
            Codec::H1(c) => {
                let ev = match c.next_event() {
                    Ok(NextEvent::Event(e)) => e,
                    Ok(NextEvent::NeedData) | Ok(NextEvent::Paused) => return Ok(Incoming::Idle),
                    Err(e) => return Err(anyhow::anyhow!("h1: {e:?}")),
                };
                Ok(match ev {
                    H1Event::Request(r) => Incoming::Request {
                        id: ReqId::H1,
                        method: String::from_utf8_lossy(r.method.as_bytes()).into_owned(),
                        path: String::from_utf8_lossy(&r.target).into_owned(),
                        ctype: r
                            .headers
                            .iter()
                            .find(|(k, _)| k.eq_ignore_ascii_case(b"content-type"))
                            .map(|(_, v)| String::from_utf8_lossy(v).into_owned())
                            .unwrap_or_default(),
                    },
                    H1Event::Data(d) => Incoming::Body {
                        id: ReqId::H1,
                        data: d.data.to_vec(),
                        end: false,
                    },
                    H1Event::EndOfMessage(_) => Incoming::End(ReqId::H1),
                    H1Event::ConnectionClosed => Incoming::Closed,
                    _ => Incoming::Ignored,
                })
            }
            Codec::H2(c) => {
                let Some(ev) = c.poll_event() else { return Ok(Incoming::Idle) };
                Ok(match ev {
                    H2Event::HeadersReceived { stream_id, headers, end_stream, .. } => {
                        let get = |n: &str| {
                            headers
                                .iter()
                                .find(|h| h.name() == n.as_bytes())
                                .map(|h| String::from_utf8_lossy(h.value()).into_owned())
                                .unwrap_or_default()
                        };
                        let _ = end_stream;
                        Incoming::Request {
                            id: ReqId::H2(stream_id),
                            method: get(":method"),
                            path: get(":path"),
                            ctype: get("content-type"),
                        }
                    }
                    H2Event::DataReceived { stream_id, data, end_stream, .. } => {
                        // Replenish the flow-control window we just consumed, for
                        // the stream and for the connection. Without this a client
                        // stops dead after the initial 65535 bytes, waiting for a
                        // WINDOW_UPDATE that never comes.
                        if !data.is_empty() {
                            let n = data.len() as u32;
                            c.send_window_update(stream_id, n)?;
                            c.send_window_update(StreamId::Connection, n)?;
                        }
                        Incoming::Body { id: ReqId::H2(stream_id), data, end: end_stream }
                    }
                    H2Event::StreamClosed { stream_id, .. } => Incoming::End(ReqId::H2(stream_id)),
                    H2Event::ConnectionError { .. } | H2Event::GoawayReceived { .. } => {
                        Incoming::Closed
                    }
                    _ => Incoming::Ignored,
                })
            }
        }
    }

    fn read_in(&mut self, buf: &mut [u8]) -> Result<bool> {
        loop {
            match self.sock.read(buf) {
                Ok(0) => return Ok(false),
                Ok(n) => self.feed(&buf[..n])?,
                Err(e) if e.kind() == ErrorKind::WouldBlock => return Ok(true),
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
    }

    fn write_out(&mut self) -> Result<()> {
        if let Codec::H2(c) = &mut self.codec {
            while let Some(chunk) = c.poll_output() {
                self.out.extend_from_slice(&chunk);
            }
        }
        let mut sent = 0;
        while sent < self.out.len() {
            match self.sock.write(&self.out[sent..]) {
                Ok(0) => break,
                Ok(n) => sent += n,
                Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                Err(e) if e.kind() == ErrorKind::Interrupted => continue,
                Err(e) => return Err(e.into()),
            }
        }
        self.out.drain(..sent);
        Ok(())
    }

    /// Whether to stop feeding this response for now. Either our own
    /// outbound buffer is deep enough, or HTTP/2 flow control says the
    /// peer has not made room.
    fn congested(&self, id: ReqId) -> bool {
        if self.out.len() >= OUT_HIGH_WATER {
            return true;
        }
        match (&self.codec, id) {
            (Codec::H2(c), ReqId::H2(sid)) => c.has_pending_send_data(sid),
            _ => false,
        }
    }

    fn wants_write(&self) -> bool {
        !self.out.is_empty() || matches!(&self.codec, Codec::H2(c) if c.has_output())
    }

}

pub fn serve(store: Store, socket: &Path) -> Result<()> {
    let mut sv = Server { store, handles: Handles::default() };
    if let Some(dir) = socket.parent() {
        std::fs::create_dir_all(dir).ok();
    }
    let _ = std::fs::remove_file(socket);
    let listener =
        UnixListener::bind(socket).with_context(|| format!("binding {}", socket.display()))?;
    listener.set_nonblocking(true)?;

    let poller = Poller::new()?;
    // Level-triggered: we do not always drain a socket completely, and oneshot
    // would leave the remainder unannounced until something else woke us.
    unsafe { poller.add_with_mode(&listener, PollEvent::readable(LISTENER_KEY), PollMode::Level)? };

    let mut conns: HashMap<usize, Conn> = HashMap::new();
    let mut events = Events::new();
    let mut buf = vec![0u8; 64 * 1024];
    let mut next_key = 1usize;

    eprintln!("klump serving {} on {}", sv.store.path().display(), socket.display());

    loop {
        events.clear();
        // The timeout is only a backstop. A follower is normally served in the
        // same pass that stored the chunk, because the writer and the readers
        // are the same thread.
        poller.wait(&mut events, Some(Duration::from_millis(200)))?;

        for ev in events.iter() {
            if ev.key == LISTENER_KEY {
                loop {
                    match listener.accept() {
                        Ok((sock, _)) => {
                            sock.set_nonblocking(true)?;
                            let key = next_key;
                            next_key += 1;
                            let conn = Conn::new(sock);
                            unsafe {
                                poller.add_with_mode(
                                    &conn.sock,
                                    PollEvent::all(key),
                                    PollMode::Level,
                                )?
                            };
                            conns.insert(key, conn);
                        }
                        Err(e) if e.kind() == ErrorKind::WouldBlock => break,
                        Err(e) => return Err(e.into()),
                    }
                }
                continue;
            }
            let Some(conn) = conns.get_mut(&ev.key) else { continue };
            if ev.readable {
                match conn.read_in(&mut buf) {
                    Ok(true) => {}
                    Ok(false) => conn.dead = true,
                    Err(e) => {
                        eprintln!("klump: conn {}: {e:#}", ev.key);
                        conn.dead = true;
                    }
                }
            }
            if !conn.dead {
                if let Err(e) = drain(conn, &mut sv) {
                    eprintln!("klump: conn {}: {e:#}", ev.key);
                    conn.dead = true;
                }
            }
        }

        // Every connection, every pass. A blob written on one connection has
        // followers on others, and they are owed the chunk now rather than at
        // the next timeout.
        let mut reap = Vec::new();
        for (key, conn) in conns.iter_mut() {
            if !conn.dead {
                if let Err(e) = pump(conn, &mut sv) {
                    eprintln!("klump: conn {key}: {e:#}");
                    conn.dead = true;
                }
            }
            if let Err(e) = conn.write_out() {
                eprintln!("klump: conn {key}: {e:#}");
                conn.dead = true;
            }
            if conn.dead && conn.out.is_empty() {
                reap.push(*key);
            } else {
                let interest = if conn.wants_write() {
                    PollEvent::all(*key)
                } else {
                    PollEvent::readable(*key)
                };
                poller.modify_with_mode(&conn.sock, interest, PollMode::Level)?;
            }
        }
        for key in reap {
            if let Some(conn) = conns.remove(&key) {
                // Anything this connection still held open is now closed, so a
                // blob unlinked while it was reading can finally be reclaimed.
                for out in &conn.outs {
                    sv.handles.close(&sv.store, out.id);
                }
                for req in conn.reqs.values() {
                    let Req::Ingest(ing) = req;
                    sv.handles.close(&sv.store, ing.id);
                }
                let _ = poller.delete(&conn.sock);
            }
        }
    }
}

/// HTTP/1.1 keep-alive: the connection can carry another request only once
/// both halves have finished this one. Cycling any earlier is refused, and
/// swallowing that refusal leaves the connection wedged.
fn maybe_cycle(conn: &mut Conn) {
    if let Codec::H1(c) = &mut conn.codec {
        if c.local_state() == h11r::State::Done && c.peer_state() == h11r::State::Done {
            if c.start_next_cycle().is_err() {
                conn.dead = true;
            }
        }
    }
}

/// Pull every event the codec has and act on it.
fn drain(conn: &mut Conn, sv: &mut Server) -> Result<()> {
    loop {
        match conn.next()? {
            Incoming::Idle => return Ok(()),
            Incoming::Ignored => {}
            Incoming::Closed => {
                conn.dead = true;
                return Ok(());
            }
            Incoming::Request { id, method, path, ctype } => {
                route(conn, sv, id, &method, &path, ctype)?;
            }
            Incoming::Body { id, data, end } => {
                if let Some(Req::Ingest(ing)) = conn.reqs.get_mut(&Conn::key(id)) {
                    sv.store.append(ing, &data)?;
                }
                if end {
                    finish_ingest(conn, sv, id)?;
                }
            }
            Incoming::End(id) => {
                finish_ingest(conn, sv, id)?;
                maybe_cycle(conn);
            }
        }
    }
}

/// Seal an upload and send the trailer as the last line of the response.
///
/// The body is newline-delimited JSON: the id went out the moment the blob
/// was opened, and this closes with what only EOF could tell us.
fn finish_ingest(conn: &mut Conn, sv: &mut Server, id: ReqId) -> Result<()> {
    let Some(Req::Ingest(ing)) = conn.reqs.remove(&Conn::key(id)) else {
        return Ok(());
    };
    let bid = ing.id;
    let trailer = sv.store.finish(ing)?;
    sv.handles.close(&sv.store, bid);
    let body = json(&serde_json::json!({
        "id": bid.to_string(),
        "size": trailer.size,
        "chunks": trailer.chunks,
        "root": trailer.root,
    }));
    conn.body(id, &body, true)?;
    maybe_cycle(conn);
    Ok(())
}

// --- request handling ---------------------------------------------------

/// Open handles, and blobs whose name is gone but whose content is still
/// being read or written.
///
/// This is the Unix file model. `DELETE` unlinks: the blob leaves the
/// namespace at once, and its chunks survive while anyone holds it open.
/// Because the daemon is the only thing touching the store and the loop is
/// single threaded, this count is authoritative and needs no locking.
#[derive(Default)]
pub struct Handles {
    open: HashMap<Scru128Id, usize>,
    pending: HashMap<Scru128Id, Vec<[u8; 32]>>,
}

impl Handles {
    fn open(&mut self, id: Scru128Id) {
        *self.open.entry(id).or_insert(0) += 1;
    }

    /// Drop a handle, reclaiming the blob's chunks if it was unlinked while
    /// we held it and we were the last one out.
    fn close(&mut self, store: &Store, id: Scru128Id) {
        if let Some(n) = self.open.get_mut(&id) {
            *n -= 1;
            if *n == 0 {
                self.open.remove(&id);
                if let Some(mut hashes) = self.pending.remove(&id) {
                    // A writer holding an unlinked blob keeps writing, exactly
                    // as it would to a deleted file. Those later chunks gave it
                    // a name again, so take that back before reclaiming.
                    if let Ok(more) = store.unlink(&id) {
                        hashes.extend(more);
                    }
                    hashes.sort_unstable();
                    hashes.dedup();
                    let _ = store.reclaim(&hashes);
                }
            }
        }
    }

    fn is_open(&self, id: &Scru128Id) -> bool {
        self.open.contains_key(id)
    }
}

/// A response still being written out. The loop pumps these every pass, so a
/// follower sees a chunk in the same iteration that stored it.
struct Out {
    /// Bytes of the current chunk the peer has not taken yet.
    carry: Vec<u8>,
    req: ReqId,
    id: Scru128Id,
    next: u32,
    follow: bool,
}

fn json(v: &serde_json::Value) -> Vec<u8> {
    let mut s = serde_json::to_vec(v).unwrap_or_default();
    s.push(b'\n');
    s
}

fn split_path(path: &str) -> (String, HashMap<String, String>) {
    let (p, q) = path.split_once('?').unwrap_or((path, ""));
    let query = q
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, "1"));
            (k.to_string(), v.to_string())
        })
        .collect();
    (p.to_string(), query)
}

impl Conn {
    fn key(id: ReqId) -> u32 {
        match id {
            ReqId::H1 => 0,
            ReqId::H2(s) => s.as_u32(),
        }
    }

    /// Response head only. `len` fixes Content-Length; None means the length
    /// is not known yet, which on HTTP/1.1 has to be chunked.
    fn head(&mut self, id: ReqId, status: u16, extra: Vec<(String, String)>, len: Option<u64>) -> Result<()> {
        match (&mut self.codec, id) {
            (Codec::H1(c), _) => {
                let mut headers: Vec<(Vec<u8>, Vec<u8>)> = extra
                    .iter()
                    .map(|(k, v)| (k.as_bytes().to_vec(), v.as_bytes().to_vec()))
                    .collect();
                match len {
                    Some(n) => headers
                        .push((b"content-length".to_vec(), n.to_string().into_bytes())),
                    None => headers
                        .push((b"transfer-encoding".to_vec(), b"chunked".to_vec())),
                }
                let resp = H1Response {
                    status: StatusCode::try_from(status).map_err(|e| anyhow::anyhow!("{e:?}"))?,
                    reason: Vec::new(),
                    headers,
                    http_version: Version::Http11,
                };
                let bytes = c.send_response(&resp).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                self.out.extend_from_slice(&bytes);
            }
            (Codec::H2(c), ReqId::H2(sid)) => {
                let mut headers = vec![HeaderField::new(":status", &status.to_string())?];
                for (k, v) in &extra {
                    headers.push(HeaderField::new(k, v)?);
                }
                if let Some(n) = len {
                    headers.push(HeaderField::new("content-length", &n.to_string())?);
                }
                c.send_response(sid, headers, false)?;
            }
            _ => {}
        }
        Ok(())
    }

    fn body(&mut self, id: ReqId, data: &[u8], end: bool) -> Result<()> {
        match (&mut self.codec, id) {
            (Codec::H1(c), _) => {
                if !data.is_empty() {
                    let b = c.send_data(data).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                    self.out.extend_from_slice(&b);
                }
                if end {
                    let b = c.end_of_message(&[]).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                    self.out.extend_from_slice(&b);
                }
            }
            (Codec::H2(c), ReqId::H2(sid)) => {
                if !data.is_empty() || end {
                    c.send_data(sid, data.to_vec(), end)?;
                }
                // Move it out of the codec now rather than in write_out, so
                // the high-water check below sees these bytes. Otherwise a
                // peer advertising a large window lets us queue a whole blob
                // into the codec before anything notices.
                while let Some(chunk) = c.poll_output() {
                    self.out.extend_from_slice(&chunk);
                }
            }
            _ => {}
        }
        Ok(())
    }

    fn reply(&mut self, id: ReqId, status: u16, ctype: &str, body: &[u8]) -> Result<()> {
        self.head(
            id,
            status,
            vec![("content-type".into(), ctype.into())],
            Some(body.len() as u64),
        )?;
        self.body(id, body, true)?;
        maybe_cycle(self);
        Ok(())
    }

    fn fail(&mut self, id: ReqId, status: u16, msg: &str) -> Result<()> {
        let b = json(&serde_json::json!({ "error": msg }));
        self.reply(id, status, "application/json", &b)
    }
}

fn blob_json(b: &crate::store::Blob) -> serde_json::Value {
    let mut v = serde_json::json!({
        "id": b.id.to_string(),
        "content_type": b.content_type,
        "status": b.status(),
    });
    if let Some(t) = &b.trailer {
        v["size"] = serde_json::json!(t.size);
        v["chunks"] = serde_json::json!(t.chunks);
        v["root"] = serde_json::json!(t.root);
    }
    v
}

/// Route one request. Bodies arrive later as `Incoming::Body`, so anything
/// that consumes one only sets up state here.
fn route(conn: &mut Conn, sv: &mut Server, id: ReqId, method: &str, raw_path: &str, ctype: String) -> Result<()> {
    let (path, query) = split_path(raw_path);
    let key = Conn::key(id);

    let resolve = |sv: &Server, s: &str| -> Option<Scru128Id> {
        crate::ident::parse_ref(s).ok().and_then(|r| sv.store.resolve(&r).ok())
    };

    match (method, path.as_str()) {
        ("POST", "/blobs") => {
            let ing = sv.store.begin(&ctype)?;
            let bid = ing.id;
            sv.handles.open(bid);
            conn.reqs.insert(key, Req::Ingest(ing));
            // The id exists now, so hand it over now. On HTTP/2 this reaches
            // the client while it is still sending, which is the whole point.
            let body = json(&serde_json::json!({ "id": bid.to_string() }));
            conn.head(id, 202, vec![("content-type".into(), "application/json".into())], None)?;
            conn.body(id, &body, false)?;
        }

        ("GET", "/blobs") => {
            let mut arr = Vec::new();
            for bid in sv.store.ids()? {
                if let Some(b) = sv.store.load(&bid)? {
                    arr.push(blob_json(&b));
                }
            }
            let body = json(&serde_json::Value::Array(arr));
            conn.reply(id, 200, "application/json", &body)?;
        }

        ("GET", "/stats") => {
            let s = sv.store.stats()?;
            let body = json(&serde_json::json!({
                "blobs": s.blobs, "ingesting": s.ingesting, "roots": s.roots,
                "unique_chunks": s.unique_chunks, "chunk_refs": s.chunk_refs,
                "logical": s.logical, "stored": s.stored, "disk": s.disk,
            }));
            conn.reply(id, 200, "application/json", &body)?;
        }

        ("POST", "/gc") => {
            let (n, bytes) = sv.store.gc()?;
            let body = json(&serde_json::json!({ "chunks": n, "bytes": bytes }));
            conn.reply(id, 200, "application/json", &body)?;
        }

        (m, p) if p.starts_with("/blobs/") => {
            let rest = &p["/blobs/".len()..];
            let (r, action) = rest.split_once('/').unwrap_or((rest, ""));
            let Some(bid) = resolve(sv, r) else {
                return conn.fail(id, 404, "no such blob");
            };
            let Some(blob) = sv.store.load(&bid)? else {
                return conn.fail(id, 404, "no such blob");
            };
            match (m, action) {
                ("POST", "verify") => {
                    let (ok, size) = sv.store.verify(&bid)?;
                    let body = json(&serde_json::json!({
                        "id": bid.to_string(), "ok": ok, "size": size,
                    }));
                    conn.reply(id, if ok { 200 } else { 409 }, "application/json", &body)?;
                }
                ("GET", _) if query.contains_key("meta") => {
                    let mut v = blob_json(&blob);
                    if let Some(t) = &blob.trailer {
                        let holders: Vec<String> = sv
                            .store
                            .by_root(&crate::ident::decode(&t.root)?)?
                            .iter()
                            .filter(|h| **h != bid)
                            .map(|h| h.to_string())
                            .collect();
                        v["also_held_by"] = serde_json::json!(holders);
                    }
                    let body = json(&v);
                    conn.reply(id, 200, "application/json", &body)?;
                }
                ("HEAD", _) => {
                    let mut extra = vec![("content-type".into(), blob.content_type.clone())];
                    if let Some(t) = &blob.trailer {
                        extra.push(("klump-root".into(), t.root.clone()));
                        extra.push(("klump-chunks".into(), t.chunks.to_string()));
                    }
                    extra.push(("klump-id".into(), blob.id.to_string()));
                    extra.push(("klump-status".into(), blob.status().into()));
                    conn.head(id, 200, extra, Some(blob.size().unwrap_or(0)))?;
                    conn.body(id, &[], true)?;
                    maybe_cycle(conn);
                }
                ("GET", _) => {
                    let follow = query.contains_key("follow");
                    let len = if follow { None } else { blob.size() };
                    sv.handles.open(bid);
                    let mut extra = vec![("content-type".into(), blob.content_type.clone())];
                    if let Some(t) = &blob.trailer {
                        extra.push(("klump-root".into(), t.root.clone()));
                    }
                    conn.head(id, 200, extra, len)?;
                    conn.outs.push(Out { carry: Vec::new(), req: id, id: bid, next: 1, follow });
                }
                ("DELETE", _) => {
                    let hashes = sv.store.unlink(&bid)?;
                    let (n, bytes) = if sv.handles.is_open(&bid) {
                        // Someone still holds it. Reclaim when they let go.
                        sv.handles.pending.insert(bid, hashes);
                        (0, 0)
                    } else {
                        sv.store.reclaim(&hashes)?
                    };
                    let body = json(&serde_json::json!({
                        "id": bid.to_string(), "freed_chunks": n, "freed_bytes": bytes,
                    }));
                    conn.reply(id, 200, "application/json", &body)?;
                }
                _ => return conn.fail(id, 405, "method not allowed"),
            }
        }

        _ => return conn.fail(id, 404, "no such route"),
    }
    Ok(())
}

/// Push whatever has arrived for each in-flight read. Called every pass, so
/// a follower is served in the same iteration that committed the chunk.
/// One DATA frame's worth. The codec's per-stream send buffer is fixed at
/// 65535 bytes and is never resized, so a 64K chunk cannot be handed over
/// whole; it has to go in pieces that fit alongside whatever flow control has
/// not let out yet.
const PIECE: usize = 16 * 1024;

/// Push as much of this response's carry as the peer will take.
fn drain_carry(conn: &mut Conn, i: usize, req: ReqId) -> Result<()> {
    while !conn.outs[i].carry.is_empty() && !conn.congested(req) {
        let take = PIECE.min(conn.outs[i].carry.len());
        let piece: Vec<u8> = conn.outs[i].carry.drain(..take).collect();
        conn.body(req, &piece, false)?;
    }
    Ok(())
}

fn pump(conn: &mut Conn, sv: &mut Server) -> Result<()> {
    let mut finished = Vec::new();
    for i in 0..conn.outs.len() {
        let (req, bid, from, follow) = {
            let o = &conn.outs[i];
            (o.req, o.id, o.next, o.follow)
        };

        // Whatever the peer would not take last pass goes first.
        drain_carry(conn, i, req)?;
        if !conn.outs[i].carry.is_empty() {
            continue; // still blocked; try again when the window opens
        }

        let (hashes, complete) = sv.store.chunks_from(&bid, from)?;
        let mut sent = 0;
        for h in &hashes {
            if conn.congested(req) {
                break;
            }
            let Some(chunk) = sv.store.chunk(h)? else { continue };
            conn.outs[i].carry = chunk;
            sent += 1;
            drain_carry(conn, i, req)?;
            if !conn.outs[i].carry.is_empty() {
                break;
            }
        }
        conn.outs[i].next = from + sent as u32;

        let drained = sent == hashes.len() && conn.outs[i].carry.is_empty();
        // A blob unlinked mid-read stops growing; end at what we have.
        let gone = sv.store.load(&bid)?.is_none();
        if drained && (complete || gone || !follow) {
            conn.body(req, &[], true)?;
            finished.push((i, bid));
        }
    }
    for (i, bid) in finished.iter().rev() {
        conn.outs.remove(*i);
        sv.handles.close(&sv.store, *bid);
    }
    if !finished.is_empty() {
        maybe_cycle(conn);
    }
    Ok(())
}
