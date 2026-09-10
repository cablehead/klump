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

const LISTENER_KEY: usize = 0;

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
    Request { id: ReqId, method: String, path: String },
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

struct Conn {
    sock: UnixStream,
    codec: Codec,
    out: Vec<u8>,
    dead: bool,
}

impl Conn {
    fn new(sock: UnixStream) -> Self {
        Self { sock, codec: Codec::Sniffing(Vec::new()), out: Vec::new(), dead: false }
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
                    Ok(NextEvent::NeedData) | Ok(NextEvent::Paused) => return Ok(Incoming::Idle),| Ok(NextEvent::Paused) => return Ok(Incoming::Ignored),
                    Err(e) => return Err(anyhow::anyhow!("h1: {e:?}")),
                };
                Ok(match ev {
                    H1Event::Request(r) => Incoming::Request {
                        id: ReqId::H1,
                        method: String::from_utf8_lossy(r.method.as_bytes()).into_owned(),
                        path: String::from_utf8_lossy(&r.target).into_owned(),
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
                        let req = Incoming::Request {
                            id: ReqId::H2(stream_id),
                            method: get(":method"),
                            path: get(":path"),
                        };
                        if end_stream {
                            // the loop will see End on the next poll
                        }
                        req
                    }
                    H2Event::DataReceived { stream_id, data, end_stream, .. } => Incoming::Body {
                        id: ReqId::H2(stream_id),
                        data,
                        end: end_stream,
                    },
                    H2Event::StreamClosed { stream_id, .. } => Incoming::End(ReqId::H2(stream_id)),
                    H2Event::ConnectionError { .. } | H2Event::GoawayReceived { .. } => {
                        Incoming::Closed
                    }
                    _ => Incoming::Ignored,
                })
            }
        }
    }

    /// A complete small response: head, body, end. The two codecs differ only
    /// in whether they hand back bytes or buffer them.
    fn respond(&mut self, id: ReqId, status: u16, ctype: &str, body: &[u8]) -> Result<()> {
        match (&mut self.codec, id) {
            (Codec::H1(c), _) => {
                let resp = H1Response {
                    status: StatusCode::try_from(status).map_err(|e| anyhow::anyhow!("{e:?}"))?,
                    reason: Vec::new(),
                    headers: vec![
                        (b"content-type".to_vec(), ctype.as_bytes().to_vec()),
                        (b"content-length".to_vec(), body.len().to_string().into_bytes()),
                    ],
                    http_version: Version::Http11,
                };
                let mut bytes = c.send_response(&resp).map_err(|e| anyhow::anyhow!("{e:?}"))?;
                bytes.extend(c.send_data(body).map_err(|e| anyhow::anyhow!("{e:?}"))?);
                bytes.extend(c.end_of_message(&[]).map_err(|e| anyhow::anyhow!("{e:?}"))?);
                self.out.extend_from_slice(&bytes);
                Ok(())
            }
            (Codec::H2(c), ReqId::H2(sid)) => {
                c.send_response(
                    sid,
                    vec![
                        HeaderField::new(":status", &status.to_string())?,
                        HeaderField::new("content-type", ctype)?,
                    ],
                    false,
                )?;
                c.send_data(sid, body.to_vec(), true)?;
                Ok(())
            }
            _ => Ok(()),
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

    fn wants_write(&self) -> bool {
        !self.out.is_empty() || matches!(&self.codec, Codec::H2(c) if c.has_output())
    }

    fn proto(&self) -> &'static str {
        match self.codec {
            Codec::Sniffing(_) => "?",
            Codec::H1(_) => "HTTP/1.1",
            Codec::H2(_) => "HTTP/2",
        }
    }
}

pub fn serve(store: Store, socket: &Path) -> Result<()> {
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

    eprintln!("klump serving {} on {}", store.path().display(), socket.display());

    loop {
        events.clear();
        poller.wait(&mut events, Some(Duration::from_millis(500)))?;

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
                if let Err(e) = drain(conn, &store) {
                    eprintln!("klump: conn {}: {e:#}", ev.key);
                    conn.dead = true;
                }
            }
            if !conn.dead {
                if let Err(e) = conn.write_out() {
                    eprintln!("klump: conn {}: {e:#}", ev.key);
                    conn.dead = true;
                }
            }

            if conn.dead && conn.out.is_empty() {
                let conn = conns.remove(&ev.key).expect("present");
                let _ = poller.delete(&conn.sock);
            } else {
                let interest = if conn.wants_write() {
                    PollEvent::all(ev.key)
                } else {
                    PollEvent::readable(ev.key)
                };
                poller.modify_with_mode(&conn.sock, interest, PollMode::Level)?;
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

/// Pull every event the codec has and act on it. Milestone: echo the request
/// line back as JSON, over whichever protocol the client chose.
fn drain(conn: &mut Conn, _store: &Store) -> Result<()> {
    loop {
        match conn.next()? {
            Incoming::Idle => return Ok(()),
            Incoming::Ignored => {}
            Incoming::Closed => {
                conn.dead = true;
                return Ok(());
            }
            Incoming::Request { id, method, path } => {
                let body = format!(
                    "{{\"proto\":\"{}\",\"method\":\"{}\",\"path\":\"{}\"}}\n",
                    conn.proto(),
                    method,
                    path
                );
                conn.respond(id, 200, "application/json", body.as_bytes())?;
                maybe_cycle(conn);
            }
            Incoming::Body { .. } => {}
            Incoming::End(_) => maybe_cycle(conn),| Incoming::End(_) => {}
        }
    }
}
