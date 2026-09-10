//! A blocking HTTP/1.1 client over the unix socket.
//!
//! The same sans-IO codec as the daemon, in the other role. Blocking rather
//! than polled: a CLI does one request at a time, and there is nothing else
//! for this thread to do while it waits.
//!
//! HTTP/1.1 rather than HTTP/2, even though the daemon prefers HTTP/2, because
//! the only thing HTTP/2 buys a client here is the early id on an upload, and
//! a CLI that is about to block until the upload finishes cannot use it.

use anyhow::{bail, Context, Result};
use h11r::{Connection, Event, Limits, Method, NextEvent, Request, Role, Version};
use std::io::{Read, Write};
use std::path::Path;

#[cfg(unix)]
use std::os::unix::net::UnixStream;
#[cfg(windows)]
use win_uds::net::UnixStream;

pub struct Client {
    sock: UnixStream,
    conn: Connection,
    buf: Vec<u8>,
}

pub struct Reply {
    pub status: u16,
}

impl Client {
    /// Connect, or `None` if nothing is listening. A missing daemon is the
    /// normal case, not an error: the CLI just opens the store itself.
    pub fn connect(socket: &Path) -> Option<Self> {
        let sock = UnixStream::connect(socket).ok()?;
        Some(Self {
            sock,
            conn: Connection::new(Role::Client, Limits::default()),
            buf: vec![0u8; 64 * 1024],
        })
    }

    /// Send a request, streaming `body` if there is one, then read the
    /// response head. The body is left on the socket for `read_body`.
    pub fn send(
        &mut self,
        method: &str,
        path: &str,
        ctype: Option<&str>,
        body: Option<&mut dyn Read>,
        mut tee: Option<&mut dyn Write>,
    ) -> Result<Reply> {
        let mut headers: Vec<(Vec<u8>, Vec<u8>)> =
            vec![(b"host".to_vec(), b"klump".to_vec())];
        if let Some(c) = ctype {
            headers.push((b"content-type".to_vec(), c.as_bytes().to_vec()));
        }
        if body.is_some() {
            // Unknown length: the CLI is usually piping from stdin.
            headers.push((b"transfer-encoding".to_vec(), b"chunked".to_vec()));
        } else {
            headers.push((b"content-length".to_vec(), b"0".to_vec()));
        }
        let req = Request {
            method: Method::from_bytes(method.as_bytes()).map_err(|e| anyhow::anyhow!("{e:?}"))?,
            target: path.as_bytes().to_vec(),
            headers,
            http_version: Version::Http11,
        };
        let head = self.conn.send_request(&req).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        self.sock.write_all(&head)?;

        if let Some(r) = body {
            let mut chunk = vec![0u8; 64 * 1024];
            loop {
                let n = match r.read(&mut chunk) {
                    Ok(0) => break,
                    Ok(n) => n,
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
                    Err(e) => return Err(e.into()),
                };
                if let Some(w) = tee.as_mut() {
                    w.write_all(&chunk[..n])?;
                    w.flush()?;
                }
                let framed = self
                    .conn
                    .send_data(&chunk[..n])
                    .map_err(|e| anyhow::anyhow!("{e:?}"))?;
                self.sock.write_all(&framed)?;
            }
        }
        let end = self.conn.end_of_message(&[]).map_err(|e| anyhow::anyhow!("{e:?}"))?;
        self.sock.write_all(&end)?;
        self.sock.flush()?;

        loop {
            match self.conn.next_event() {
                Ok(NextEvent::Event(Event::Response(r))) => {
                    return Ok(Reply { status: r.status.as_u16() })
                }
                Ok(NextEvent::Event(Event::ConnectionClosed)) => bail!("daemon closed the connection"),
                Ok(NextEvent::Event(_)) => continue,
                Ok(NextEvent::NeedData) | Ok(NextEvent::Paused) => self.fill()?,
                Err(e) => bail!("daemon spoke badly: {e:?}"),
            }
        }
    }

    /// Stream the response body to `out`. Returns bytes written.
    pub fn read_body<W: Write>(&mut self, out: &mut W) -> Result<u64> {
        let mut total = 0u64;
        loop {
            // Copy out of the codec's borrow before touching the socket again.
            let step = match self.conn.next_event() {
                Ok(NextEvent::Event(Event::Data(d))) => Some(d.data.to_vec()),
                Ok(NextEvent::Event(Event::EndOfMessage(_))) => {
                    out.flush()?;
                    return Ok(total);
                }
                Ok(NextEvent::Event(Event::ConnectionClosed)) => {
                    out.flush()?;
                    return Ok(total);
                }
                Ok(NextEvent::Event(_)) => None,
                Ok(NextEvent::NeedData) | Ok(NextEvent::Paused) => {
                    self.fill()?;
                    continue;
                }
                Err(e) => bail!("daemon spoke badly: {e:?}"),
            };
            if let Some(bytes) = step {
                out.write_all(&bytes)?;
                out.flush()?;
                total += bytes.len() as u64;
            }
        }
    }

    fn fill(&mut self) -> Result<()> {
        let n = self.sock.read(&mut self.buf).context("reading from daemon")?;
        if n == 0 {
            self.conn.receive_data(&[]).ok();
            bail!("daemon closed the connection");
        }
        let bytes = self.buf[..n].to_vec();
        self.conn
            .receive_data(&bytes)
            .map_err(|e| anyhow::anyhow!("{e:?}"))?;
        Ok(())
    }

    /// The common case: a request with no body whose reply is one JSON value.
    pub fn json(&mut self, method: &str, path: &str) -> Result<serde_json::Value> {
        let reply = self.send(method, path, None, None, None)?;
        let mut body = Vec::new();
        self.read_body(&mut body)?;
        let v: serde_json::Value = serde_json::from_slice(&body)
            .with_context(|| format!("{method} {path}: bad JSON from daemon"))?;
        if reply.status >= 400 {
            bail!(
                "{}",
                v.get("error").and_then(|e| e.as_str()).unwrap_or("request failed")
            );
        }
        Ok(v)
    }
}
