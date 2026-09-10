<h1>
<p align="center">
  klump
</h1>
  <p align="center">
    Streaming content-addressed blob storage. Chunks go in as they arrive, dedup
    across every blob that shares them, and come back out one chunk at a time.
    <br />
    <a href="#install">Install</a>
    ·
    <a href="#two-names">Two names</a>
    ·
    <a href="#on-disk">On disk</a>
  </p>
</p>

---

klump stores blobs in [fjall](https://github.com/fjall-rs/fjall), an LSM
key-value store. Content is split into 64K chunks and each chunk is stored
under its BLAKE3 hash, so a chunk shared by a hundred blobs is stored once.
Nothing is buffered whole. Chunks are committed as they arrive and read back
one at a time, so a 40 GB video costs the same memory as a 40 KB one.

A blob gets its id the moment ingest opens, before anyone knows what the
content will turn out to be.

## Try it

Two files, the second a copy of the first with 100K appended. Random bytes, so
nothing compresses and the only saving on show is dedup.

```console
$ head -c 1000000 /dev/urandom > a.bin
$ cat a.bin > b.bin && head -c 100000 /dev/urandom >> b.bin

$ klump put ./store a.bin -v
03guauzkos037euah9u9uvz7z
977K in 16 chunks, 1ms at 663M/s, root gboqvz6qmpkjip3nvwtsebdtpn42caociumbxp7pll6kvzbksoka

$ klump put ./store b.bin
03guauzkplma73c5puvr2ke4w

$ klump stats ./store
blobs          2
distinct roots 2
chunks         18 unique, 33 referenced
dedup          45.5%
logical           2.0M  what the blobs contain
stored            1.1M  unique chunk bytes, 1.9x from dedup
on disk           1.1M  includes journal and space pending compaction
```

`b.bin` shares fifteen of its seventeen chunks with `a.bin`, so storing a second
nearly-identical megabyte cost two chunks. Fifteen, not sixteen, because the
chunk straddling the 983040-byte boundary now has new bytes in it.

Blobs come back by id, by an unambiguous prefix of one, or by the hash of
their own content:

```console
$ klump get ./store 03guauzkos > copy.bin
$ klump get ./store gboqvz6qmpkjip3nvwtsebdtpn42caociumbxp7pll6kvzbksoka | cmp - a.bin
$ klump verify ./store 03guauzkos
ok  03guauzkos037euah9u9uvz7z  977K in 0ms
```

Deleting a blob reclaims only the chunks nothing else wants:

```console
$ klump rm ./store 03guauzkpl
removed 03guauzkplma73c5puvr2ke4w
freed 2 chunks, 114K
```

## Install

```
cargo install --path .
```

Every command takes the store directory as its first argument. The daemon's
socket lives inside it as `<store>/sock`, so that one path is all a client and
a server have to agree on.

## Two names

Every blob has two, and they answer different questions.

```
id     03guauzkos037euah9u9uvz7z                             a HANDLE
root   gboqvz6qmpkjip3nvwtsebdtpn42caociumbxp7pll6kvzbksoka  an IDENTITY
```

The **id** is a [scru128](https://github.com/scru128/spec): 25 Base36 digits,
sortable by creation time. It exists from the moment ingest opens, which is
what lets you hand it to a reader before the content has finished arriving.

The **root** is the BLAKE3 hash of the content, 52 lowercase base32 digits. It
is not known until EOF, and two blobs holding the same bytes share it. It is
plain BLAKE3 of the bytes, not a hash over the chunk list, so it does not move
if the chunk size ever changes and `b3sum` computes the same value.

Both are lowercase alphanumerics. They survive a URL, a filename and a shell
with nothing escaped, which is most of why there is no `blake3-` prefix and no
base64 anywhere.

Ingesting the same content twice gives you two ids and one root. That is a
legitimate state, not a mistake, so `roots` is a multimap and `info` will tell
you who else is holding the same bytes:

```console
$ klump put ./store a.bin    # the same bytes, a second time
03guav5z7u0axh832vlvslch1

$ klump info ./store 03guauzkos
id            03guauzkos037euah9u9uvz7z
status        complete
content-type  application/octet-stream
size          977K (1000000 bytes)
chunks        16
root          gboqvz6qmpkjip3nvwtsebdtpn42caociumbxp7pll6kvzbksoka
also held by  03guav5z7u0axh832vlvslch1
```

## Streaming

`put` never holds more than one chunk in memory, and commits each chunk before
reading the next. `get` streams back the same way.

`--tee` writes the content onward as it is stored, so a pipeline captures and
consumes in a single pass. The blob name goes to stderr, because stdout is
carrying the bytes:

```console
$ ffmpeg ... | klump put ./store --tee -t video/mp4 2>id.txt | mpv -
```

`get --follow` keeps reading while a blob is still being ingested and returns
when its trailer lands. That needs a daemon, because the store allows one
process at a time; see [The daemon](#the-daemon).

## On disk

Four keyspaces.

```
cas    32B hash              -> chunk bytes    kv-separated, Lz4
blobs  16B id ++ 4B seq      -> see below      plain
refs   32B hash ++ 16B id    -> ()             plain
roots  32B root ++ 16B id    -> ()             plain
```

`blobs` holds one entry per chunk between two markers:

```
seq 0          content type          known when ingest opens
seq 1..n       32B chunk hash        in order
seq u32::MAX   size, chunks, root    known only at EOF
```

Size and root are trailer fields because they cannot be header fields. You do
not know either one until the last byte arrives. A blob is complete exactly
when its trailer exists, and since `u32::MAX` sorts last, a prefix scan finds
that out by reading to the end of what it was already reading.

`refs` is what makes deletion possible. Chunks are shared, so dropping a blob
may not drop its chunks. One key per (chunk, blob) pair turns "does anything
still want this chunk" into a prefix scan. Within-blob duplicates collapse for
free, because the key is the same both times.

## Deletion

`rm` is two steps, in this order for a reason. First a batch removes the
blob's entries and its refs atomically. Only then is it safe to ask whether a
chunk is orphaned, because this blob's own claim on it is gone.

`gc` sweeps chunks nothing references. A healthy store finds none, since `rm`
reclaims as it goes. It exists because an interrupted `rm` can leave a chunk
stranded, and a safety net you can run beats one you have to trust.

## Why fjall, and what is tuned

Chunks are the only large values, so `cas` is the only keyspace with
key-value separation. The LSM holds a 32-byte key and a value handle, and
compaction moves those rather than the payload. Lz4 on the blobs earns 3.4x on
tarballs and 4x to 7x on most real content.

One default is changed, and it matters:

```rust
.filter_block_pinning_policy(PinningPolicy::all(true))
```

Bloom filters answer "I definitely do not have this chunk" without reading the
table, which is the question every `put` asks. fjall pins those filters at L0
only and leaves deeper ones in the shared block cache. Once the tree compacts
down to one large table, its filter is too big to survive there and every
lookup re-reads the whole thing.

Measured on fjall 3.1.10 at one million chunks, that is the difference between
**271ns and 72us**, and 72us is worse than the 53us write the check exists to
avoid. So the check-then-write becomes a pure loss at exactly the scale where
you need it. Pinning costs about 1.25 MB of memory per million chunks, and
total RSS goes **down**, from 364 MB to 43 MB, because the filter stops
evicting everything else.

klump checks before writing rather than writing blindly. A `contains_key` on
an absent key costs roughly 0.5% of a 64K insert, so it pays for itself above
about 0.5% dedup, and a blind duplicate write would also burn space until blob
GC reclaims it.


## The daemon

fjall gives one process exclusive access to a store, so a long-running klump
would lock every other invocation out. `klump serve` holds the store and speaks
HTTP over a unix socket; the CLI notices the socket and routes through it. Same
commands, same output, either way.

```console
$ klump serve /var/klump &
klump serving /var/klump on /var/klump/sock

$ klump put /var/klump video.mp4 -t video/mp4   # over the socket now
03guee76zwfg4u8rszu96hxfp
```

It speaks **HTTP/1.1 and HTTP/2 on the same socket**, chosen per connection by
matching the first 24 bytes against HTTP/2's connection preface. Plain `curl`
works, so does `curl --http2-prior-knowledge`, and neither needs an `Upgrade`
dance.

```
POST   /blobs               stream an upload, reply is newline-delimited JSON
GET    /blobs               list
GET    /blobs/<ref>         stream content; ?follow keeps reading while it arrives
GET    /blobs/<ref>?meta    metadata as JSON
HEAD   /blobs/<ref>         metadata as headers
DELETE /blobs/<ref>         unlink
POST   /blobs/<ref>/verify  rehash and check against the root
GET    /stats
POST   /gc
```

`<ref>` is a blob id, an unambiguous prefix of one, or a root hash.

### Why HTTP/2 earns its place

On HTTP/1.1 a response cannot usefully begin until the request body finishes,
so an upload only learns its id at the end. HTTP/2 frames request and response
independently, so the id comes back straight away:

```console
$ slow-producer | curl --http2-prior-knowledge --unix-socket /var/klump/sock \
    -X POST -T - http://localhost/blobs
  +   7ms  {"id":"03gued3ej4mpoy46e6gp53ffe"}    <- upload runs for another 3.2s
  ...
  {"chunks":8,"id":"...","root":"...","size":524288}
```

Seven milliseconds into a three second upload you have an id to hand a
follower. That is the whole reason blobs are named before they exist.

Flow control is per stream, so one slow reader is throttled on its own rather
than stalling everything sharing the connection. Serving 60MB across three
concurrent streams peaks at 24MB of memory.

### Following

```console
$ klump get /var/klump <id> --follow
$ curl --unix-socket /var/klump/sock "http://localhost/blobs/<id>?follow=1"
```

Returns when the blob's trailer lands. The writer and every reader are the same
thread, so a follower is served in the same pass that stored the chunk rather
than on a timer.

### Deleting something in use

`DELETE` follows the Unix file model rather than taking a lock. The name goes at
once and the chunks survive while anyone still holds the blob open:

```console
$ klump rm /var/klump <id>   # while a follower is mid-stream
removed <id>
freed 0 chunks, 0B       # deferred: someone still has it open
```

The follower reads to the end of what exists. A writer that was mid-upload
keeps writing, exactly as it would to a deleted file, and its content is
reclaimed when it closes. The race a lock would have guarded is unreachable,
because reclamation only happens when nothing is mid-operation.

## Architecture

One thread, one loop, no async runtime.

```
polling          readiness: epoll, kqueue, or wepoll over IOCP on Windows
h11r             sans-IO HTTP/1.1  receive_data / next_event / send_*
shiguredo_http2  sans-IO HTTP/2    feed / process / poll_event / poll_output
fjall            the store, blocking
```

Neither codec performs IO, so every syscall lives in one file and the loop reads
as a `while`:

```rust
loop {
    poller.wait(&mut events, timeout);
    for c in ready {
        c.feed(read(&mut buf));
        while let Some(ev) = c.next() { route(ev) }
    }
    for c in all { pump(c); write_out(c) }
}
```

Threads were the alternative and they lose here. HTTP/2 multiplexes many streams
over one connection, which thread-per-connection has to demux by hand, and
blocking on fjall would want `spawn_blocking` around every store call. The loop
suits both, and it is why the Windows port is confined to which `UnixListener`
gets imported.

The cost is that fjall blocks, so a store call is head-of-line for every
connection. A commit is about 53us in the common case, which does not bind. The
tail is bimodal: once L0 backs up, `local_backpressure` deliberately slows the
writer, first by spinning and then in 10ms sleeps, and a single loop shares that
pause with everyone. `worker_threads` is the lever, since it sets how fast flush
and compaction drain. `max_write_buffer_size` is not: fjall stores it and never
reads it.

## Numbers

On an 8-core Ryzen microVM, release build:

```
ingest, 100 MB from a file           659 MB/s
verify, 95 MB                        71ms
BLAKE3 hash                          4163 MiB/s single-threaded
Lz4 on tar / HTML / binaries         3.4x to 7.3x
tar v1 vs v2, one member added       47.1% dedup, 6.8x overall
id returned on a 3.2s HTTP/2 upload  7ms
daemon RSS serving 60MB, 3 streams   24MB
```

## Not built

- No verified streaming. The root proves a whole blob after you have it. Proving
  a range mid-stream needs BLAKE3's interior tree nodes stored alongside, which
  [bao-tree](https://github.com/n0-computer/bao-tree) does. At a 64K block size,
  matching the chunk size, that outboard would cost 0.098% of content size.
- No library crate. klump is a binary; the store and client modules are the
  shape a library would take.
- No TLS and no TCP. The daemon listens on a unix socket only. rustls is
  natively blocking, so adding it would not disturb the loop.
- Fixed-size chunking, so dedup only survives changes that do not move bytes.
  Appending to a file keeps every chunk before the boundary. Inserting near the
  front shifts everything after it into new chunk boundaries and dedup goes to
  zero. Content-defined chunking with a rolling hash, as in FastCDC, cuts on
  content instead of offset and survives insertions. It is the obvious next
  thing if tarballs that change in the middle turn out to matter.
