<h1>
<p align="center">
  klump
</h1>
  <p align="center">
    Content-addressed blob storage that streams. Blobs are named before they
    finish uploading, so a reader can start while the writer is still going.
    <br />
    <a href="#install">Install</a>
    ·
    <a href="#the-daemon">Daemon</a>
    ·
    <a href="#keyspaces">Keyspaces</a>
  </p>
</p>

---

klump stores blobs in [fjall](https://github.com/fjall-rs/fjall), an LSM
key-value store. Content is split into 64K chunks, and each chunk is keyed by
its BLAKE3 hash, so a chunk shared by a hundred blobs is stored once.

Content is never buffered whole. Chunks are written as they arrive and read
back one at a time. What does scale with blob size is the chunk list: 32 bytes
per 64K of content, so a 40 GB blob holds about 20 MB of hashes in memory while
it is being read or written.

A blob gets its id when the upload opens, before anyone knows what the content
will be.

## Install

```
cargo install --path .
```

Every command takes the store directory as its first argument. The daemon's
socket lives inside it at `<store>/sock`.

## Usage

Two files. The second is a copy of the first with 100K appended. The bytes are
random, so nothing compresses and the only saving visible is dedup.

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

`b.bin` shares fifteen of its seventeen chunks with `a.bin`. Fifteen rather
than sixteen because the chunk that straddles the 983040-byte boundary now has
new bytes in it.

Blobs come back by id, by an unambiguous prefix of an id, or by the hash of
their own content.

```console
$ klump get ./store 03guauzkos > copy.bin
$ klump get ./store gboqvz6qmpkjip3nvwtsebdtpn42caociumbxp7pll6kvzbksoka | cmp - a.bin
$ klump verify ./store 03guauzkos
ok  03guauzkos037euah9u9uvz7z  977K in 0ms
```

Deleting a blob reclaims only the chunks nothing else references.

```console
$ klump rm ./store 03guauzkpl
removed 03guauzkplma73c5puvr2ke4w
freed 2 chunks, 114K
```

## Blob ids and content hashes

Every blob has both.

```
id     03guauzkos037euah9u9uvz7z
root   gboqvz6qmpkjip3nvwtsebdtpn42caociumbxp7pll6kvzbksoka
```

The **id** is a [scru128](https://github.com/scru128/spec): 25 Base36 digits,
sortable by creation time. It exists from the moment the upload opens, which is
what lets you hand it to a reader before the content has arrived.

The **root** is the BLAKE3 hash of the content, rendered as 52 lowercase base32
digits. It is not known until the last byte lands. Two blobs holding the same
bytes have the same root. It is the plain BLAKE3 hash of the content rather
than a hash over the chunk list, so `b3sum` computes the same value and the
root does not change if the chunk size ever does.

Both forms are lowercase alphanumerics. They pass through a URL, a filename or
a shell without escaping, which is why there is no `blake3-` prefix and no
base64 anywhere.

Uploading the same content twice gives two ids and one root. That is allowed,
so `roots` is a multimap and `info` reports who else holds the same bytes.

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

`put` holds one chunk at a time and writes each one before reading the next.
`get` reads back the same way.

`--tee` forwards the content to stdout as it is stored, so a pipeline can
capture and consume in one pass. The blob name goes to stderr instead.

```console
$ ffmpeg ... | klump put ./store --tee -t video/mp4 2>id.txt | mpv -
```

`get --follow` keeps reading while a blob is still uploading and returns when
the trailer lands. It needs a daemon, because the store allows one process at a
time. See [The daemon](#the-daemon).

## Keyspaces

```
cas    32B hash              -> chunk bytes    kv-separated, Lz4
blobs  16B id ++ 4B seq      -> see below      plain
refs   32B hash ++ 16B id    -> ()             plain
roots  32B root ++ 16B id    -> ()             plain
```

`blobs` holds one entry per chunk, between two markers.

```
seq 0          content type          known when the upload opens
seq 1..n       32B chunk hash        in order
seq u32::MAX   size, chunks, root    known only at the end
```

Size and root sit in a trailer because you cannot know either until the last
byte arrives. A blob is complete exactly when its trailer exists. `u32::MAX`
sorts last, so a prefix scan learns that by reading to the end of the entries
it was already reading.

`refs` is what makes deletion possible. Chunks are shared between blobs, so
dropping a blob may not drop its chunks. One key per (chunk, blob) pair turns
"does anything still reference this chunk" into a prefix scan. If a blob
contains the same chunk twice, the key is identical both times, so the count
does not need separate deduplication.

## Deleting blobs

`rm` runs in two steps. A batch removes the blob's entries and its refs
atomically. Only after that commits is it safe to ask whether a chunk is
orphaned, because until then the blob's own reference is still there.

`gc` sweeps chunks that nothing references. A healthy store has none, since
`rm` reclaims as it goes. It exists for the case where an interrupted `rm`
leaves a chunk stranded.

## fjall configuration

Chunks are the only large values, so `cas` is the only keyspace using key-value
separation. The LSM holds a 32-byte key and a value handle, and compaction
moves those instead of the payload. Lz4 on the chunks gets 3.4x on tarballs and
4x to 7x on most content.

One default is changed:

```rust
.filter_block_pinning_policy(PinningPolicy::all(true))
```

Every write asks "do I already have this chunk", and a bloom filter answers no
without reading the table. By default fjall pins those filters at L0 only and
leaves deeper ones in the shared block cache. That cache is sharded, and a
filter block for a large table does not fit in a single shard, so it cannot
stay cached and every lookup re-reads it.

Measured on fjall 3.1.10 with criterion, 100 samples per point, `contains_key`
on an absent key:

```
                     1M chunks   8M chunks   memory at 8M
stock                  69.4 us      288 us      76-89 MB
pinned at all levels    220 ns      389 ns         30 MB
partitioned at L1+      487 ns      950 ns         29 MB
```

Pinning and partitioning cost the same memory. Partitioning opens leaner, 1 MB
against 11 MB, because it defers loading the filter, but once lookups start it
caches the same data. Pinning is chosen for the latency.

Stock is the case that matters: at 288us, the lookup costs more than the 53us
write it exists to avoid, so checking before writing becomes a loss at exactly
the size where dedup is worth most. With the filter resident, that check costs
under half a percent of a 64K insert, so it pays for itself above roughly half
a percent dedup.

## The daemon

fjall gives one process exclusive access to a store, so a long-running klump
would lock out every other invocation. `klump serve` holds the store and speaks
HTTP over a unix socket. The CLI notices the socket and routes through it, with
the same commands and the same output either way.

```console
$ klump serve /var/klump &
klump serving /var/klump on /var/klump/sock

$ klump put /var/klump video.mp4 -t video/mp4   # over the socket now
03guee76zwfg4u8rszu96hxfp
```

It speaks **HTTP/1.1 and HTTP/2 on the same socket**, picked per connection by
matching the first 24 bytes against HTTP/2's connection preface. Plain `curl`
works and so does `curl --http2-prior-knowledge`, with no `Upgrade` handshake.

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

### Why HTTP/2

On HTTP/1.1 a response cannot usefully begin until the request body is
finished, so an upload only learns its id at the end. HTTP/2 frames the request
and the response independently, so the id comes back immediately.

```console
$ slow-producer | curl --http2-prior-knowledge --unix-socket /var/klump/sock \
    -X POST -T - http://localhost/blobs
  +   7ms  {"id":"03gued3ej4mpoy46e6gp53ffe"}    <- upload runs for another 3.2s
  ...
  {"chunks":8,"id":"...","root":"...","size":524288}
```

Flow control is also per stream, so one slow reader is throttled on its own
instead of stalling everything else sharing the connection. Serving 60MB across
three concurrent streams peaks at 24MB of memory.

### Following a blob during upload

```console
$ klump get /var/klump <id> --follow
$ curl --unix-socket /var/klump/sock "http://localhost/blobs/<id>?follow=1"
```

Returns when the blob's trailer lands. The writer and every reader run on the
same thread, so a follower is served in the same pass that stored the chunk
rather than on a timer.

### Deleting a blob that is in use

`DELETE` behaves like unlinking a file. The name disappears immediately, and
the chunks survive while anyone still holds the blob open.

```console
$ klump rm /var/klump <id>   # while a follower is mid-stream
removed <id>
freed 0 chunks, 0B       # deferred: someone still has it open
```

The follower reads to the end of what exists. A writer that was mid-upload goes
on writing, and its content is reclaimed when it closes. This is also why
deletion needs no lock: chunks are only reclaimed once nothing is mid-operation
on that blob.

## How the daemon works

One thread, one loop, no async runtime.

```
polling          readiness: epoll, kqueue, or wepoll over IOCP on Windows
h11r             sans-IO HTTP/1.1  receive_data / next_event / send_*
shiguredo_http2  sans-IO HTTP/2    feed / process / poll_event / poll_output
fjall            the store, blocking
```

Neither codec performs IO. Every syscall lives in one file, and the loop reads
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

Thread-per-connection was the alternative. HTTP/2 multiplexes many streams over
one connection, which threads have to demux by hand. The loop also keeps the
Windows port down to which `UnixListener` gets imported.

The cost is that fjall blocks, so a store call holds up every connection. A
commit is around 53us most of the time, which does not bind. The tail is the
problem: once L0 backs up, `local_backpressure` slows the writer on purpose,
first by spinning and then in 10ms sleeps, and a single loop shares that pause
with everyone. `worker_threads` is the lever, since it sets how fast flush and
compaction drain. `max_write_buffer_size` is not a lever: fjall stores the
value and never reads it.

## Benchmarks

8-core Ryzen microVM, release build.

```
ingest, 100 MB from a file           659 MB/s
verify, 95 MB                        71ms
BLAKE3 hash                          4163 MiB/s single-threaded
Lz4 on tar / HTML / binaries         3.4x to 7.3x
tar v1 vs v2, one member added       47.1% dedup, 6.8x overall
id returned on a 3.2s HTTP/2 upload  7ms
daemon RSS serving 60MB, 3 streams   24MB
```

## Limitations

- No verified streaming. The root proves a whole blob once you have all of it.
  Proving a byte range mid-stream needs BLAKE3's interior tree nodes stored
  alongside, which [bao-tree](https://github.com/n0-computer/bao-tree) does. At
  a 64K block size matching the chunk size, that outboard costs 0.098% of
  content size.
- No library crate. klump is a binary. The store and client modules are roughly
  the shape a library would take.
- No TLS and no TCP. The daemon listens on a unix socket. rustls is natively
  blocking, so adding it would not disturb the loop.
- Fixed-size chunking, so dedup only survives edits that do not shift bytes.
  Appending to a file keeps every chunk before the boundary intact. Inserting
  near the front moves every later chunk boundary and dedup drops to zero.
  Content-defined chunking with a rolling hash, as in FastCDC, cuts on content
  rather than offset and survives insertions.
