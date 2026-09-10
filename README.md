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

$ klump put a.bin -v
03guauzkos037euah9u9uvz7z
977K in 16 chunks, 1ms at 663M/s, root gboqvz6qmpkjip3nvwtsebdtpn42caociumbxp7pll6kvzbksoka

$ klump put b.bin
03guauzkplma73c5puvr2ke4w

$ klump stats
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
$ klump get 03guauzkos > copy.bin
$ klump get gboqvz6qmpkjip3nvwtsebdtpn42caociumbxp7pll6kvzbksoka | cmp - a.bin
$ klump verify 03guauzkos
ok  03guauzkos037euah9u9uvz7z  977K in 0ms
```

Deleting a blob reclaims only the chunks nothing else wants:

```console
$ klump rm 03guauzkpl
removed 03guauzkplma73c5puvr2ke4w
freed 2 chunks, 114K
```

## Install

```
cargo install --path .
```

The store lives at `~/.klump` unless `--store` or `KLUMP_STORE` says otherwise.

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
$ klump put a.bin          # the same bytes, a second time
03guav5z7u0axh832vlvslch1

$ klump info 03guauzkos
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
$ ffmpeg ... | klump put --tee -t video/mp4 2>id.txt | mpv -
```

`get --follow` keeps reading while a blob is still being ingested and returns
when its trailer lands. See the caveat in [Concurrency](#concurrency): today
that is a library path, not a CLI one.

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

## Concurrency

fjall gives one process exclusive access to a store. There is no read-only or
secondary mode: a second klump on the same directory gets

```
klump: store ~/.klump is locked by another klump process.
```

So `get --follow` cannot currently watch a blob that another `klump put` is
writing. Following works inside one process, which is a library path today.
Making it work across processes means a daemon owning the store, and that is
not built.

`rm` racing a `put` of the same chunk is the one unsafe interleaving: the
remove can observe no refs and delete a chunk the ingest is about to reference.
Single-process access makes this unreachable from the CLI today. A daemon would
have to close it, with a transaction around the probe and the delete.

## Numbers

On an 8-core Ryzen microVM, release build:

```
ingest, 100 MB from a file      659 MB/s
verify, 95 MB                   71ms
BLAKE3 hash                     4163 MiB/s single-threaded
Lz4 on tar / HTML / binaries    3.4x to 7.3x
tar v1 vs v2, one member added  47.1% dedup, 6.8x overall
```

## Not built

- No verified streaming. The root proves a whole blob after you have it. Proving
  a range mid-stream needs BLAKE3's interior tree nodes stored alongside, which
  [bao-tree](https://github.com/n0-computer/bao-tree) does. At a 64K block size,
  matching the chunk size, that outboard would cost 0.098% of content size.
- No daemon, so no concurrent readers. See [Concurrency](#concurrency).
- No library crate. klump is a binary; the store module is the shape a library
  would take.
- Fixed-size chunking, so dedup only survives changes that do not move bytes.
  Appending to a file keeps every chunk before the boundary. Inserting near the
  front shifts everything after it into new chunk boundaries and dedup goes to
  zero. Content-defined chunking with a rolling hash, as in FastCDC, cuts on
  content instead of offset and survives insertions. It is the obvious next
  thing if tarballs that change in the middle turn out to matter.
