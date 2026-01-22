# klump

Chunked blob storage experiment using [fjall](https://github.com/fjall-rs/fjall).

## Problem

Storing large blobs requires buffering entire content before write. Can't stream, can't know ID until done.

## Approach

- Split incoming bytes into 64KB chunks
- Store chunks by BLAKE3 hash (content-addressed, deduped)
- Blob ID (scru128) assigned immediately, before content fully ingested
- Append-only: each chunk write adds one entry, EOF marker signals completion

Two keyspaces:
- `cas`: `hash (32B) → chunk bytes`
- `blobs`: `blob_id (16B) + seq (4B) → content-type (seq 0)` | `hash (32B)` | `empty (EOF)`

```
blob_id + seq:0  → "text/plain"  (content-type)
blob_id + seq:1  → hash          (chunk)
blob_id + seq:2  → hash          (chunk)
blob_id + seq:3  → (empty)       (EOF)
```

Status inferred: last entry empty = Complete, otherwise Ingesting.

Readers can prefix-scan `blob_id` to stream chunks as they arrive.

## Usage

```
echo "hello" | klump put -t text/plain   # returns scru128 id
klump get <id>                           # streams content to stdout
klump list                               # show all blobs
klump info <id>                          # show chunks
```

## Why

Exploring streaming writes for [xs](https://github.com/cablehead/xs). Readers could subscribe to a blob ID and process chunks as they arrive, before ingest completes.
