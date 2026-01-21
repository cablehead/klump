# klump

Chunked blob storage experiment using fjall.

## Problem

Storing large blobs requires buffering entire content before write. Can't stream, can't know ID until done.

## Approach

- Split incoming bytes into 64KB chunks
- Store chunks by SHA-256 hash (content-addressed, deduped)
- Blob ID (scru128) assigned immediately, before content fully ingested
- Each chunk write appends one index entry (no manifest rewriting)

Two keyspaces:
- `cas`: `hash (32B) → chunk bytes`
- `blobs`: `blob_id (16B) → meta (9B)` and `blob_id (16B) + seq (4B) → hash (32B)`

Meta: 1 byte status + 8 bytes size. Status transitions: `Ingesting` → `Complete`

Readers can prefix-scan `blob_id` to stream chunks as they arrive.

## Usage

```
echo "hello" | klump put     # returns scru128 id
klump get <id>               # streams content to stdout
klump list                   # show all blobs
klump info <id>              # show chunks
```

## Why

Exploring streaming writes for [xs](https://github.com/cablehead/xs). Readers could subscribe to a blob ID and process chunks as they arrive, before ingest completes.
