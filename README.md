# rwa-xstock-consumer

Consumes the token-info-changed Kafka topic, keeps only `RWA_XSTOCK` (tag `36`)
events, and persists them to a local bbolt store that is deduplicated by
`(partition, offset)` and ordered by a monotonic local sequence (`seq`). A JSON-RPC
API exposes that store as an incremental feed.

## Run

```bash
go build .
./rwa-xstock-consumer
```

Key config (`config.toml`): `store_file` (bbolt db path), `api_addr` (JSON-RPC
listen address; empty disables the API).

## JSON-RPC API

JSON-RPC 2.0 over HTTP `POST` to `api_addr` (default `127.0.0.1:8080`). Single
requests only (no batch). Every request needs `"jsonrpc": "2.0"` and an `id`.

The store is an append-only feed keyed by `seq` (a local monotonically increasing
cursor, independent of Kafka offset so it works across partitions). Clients pull
incrementally: pass the `latestSeq` from the previous response as the next `start`.

### `latest_seq`

Returns the highest `seq` currently in the store (0 if empty). Useful as a starting
cursor or to check how far the feed has advanced.

| Params | Result |
|--------|--------|
| none   | `number` — the latest `seq` |

```bash
curl -s 127.0.0.1:8080 \
  -d '{"jsonrpc":"2.0","method":"latest_seq","id":1}'
```

```json
{"jsonrpc":"2.0","result":42,"id":1}
```

### `tokens`

Returns the net token changes over the half-open range **`(start, latestSeq]`** —
that is, events with **`start < seq <= latestSeq`** (`start` exclusive, `latestSeq`
inclusive): the deduplicated set of TokenIDs added (`new`) and deleted (`deleted`),
plus `latestSeq` (the cursor to pass back next time).

Classification is **last-wins** per TokenID within the range: added-then-deleted
nets to `deleted`, deleted-then-re-added nets to `new`, so `new` and `deleted` are
disjoint. Both arrays are sorted; empty sets render as `[]`.

| Params | Type | Description |
|--------|------|-------------|
| `start` | `number` | Exclusive cursor; returns changes with `seq > start`. Omit or `0` to read from the beginning. |

| Result field | Type | Description |
|--------------|------|-------------|
| `start` | `number` | Echoes the request cursor; the (exclusive) lower bound of the covered range |
| `new` | `string[]` | TokenIDs (`chainIndex-tokenAddress`) added/updated in `(start, latestSeq]` |
| `deleted` | `string[]` | TokenIDs deleted in `(start, latestSeq]` |
| `latestSeq` | `number` | Highest `seq` covered by this response (inclusive); use as the next `start` (unchanged from `start` when nothing is new) |

```bash
curl -s 127.0.0.1:8080 \
  -d '{"jsonrpc":"2.0","method":"tokens","params":{"start":0},"id":2}'
```

```json
{
  "jsonrpc": "2.0",
  "result": {
    "start": 0,
    "new": ["501-0x9f8a...c2", "1-0x3bce...77"],
    "deleted": ["501-0x12de...90"],
    "latestSeq": 42
  },
  "id": 2
}
```

Next incremental pull uses the returned cursor:

```bash
curl -s 127.0.0.1:8080 \
  -d '{"jsonrpc":"2.0","method":"tokens","params":{"start":42},"id":3}'
```

When there is nothing new, `new`/`deleted` are empty and `latestSeq` equals `start`:

```json
{"jsonrpc":"2.0","result":{"start":42,"new":[],"deleted":[],"latestSeq":42},"id":3}
```

### Errors

Standard JSON-RPC 2.0 error object: `{"code":<int>,"message":<string>,"data":<string?>}`.

| Code | Meaning |
|------|---------|
| `-32700` | Parse error (malformed JSON body) |
| `-32600` | Invalid request (`jsonrpc` is not `"2.0"`) |
| `-32601` | Method not found |
| `-32602` | Invalid params |
| `-32603` | Internal error (store read failed) |

```bash
curl -s 127.0.0.1:8080 \
  -d '{"jsonrpc":"2.0","method":"nope","id":9}'
```

```json
{"jsonrpc":"2.0","error":{"code":-32601,"message":"method not found: nope"},"id":9}
```
