# Upgrading — what an operator sees change

Changes a deployment notices without reading the code: a metric that moved, a cost
that shifted from one counter to another. Newest first. Each entry says what changed,
since when, and what to compare against what.

## The per-event insert's dup probe is metered as `exists`, not `get` (2026-11)

`insert()` asks "is this id already stored?" before every write. It used to ask with
`EventIndex.get`, a whole-document read, so the cost ledger (`CostLedger`, the
`StoreTelemetry` a relay's operator page reads) booked it as `PortCall.Get` under the
`Insert` activity. It now asks `EventIndex.existingIds`, an existence check, and is
booked as `PortCall.Exists`:

- **Id-keyed index (the default):** a document GET of the id field alone
  (`fieldSet=event:id`), on the same connection and to the same node as before.
- **Address-keyed index (`VESPA_ADDRESS_KEYED=1`):** a one-id search on the
  attribute-only `dedup` summary, where it was an id-attribute search on the full
  summary.

So, on a dashboard, `Insert × get` falls to about the kind-5 deletions' own target reads
and `Insert × exists` rises by about one call per `insert()`. Their sum is what to compare across
the upgrade. The read is also cheaper: on a 3,000-tag kind 3 the writer lock's hold per
duplicate went from 16.5 to 2.1 ms (`benchmark/README.md` §2bc).

## A kind 5's e-tag targets are read as stubs (2026-11)

`insert()` of a NIP-09 deletion reads each e-tagged target before removing it (a
`batchInsert()` carrying one reads them in its own preload, unchanged). Each target is still one document GET booked as `get`, but it now asks for the
target's attribute fields alone (`EventIndex.stubs`), so a 3,000-tag kind 3 comes back as
five fields. Only a trust-kind target (a 30382 card, a 10040) is then read again whole, one
more `get` each, since the trust projection reads the removed document.
