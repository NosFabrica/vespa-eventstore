# Replaceable/deletion constraints beyond one node — what actually needs sharding

The question: the store enforces NIP-01 supersession, NIP-09 deletions and
NIP-62 vanishes with query-then-write logic. Does that survive a multi-node
Vespa? Do we need "one node per pubkey"?

## Multi-node Vespa is NOT the threat

The store's correctness rests on two invariants (see `NostrSemanticsStore`):

1. **One writer at a time** — every admission check and its write happen behind
   a single client-side mutex, so query-then-write is atomic against other
   writes *from this store instance*.
2. **Read-your-writes** — an acked put is visible to the next search
   (`EventIndex`'s contract; proton indexes on the write path before acking).

Both are **location-transparent**. Adding content nodes changes where
documents live (Vespa hashes docids into buckets and distributes them), not
who is allowed to write or what an acked write means: the distributor
serializes operations per document, queries fan out to every node, and a hit
is visible cluster-wide once acked. A multi-node cluster behind a single
store instance keeps every constraint exactly as a single node does. Nothing
about node count needs pubkey placement — and Vespa wouldn't let us steer
per-pubkey data placement anyway (docid hashing owns it).

## Multiple WRITERS are the threat

The races appear when a **second store instance** (or any other feeder)
writes the same cluster — regardless of node count, including one node:

- two instances insert two versions of the same replaceable concurrently;
  both pass the supersede probe (neither sees the other), both get stored —
  two live versions of a kind-0;
- an event and the kind-5/62 that covers it arrive at different instances at
  the same moment; the guard probe misses the not-yet-visible tombstone and
  the covered event survives.

There are no cross-document transactions to fix this engine-side — Vespa's
conditions see only the one document they address.

## The right sharding: one WRITE LANE per owner, not one node per pubkey

The saving property of Nostr's semantics: **every constraint this store
enforces is scoped to a single owner.** Replaceable addresses are
`(kind, pubkey[, d-tag])` — one author. NIP-09 only erases same-owner
targets (cross-author kind-5s are stored but inert). NIP-62 sweeps one
owner's history. Gift-wrap enforcement keys on the wrap's owner (the
recipient). Different owners' events **commute** — no interleaving of them
can violate anything.

So the "one node per pubkey" instinct is right if "node" means **writer**,
not Vespa node: consistent-hash the event's OWNER (`EventDoc.owner`: the
gift-wrap recipient for 1059, else the author) onto N ingest lanes — separate
store instances, or queues in front of them. Within a lane the existing mutex
provides full correctness; across lanes there is nothing to protect. Data
placement stays Vespa's job; N scales with ingest, unrelated to the cluster's
node count.

A relay fleet gets this almost for free: route EVENT ingestion by
`hash(owner) % lanes` at the load balancer or queue layer. Queries need no
routing at all — reads are stateless against the cluster.

## Engine-side hardening (optional, orthogonal)

- **Address-keyed replaceables + test-and-set** (docs/server-side-constraints.md):
  if replaceables used their address as the docid with a newest-wins
  condition, supersession would be atomic under ANY number of writers — the
  one constraint that could leave the client entirely. (Measured slower on a
  single node for latency, but in a multi-writer deployment it is a
  *correctness* device, which changes the calculus.)
- **NIP-40 expiry** already holds under any writer count — the GC selection
  is engine-side.
- Deletion/vanish blocking cannot be a condition (cross-document); it stays
  with the owner lane (or a future document-processor chain, which would
  centralize admission but must itself serialize per owner to close the same
  race).
