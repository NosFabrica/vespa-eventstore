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

### A different second-writer failure: the guard-owner cache

The two races above are *windows* — microseconds between one instance's probe
and another's write. The guard-owner cache (`GuardOwners`) fails differently
and needs its own answer:

- it caches the owners that have a stored kind-5/62, so nearly every insert can
  skip both admission probes;
- it is loaded ONCE, lazily, and afterwards learns new guards only from writes
  made through this store instance;
- so a tombstone another writer stores is invisible to this instance **for the
  rest of its process lifetime**, and every event that tombstone covers is
  admitted and reported accepted. No concurrency required — the two writes can
  be hours apart.

The deployment that hits it is not exotic: two processes split **by role**
(serving relay + sync router) rather than by owner, both touching the same
authors, with the router mirroring kinds 5 and 62 from upstream relays.

`WriterTopology` is how a deployment settles it, because no store can detect a
sibling feeder:

| | guard cache | a foreign tombstone is honoured | serves a deleted event? |
|---|---|---|---|
| `SHARED_STRICT` (default) | none; every insert probes | immediately | no |
| `SINGLE_WRITER` | loaded once, never rebuilt | never — the mode asserts there are none | no, while the assertion holds |
| `SHARED` | rebuilt every `guardRefreshSeconds` | within one rebuild | yes, inside that window |

`SHARED_STRICT` is the default because the cache is **purely a performance
device**. It never makes anything more correct — it only decides to skip a
probe the store would otherwise run — so every one of its failures is in one
direction: an event a tombstone covers is admitted, stored, and served. And
nothing repairs that afterwards; re-delivering the tombstone hits the dedup
gate before `applyDeletion`, so no re-sweep occurs. A deployment that has said
nothing about its writers must not be trading that invariant for read capacity,
so the savings are opted into by asserting a property of the deployment.

`SINGLE_WRITER` is the fast path with no window at all — assert it when it is
true. `SHARED` is for a multi-writer deployment that genuinely accepts a
bounded window; read the bound honestly first, because it is
`max(guardRefreshSeconds, rebuild duration)` and the rebuild is a document-API
visit whose cost scales with the WHOLE corpus rather than with the number of
guards (see `VespaEventIndex.visitIds`' measured scan rates — a full pass on a
42.5M-doc corpus ran ~28 minutes). On a large corpus the rebuild duration is
the binding term, measured in hours, not the configured interval.

Rebuilds are union-only (a guard noted while the scan runs is folded into the
replacement before it is published), so a refresh can never turn a flagged
owner unflagged. `refreshGuardOwners()` is the on-demand barrier — worth
calling after a sync round that mirrored guards.

Note the shapes differ: owner-lane sharding fixes the races above completely,
and it also satisfies `SINGLE_WRITER` — one lane sees all of its owners' guards.
Role-split processes satisfy neither.

### The same failure, one level up: the kind-10040 provider pass

`ProviderMap` caches one pass over every stored kind-10040, and the observer's
lens (`rankKey` / `followersKey`), the Trusted List gate (`Delegations`) and the
card projection's "is this signer a named service" all read it. Like the guard
cache it learned only from this instance's own writes: a 10040 stored through
it drops the pass, and nothing else did. De10df3 moved the gate onto that pass
reasoning that "every write passes through the projection that invalidates
it" — true per process, not per index.

Measured on search-staging (2026-09-26, relay process up nine days, most 10040s
mirrored in by the sync router): 24 of 47 recent observers resolved **no lens**
and got empty ranked pages under the observer gate, and bare-`30392`
delegations added after the relay's last rebuild did not unpack
(NosFabrica/vespa-eventstore#145). The reverse held too: the sync skipped cards
by a service first named on the relay's socket, because its pass did not name
it.

It is not settled the guard-cache way. Not caching (`SHARED_STRICT`'s answer
there) would put a `complete` read of every 10040 on every observer query and
fail them all whenever coverage dips. So:

| | provider pass | a foreign 10040 applies |
|---|---|---|
| `SINGLE_WRITER` | cached until a local 10040 write | never — the mode asserts there are none |
| `SHARED_STRICT`, `SHARED` | also rebuilt every `providerRefreshSeconds` (default 60) | within one rebuild |

A rebuild is one `/search/` of the stored 10040s (hundreds of documents, not
the corpus), readers keep the old pass while it runs, and a rebuild that throws
or reads nothing keeps the old pass (`BackgroundFailures` "trust.providers";
the `trust.providers.age.secs` gauge climbs past the interval). A rebuild that
names a service this process never named queues that service's walk, so the
cards it skipped meanwhile are projected. `refreshTrustProviders()` is the
on-demand barrier; `explainTrust` reports a pass that disagrees with the stored
list instead of blaming the list.

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
