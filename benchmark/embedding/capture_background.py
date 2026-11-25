#!/usr/bin/env python3
"""Capture a query-INDEPENDENT background slice for the embedding experiment.

    python3 benchmark/embedding/capture_background.py /tmp/eval/corpus.json /tmp/eval/background.json

READ ONLY: REQ only, never an EVENT (AGENTS.md). Read docs/embedding-experiment.md first.

Why it exists. `capture_eval.py` pulls each topic's production page plus an
unranked slice, which are the documents the WORD MATCHER already found. That
measures a re-ranking, and its own docstring warns it is the wrong corpus for
a recall change: a dense retriever's whole claim is the note ABOUT coffee that
never says "coffee", and no such note is in a corpus built by searching for
"coffee". This adds one.

What it pulls. The most-trusted authors under the canonical observer's lens
(the provider's 30382 `rank` the eval capture already holds, highest first),
and their recent notes (kind 1) and long-form articles (kind 30023), with a
cap per author so no one prolific account fills the slice. The REQ carries
`include:spam` (no words), the relay's unranked read: it answers no other
lens-less read, and the authors are already chosen by trust. Their trust cards
are already in the eval capture, since every author is drawn from them.

What it must NOT do: choose documents by any topic's words. The slice is the
same for every topic; selecting it by query would put back exactly the bias
it is here to remove, and a dense win measured on it would be the selection.
"""
import asyncio, json, ssl, sys, time
import websockets

RELAY = "wss://search.brainstorm.world/"
PROVIDER = "7d7ffd720b907fe597a7f454afe02f2dc1eca440baa029e9117b1c3209839377"
AUTHORS = 600        # most-trusted authors to sample
PER_AUTHOR = 40      # newest notes + articles per author, at most
BATCH = 10           # authors per REQ
MIN_RANK = 2         # the store's default gate (min_rank = 2): below it nothing is served anyway


async def req(ws, sub, filt, budget=90):
    await ws.send(json.dumps(["REQ", sub, filt]))
    out, deadline = [], time.time() + budget
    while time.time() < deadline:
        try:
            raw = await asyncio.wait_for(ws.recv(), timeout=deadline - time.time())
        except asyncio.TimeoutError:
            print(f"  TIMEOUT {sub}", file=sys.stderr)
            break
        m = json.loads(raw)
        if m[0] == "EVENT" and m[1] == sub:
            out.append(m[2])
        elif m[0] in ("EOSE", "CLOSED") and m[1] == sub:
            if m[0] == "CLOSED":
                print(f"  CLOSED {sub}: {m[2:]}", file=sys.stderr)
            break
    await ws.send(json.dumps(["CLOSE", sub]))
    return out


def ranked_authors(corpus):
    """(rank, pubkey) for every author the provider scored, highest first."""
    ranks = {}
    for e in corpus:
        if e["kind"] != 30382 or e["pubkey"] != PROVIDER:
            continue
        d = next((t[1] for t in e["tags"] if len(t) > 1 and t[0] == "d"), None)
        r = next((t[1] for t in e["tags"] if len(t) > 1 and t[0] == "rank"), None)
        if d and r is not None:
            try:
                ranks[d] = max(ranks.get(d, 0.0), float(r))
            except ValueError:
                pass
    return sorted(((r, p) for p, r in ranks.items() if r >= MIN_RANK), reverse=True)


async def main():
    corpus_path, out_path = sys.argv[1], sys.argv[2]
    corpus = json.load(open(corpus_path))
    seen = {e["id"] for e in corpus}
    authors = [p for _, p in ranked_authors(corpus)[:AUTHORS]]
    print(f"{len(authors)} authors at rank >= {MIN_RANK} (of the eval capture's cards)", file=sys.stderr)
    out = []
    async with websockets.connect(RELAY, ssl=ssl.create_default_context(), max_size=32 << 20, open_timeout=45) as ws:
        for i in range(0, len(authors), BATCH):
            chunk = authors[i:i + BATCH]
            got = await req(ws, f"b{i}", {"kinds": [1, 30023], "authors": chunk, "search": "include:spam", "limit": PER_AUTHOR * len(chunk)})
            per = {}
            for e in sorted(got, key=lambda e: -e["created_at"]):
                if e["id"] in seen or per.get(e["pubkey"], 0) >= PER_AUTHOR:
                    continue
                per[e["pubkey"]] = per.get(e["pubkey"], 0) + 1
                seen.add(e["id"])
                out.append(e)
            print(f"  authors {i}-{i + len(chunk)}: {len(got)} got, total {len(out)}", file=sys.stderr)
    with open(out_path, "w") as f:
        json.dump(out, f)
    print(f"wrote {len(out)} background events to {out_path}", file=sys.stderr)


asyncio.run(main())
