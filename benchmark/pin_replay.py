#!/usr/bin/env python3
"""Replay a Quartz pin bump on REAL data — the companion of the `pinReplayProbe` task.

A pin bump's reindex list comes from reading upstream's diff, and a kind left out
of it is silently never repaired (`reindexFullTextSearch(kinds)` is only as
complete as its list). This checks the list against real events instead:

    # 1. capture (READ ONLY: REQ + COUNT, never an EVENT): the kinds you will
    #    reindex, and a CONTROL set — the searchable kinds you will NOT
    python3 benchmark/pin_replay.py capture moved.json   moved_kinds.json   600
    python3 benchmark/pin_replay.py capture control.json control_kinds.json 150
    # 2. load both at the OLD pin, then snapshot: must print 0 stale
    ./gradlew :benchmark:exportLoad --args="moved.json control.json"
    ./gradlew :benchmark:pinReplayProbe --args="snapshot old.jsonl"
    # 3. switch gradle/libs.versions.toml to the NEW pin; snapshot BEFORE the repair
    ./gradlew :benchmark:pinReplayProbe --args="snapshot pre.jsonl"
    #    every kind it prints stale must be in moved_kinds.json — a stale
    #    CONTROL kind is a kind the list missed
    python3 benchmark/pin_replay.py probes pre.jsonl probes.jsonl
    ./gradlew :benchmark:pinReplayProbe --args="search probes.jsonl before.jsonl"
    ./gradlew :benchmark:pinReplayProbe --args="reindex moved_kinds.json"
    ./gradlew :benchmark:pinReplayProbe --args="snapshot post.jsonl --require-clean"
    ./gradlew :benchmark:pinReplayProbe --args="search probes.jsonl after.jsonl"
    python3 benchmark/pin_replay.py grade before.jsonl after.jsonl

The probes turn the column diff into searches a client would send: per moved doc,
a term the new pin ADDS (must miss before the repair, hit after) and one it DROPS
(the reverse), plus control terms from docs that do not move (hit both times).
The 68268da413 run is in benchmark/README.md, "Replaying a pin bump".

Kind lists are JSON arrays of ints. The relay gates every read through a web of
trust; `include:spam` reads it unranked (see capture_staging.py for why), and the
capture adds the canonical observer's 10040 and provider cards so a local
`observer:` lens works too.
"""
import asyncio, collections, json, random, re, ssl, sys, time

RELAY = "wss://search.brainstorm.world/"
OBSERVER = "460c25e682fda7832b52d1f22d3d22b3176d972f60dcdc3212ed8c92ef85065c"
PROVIDER = "7d7ffd720b907fe597a7f454afe02f2dc1eca440baa029e9117b1c3209839377"


async def _req(ws, sub, filt, budget=60):
    await ws.send(json.dumps(["REQ", sub, filt]))
    out, deadline = [], time.time() + budget
    while time.time() < deadline:
        try:
            m = json.loads(await asyncio.wait_for(ws.recv(), timeout=deadline - time.time()))
        except asyncio.TimeoutError:
            break
        if m[0] == "EVENT" and m[1] == sub:
            out.append(m[2])
        elif m[0] in ("EOSE", "CLOSED") and m[1] == sub:
            if m[0] == "CLOSED":
                print(f"  CLOSED {sub}: {m[2:]}", file=sys.stderr)
            break
    await ws.send(json.dumps(["CLOSE", sub]))
    return out


async def _count(ws, sub, filt, budget=60):
    await ws.send(json.dumps(["COUNT", sub, filt]))
    deadline = time.time() + budget
    while time.time() < deadline:
        try:
            m = json.loads(await asyncio.wait_for(ws.recv(), timeout=deadline - time.time()))
        except asyncio.TimeoutError:
            return None
        if m[0] == "COUNT" and m[1] == sub:
            return m[2].get("count")
        if m[0] == "CLOSED" and m[1] == sub:
            return None
    return None


async def capture(out_path, kinds_path, per_kind):
    """A recent page of each kind plus the lens; the live COUNT per kind beside it (the repair's real size)."""
    import websockets

    kinds = json.load(open(kinds_path))
    corpus, seen, counts = [], set(), {}

    def add(evs):
        fresh = [e for e in evs if e["id"] not in seen]
        seen.update(e["id"] for e in fresh)
        corpus.extend(fresh)
        return len(fresh)

    async with websockets.connect(RELAY, ssl=ssl.create_default_context(), max_size=32 << 20, open_timeout=45) as ws:
        add(await _req(ws, "l1", {"kinds": [10040], "authors": [OBSERVER], "search": "include:spam"}))
        add(await _req(ws, "l2", {"kinds": [30382], "authors": [PROVIDER], "limit": 3000, "search": "include:spam"}, budget=120))
        for k in kinds:
            counts[k] = await _count(ws, f"c{k}", {"kinds": [k], "search": "include:spam"})
            got = add(await _req(ws, f"r{k}", {"kinds": [k], "limit": per_kind, "search": "include:spam"}, budget=90)) if counts[k] else 0
            print(f"  kind {k}: live {counts[k]} captured {got}", file=sys.stderr)
    json.dump(corpus, open(out_path, "w"))
    json.dump(counts, open(out_path + ".counts.json", "w"))
    print(f"wrote {len(corpus)} events; live total {sum(v or 0 for v in counts.values()):,}", file=sys.stderr)


WORD = re.compile(r"[a-z]{5,20}")


def probes(snap_path, out_path, per_kind=25):
    """Search probes from a snapshot taken under the NEW pin BEFORE the repair (stored = old columns, derived = new)."""
    random.seed(7)
    rows = [json.loads(l) for l in open(snap_path)]
    text = lambda cols: "\n".join(cols.values()).lower()
    df = collections.Counter()
    for r in rows:
        df.update(set(WORD.findall(text(r["derived"]))) | set(WORD.findall(text(r["stored"]))))

    def pick(words, other):
        # Rarest first, and absent from the other side even as a SUBSTRING:
        # a term most of the corpus carries proves nothing at limit 500.
        words = sorted((w for w in words if w not in other), key=lambda w: (df[w], w))
        return words[0] if words and df[words[0]] <= 200 else None

    by_kind = collections.defaultdict(list)
    for r in rows:
        by_kind[r["kind"]].append(r)
    out = []

    def probe(kind, row, kind_of_probe, term):
        if term:
            out.append({"kinds": [kind], "search": f"{term} include:spam", "limit": 500, "id": row["id"], "probe": kind_of_probe, "term": term})

    for k, rs in sorted(by_kind.items()):
        random.shuffle(rs)
        for r in [r for r in rs if r["stored"] != r["derived"]][:per_kind]:
            s, d = text(r["stored"]), text(r["derived"])
            probe(k, r, "added", pick(set(WORD.findall(d)), s))
            probe(k, r, "dropped", pick(set(WORD.findall(s)), d))
        # Control terms come from the TEXT tiers: a website is one URL token,
        # and a word buried inside it is not something a search can hit.
        for r in [r for r in rs if r["stored"] == r["derived"] and r["derived"]][:5]:
            probe(k, r, "control", pick(set(WORD.findall(text({f: v for f, v in r["derived"].items() if f != "website"}))), ""))
    with open(out_path, "w") as f:
        f.writelines(json.dumps(q) + "\n" for q in out)
    print(dict(collections.Counter(q["probe"] for q in out)))


def grade(before_path, after_path):
    """Exit 1 unless every probe's doc is found / missed as its kind of probe expects."""
    want = {"added": (False, True), "dropped": (True, False), "control": (True, True)}
    load = lambda p: [json.loads(l) for l in open(p)]
    tally, bad = collections.Counter(), []
    for b, a in zip(load(before_path), load(after_path), strict=True):
        assert (b["search"], b["id"]) == (a["search"], a["id"])
        got = (b["id"] in b["ids"], a["id"] in a["ids"])
        ok = got == want[b["probe"]]
        tally[(b["probe"], "ok" if ok else "MISMATCH")] += 1
        if not ok:
            bad.append((b["probe"], b["kinds"][0], b["term"], b["id"][:12], got))
    for k, v in sorted(tally.items()):
        print(*k, v)
    for x in bad:
        print("  ", x)
    sys.exit(1 if bad else 0)


if __name__ == "__main__":
    cmd, rest = sys.argv[1], sys.argv[2:]
    if cmd == "capture":
        asyncio.run(capture(rest[0], rest[1], int(rest[2]) if len(rest) > 2 else 600))
    elif cmd == "probes":
        probes(rest[0], rest[1], int(rest[2]) if len(rest) > 2 else 25)
    elif cmd == "grade":
        grade(rest[0], rest[1])
    else:
        sys.exit(__doc__)
