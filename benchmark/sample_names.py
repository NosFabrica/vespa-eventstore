#!/usr/bin/env python3
"""How common a word is AS A NAME on production: the input `rankAb --name-df` needs.

    python3 benchmark/sample_names.py sample /tmp/names            # -> /tmp/names/stratified.json
    docker run -d --name vespa-sample -p 8081:8080 -p 19072:19071 vespaengine/vespa
    VESPA_URL=http://localhost:8081 VESPA_CONFIG_URL=http://localhost:19072 \
        ./gradlew :benchmark:exportLoad --args="/tmp/names/stratified.json"
    python3 benchmark/sample_names.py measure http://localhost:8081 bitcoin odell … > benchmark/name_df.json

WHY A SAMPLE. event.sd's perfect_share() damps the perfect-match rung by
name_df_ratio(): the share of indexed documents carrying the query word in
name, display_name or search_primary, read from the LOCAL index. An evaluation
corpus (capture_eval.py) is drawn from the topics' own pages, so its names are
full of the words it queries ("bitcoin" names 7% of it), and the damping would
fire on everything. Production's ratio is what the ranking will see; a fair
sample of production measures it, and rankAb injects it into the evaluation
cluster (docs/search-evaluation.md §9).

WHY STRATIFIED. Random instants with the N events before each sample time
uniformly, not events uniformly, and the naming surface lives in kinds whose
events cluster: on 2026-10-23 a plain time sample held profiles at 6.0% (13.1%
on production) and wiki articles at 0.16% (1.2%, six million titles, mostly one
bulk importer). `sample` therefore COUNTs production's share of every kind that
carries a name, tops up the under-represented ones with kind-filtered draws,
and down-samples so each matches production. The kinds it does not list keep
their sampled mix, and their combined share fixes the sample's size.

`measure` reads name_df_ratio() itself, through the `search` profile's
match-features, so the number is Vespa's own extraction and tokenisation, not a
re-implementation of it. A word with no hit reads 0 (nothing names it). Words
with punctuation (primal.net) are measured but `rankAb` cannot inject them.

READ ONLY: REQ and COUNT only, never an EVENT (CLAUDE.md).
"""
import asyncio, json, random, ssl, sys, time, urllib.request
import websockets

RELAY = "wss://search.brainstorm.world/"
# Kinds whose name, display_name or title feeds the naming fields, plus kind 1,
# whose share sets the denominator. Shares are COUNTed live, never hard-coded.
NAMING_KINDS = [0, 1, 30023, 30000, 30003, 30311, 31923, 30818, 34235, 40, 41, 30617, 31990, 32267, 34550, 39089, 30015, 30030]
START = 1640995200  # 2022-01-01: before it the corpus is thin and the dirt (year 2100) is after it


async def req(ws, sub, filt, budget=60):
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
            break
    await ws.send(json.dumps(["CLOSE", sub]))
    return out


async def count(ws, sub, filt):
    await ws.send(json.dumps(["COUNT", sub, filt]))
    deadline = time.time() + 90
    while time.time() < deadline:
        m = json.loads(await asyncio.wait_for(ws.recv(), timeout=deadline - time.time()))
        if m[0] == "COUNT" and m[1] == sub:
            return m[2].get("count")
        if m[0] == "CLOSED" and m[1] == sub:
            return None
    return None


async def draw(ws, pool, points, per, kinds=None):
    """The `per` newest events before each instant, into `pool` (deduplicated by id)."""
    for i, t in enumerate(points):
        filt = {"search": "include:spam", "until": t, "limit": per}
        if kinds:
            filt["kinds"] = kinds
        for e in await req(ws, f"d{i}", filt):
            pool.setdefault(e["id"], e)
        if i % 50 == 49:
            print(f"  {i + 1}/{len(points)} draws, pool {len(pool)}", file=sys.stderr)


async def sample(outdir, points=800, per=400, seed=20261023):
    rnd = random.Random(seed)
    end = int(time.time())
    async with websockets.connect(RELAY, ssl=ssl.create_default_context(), max_size=64 << 20, open_timeout=45) as ws:
        total = await count(ws, "all", {"search": "include:spam"})
        share = {k: (await count(ws, f"k{k}", {"kinds": [k], "search": "include:spam"})) / total for k in NAMING_KINDS}
        print(f"production: {total:,} events; naming-kind shares {json.dumps({k: round(v, 6) for k, v in share.items()})}", file=sys.stderr)
        pool = {}
        await draw(ws, pool, sorted(rnd.randint(START, end) for _ in range(points)), per)
        # Size the sample by what the unlisted kinds can support, then top up
        # every listed kind short of its production share. Smaller draws for
        # the clustered kinds: a bulk import otherwise answers every instant.
        def plan():
            by = {}
            for e in pool.values():
                by.setdefault(e["kind"] if e["kind"] in share else "other", []).append(e)
            n = int(len(by.get("other", [])) / (1 - sum(share.values())))
            return by, n
        by, n = plan()
        for k in NAMING_KINDS:
            want, have = round(n * share[k]), len(by.get(k, []))
            if have < want:
                need = want - have
                small = max(20, min(per, need // 40))
                draws = max(10, 3 * need // small)
                print(f"  kind {k}: {have}/{want}, topping up with {draws} draws of {small}", file=sys.stderr)
                await draw(ws, pool, sorted(rnd.randint(START, end) for _ in range(draws)), small, [k])
        by, n = plan()
        out = list(by.get("other", []))
        for k in NAMING_KINDS:
            have = by.get(k, [])
            take = rnd.sample(have, min(round(n * share[k]), len(have)))
            out += take
            if len(take) < round(n * share[k]):
                print(f"  kind {k}: only {len(take)}/{round(n * share[k])}", file=sys.stderr)
        rnd.shuffle(out)
    with open(f"{outdir}/stratified.json", "w") as f:
        json.dump(out, f)
    print(f"wrote {len(out)} events (pool {len(pool)}) to {outdir}/stratified.json", file=sys.stderr)


def measure(vespa, words):
    yql = ('select * from event where ({defaultIndex:"name"}userInput(@w0)) or ({defaultIndex:"display_name"}userInput(@w0))'
           ' or ({defaultIndex:"search_primary"}userInput(@w0)) or ({defaultIndex:"search_text"}userInput(@w0))')
    out = {"_doc": f"share of documents carrying the word in name, display_name or search_primary: event.sd name_df_ratio() "
                   f"on a kind-stratified random sample of {RELAY} (benchmark/sample_names.py), measured {time.strftime('%Y-%m-%d')}"}
    for w in words:
        body = json.dumps({"yql": yql, "w0": w, "ranking": "search", "hits": 1,
                           "ranking.features.query(n_words)": "1", "ranking.features.query(min_rank)": "-1e9"}).encode()
        root = json.load(urllib.request.urlopen(urllib.request.Request(f"{vespa.rstrip('/')}/search/", body, {"Content-Type": "application/json"})))["root"]
        hit = (root.get("children") or [{}])[0]
        out[w.lower()] = hit.get("fields", {}).get("matchfeatures", {}).get("name_df_ratio", 0.0)
    json.dump(out, sys.stdout, ensure_ascii=False, indent=1)
    print()


if __name__ == "__main__":
    if sys.argv[1] == "sample":
        asyncio.run(sample(sys.argv[2]))
    elif sys.argv[1] == "measure":
        measure(sys.argv[2], sys.argv[3:])
    else:
        sys.exit(__doc__)
