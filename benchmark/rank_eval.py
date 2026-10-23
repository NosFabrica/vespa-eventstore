#!/usr/bin/env python3
"""Does the search order make sense? Offline evaluation of the default order vs `sort:links`.

    python3 benchmark/capture_links.py /tmp/eval.json --per-term 300 --popular 0 --no-oracle --terms "..."
    VESPA_URL=... ./gradlew :benchmark:exportLoad --args=/tmp/eval.json
    LINK_PROBE_MODE=dump LINK_PROBE_OUT=/tmp/eval.dump.json ./gradlew :benchmark:linkRerankProbe --args="/tmp/eval.json 10"
    python3 benchmark/rank_eval.py packets /tmp/eval.json /tmp/eval.dump.json /tmp/eval-out   # metrics + blind packets
    # ... judges grade /tmp/eval-out/packet-*.json into /tmp/eval-out/grades-*.json ...
    python3 benchmark/rank_eval.py score /tmp/eval-out                                        # unblind and score

Two kinds of evidence, kept apart:

  PROXIES, computed here from the data: duplicate texts and repeated authors in a
  page, its authors' trust (the lens's rank cards), its age, and how much of it
  anyone trusted engaged with. Cheap and objective, and blind to RELEVANCE.

  JUDGMENT, from readers who see only the query and the unlabeled top-10 lists of
  every order (letters assigned at random per query, independently per packet,
  the key written to a separate file): a 0-2 grade per result and a best-to-worst
  ranking of the lists. Two packets with independent assignments, so position
  bias and a single reader's taste both show up as disagreement instead of
  hiding in one number.

  The orders (ORDERS): the default `search` order and `sort:links` as the store
  serves them, and both again with a note's subject match on the body's rung
  instead of a profile name's (the dump's FLAT variant).

Read-only: nothing here talks to a relay or an engine.
"""
import json, math, os, random, re, statistics, sys, time


def load(capture, dump):
    events = {e["id"]: e for e in json.load(open(capture))}
    meta = json.load(open(capture + ".meta.json"))
    pages = json.load(open(dump))
    rank = {}
    for e in events.values():
        if e["kind"] == 30382:
            d = next((t[1] for t in e["tags"] if t[0] == "d" and len(t) > 1), None)
            r = next((t[1] for t in e["tags"] if t[0] == "rank" and len(t) > 1), None)
            if d and r and r.lstrip("-").isdigit():
                rank[d] = int(r)
    return events, meta, pages, rank


def tiers(window):
    """Text tier per hit, as linkRerankProbe's bands view cuts them: runs of the sorted
    distinct text scores with no gap above 2x."""
    raw = sorted({h["text"] for h in window.values()}, reverse=True)
    cuts = [b for a, b in zip(raw, raw[1:]) if b <= 0 or a / b > 2.0]
    return {i: sum(1 for c in cuts if h["text"] <= c) for i, h in window.items()}


def norm(text):
    return re.sub(r"\W+", " ", text.lower()).strip()[:200]


def features(term, ids, events, window, tier, rank, now):
    rows = []
    for pos, i in enumerate(ids, 1):
        e, w = events.get(i), window.get(i, {})
        if e is None:
            continue
        rows.append(
            {
                "pos": pos,
                "engine_pos": w.get("pos"),
                "tier": tier.get(i),
                "author_rank": rank.get(e["pubkey"], 0),
                "age_days": round((now - e["created_at"]) / 86400, 1),
                "people": w.get("people", 0),
                "link": round(w.get("link", 0.0), 2),
                "author": e["pubkey"],
                "text": e["content"],
            }
        )
    return rows


def proxies(rows):
    texts = [norm(r["text"]) for r in rows]
    return {
        "dup_texts": len(texts) - len(set(texts)),
        "repeat_authors": len(rows) - len({r["author"] for r in rows}),
        "author_rank_p50": statistics.median([r["author_rank"] for r in rows]) if rows else 0,
        "age_days_p50": statistics.median([r["age_days"] for r in rows]) if rows else 0,
        "engaged": sum(1 for r in rows if r["people"] > 0),
        "people_p50": statistics.median([r["people"] for r in rows]) if rows else 0,
        "off_top_tier": sum(1 for r in rows if r["tier"] not in (None, 0)),
    }


ORDERS = {
    "base": ("base", "window"),  # the default `search` order, as the store serves it
    "links": ("links", "window"),  # + sort:links, as the store serves it
    "flat": ("flat", "flat_window"),  # the subject rung dropped to the body's (w_name_tier = 4000)
    "flat_links": ("flat_links", "flat_window"),  # flat + the sort:links rerank
}


def packets(capture, dump, out):
    events, meta, pages, rank = load(capture, dump)
    now = meta.get("captured_at", int(time.time()))
    os.makedirs(out, exist_ok=True)
    orders = [o for o, (key, _) in ORDERS.items() if all(key in p for p in pages.values())]
    report, keys = {}, {}
    rows = {}
    for term, page in pages.items():
        for o in orders:
            key, win = ORDERS[o]
            rows[term, o] = features(term, page[key], events, page[win], tiers(page[win]), rank, now)
        report[term] = {o: proxies(rows[term, o]) for o in orders}
    for seed in (1, 2):
        rnd = random.Random(seed)
        packet = []
        for term in pages:
            labels = [chr(ord("A") + i) for i in range(len(orders))]
            shuffled = orders[:]
            rnd.shuffle(shuffled)
            keys.setdefault(term, {})[f"packet{seed}"] = dict(zip(labels, shuffled))
            # What a reader of a results page sees: the post, when, and who (short id) —
            # enough to judge relevance, freshness and repetition, nothing about why it ranked.
            show = lambda rs: [{"n": r["pos"], "date": time.strftime("%Y-%m-%d", time.gmtime(now - r["age_days"] * 86400)), "author": r["author"][:6], "text": r["text"][:400]} for r in rs]
            packet.append({"query": term, **{lab: show(rows[term, o]) for lab, o in zip(labels, shuffled)}})
        json.dump({"today": time.strftime("%Y-%m-%d", time.gmtime(now)), "queries": packet}, open(f"{out}/packet-{seed}.json", "w"), ensure_ascii=False, indent=1)
    json.dump(keys, open(f"{out}/key.json", "w"), indent=1)
    json.dump(report, open(f"{out}/proxies.json", "w"), indent=1)

    cols = ["dup_texts", "repeat_authors", "author_rank_p50", "age_days_p50", "engaged", "people_p50"]
    print(f"{'query':<18} " + " ".join(f"{c:>26}" for c in cols))
    for term, r in report.items():
        print(f"{term:<18} " + " ".join(f"{' / '.join(str(round(r[o][c])) for o in orders):>26}" for c in cols))
    print(f"(each cell: {' / '.join(orders)}, over the top 10)")
    for c in cols:
        print(f"  {c:<16} " + "  ".join(f"{o} {statistics.mean(report[t][o][c] for t in report):.1f}" for o in orders))
    print(f"\nwrote {out}/packet-1.json, packet-2.json (blind), key.json, proxies.json")


def dcg(grades):
    return sum((2 ** g - 1) / math.log2(i + 2) for i, g in enumerate(grades))


def score(out):
    keys = json.load(open(f"{out}/key.json"))
    rows = []
    for seed in (1, 2):
        path = f"{out}/grades-{seed}.json"
        if not os.path.exists(path):
            continue
        for g in json.load(open(path)):
            k = keys[g["query"]][f"packet{seed}"]
            grades = {k[lab]: g["grades"][lab] for lab in k}
            ideal = sorted((x for gs in grades.values() for x in gs), reverse=True)[:10]
            ranking = [k[lab] for lab in g["ranking"]]  # best first
            for o, gs in grades.items():
                rows.append({"query": g["query"], "judge": seed, "order": o, "rel": sum(gs), "ndcg": dcg(gs) / (dcg(ideal) or 1), "place": ranking.index(o) + 1 if o in ranking else None})
    if not rows:
        sys.exit("no grades-*.json yet")
    orders = list(dict.fromkeys(r["order"] for r in rows))
    print(f"{'query':<18} judge  " + "  ".join(f"{o:>16}" for o in orders) + "   (nDCG@10 / place)")
    for q in dict.fromkeys(r["query"] for r in rows):
        for j in sorted({r["judge"] for r in rows if r["query"] == q}):
            cells = {r["order"]: r for r in rows if r["query"] == q and r["judge"] == j}
            print(f"{q:<18} {j:>5}  " + "  ".join(f"{cells[o]['ndcg']:>10.3f} / {cells[o]['place']}" for o in orders))
    print()
    for j in sorted({r["judge"] for r in rows}):
        rs = [r for r in rows if r["judge"] == j]
        print(f"judge {j}: " + "   ".join(f"{o}: nDCG {statistics.mean(r['ndcg'] for r in rs if r['order'] == o):.3f}, place {statistics.mean(r['place'] for r in rs if r['order'] == o):.2f}, firsts {sum(1 for r in rs if r['order'] == o and r['place'] == 1)}" for o in orders))
    # Agreement: do the two judges put the same order first?
    firsts = {}
    for r in rows:
        if r["place"] == 1:
            firsts.setdefault(r["query"], {})[r["judge"]] = r["order"]
    both = [f for f in firsts.values() if len(f) == 2]
    print(f"\njudges pick the same best order on {sum(1 for f in both if f[1] == f[2])} of {len(both)} queries")
    json.dump(rows, open(f"{out}/scored.json", "w"), indent=1)


if __name__ == "__main__":
    if sys.argv[1] == "packets":
        packets(*sys.argv[2:5])
    elif sys.argv[1] == "score":
        score(sys.argv[2])
