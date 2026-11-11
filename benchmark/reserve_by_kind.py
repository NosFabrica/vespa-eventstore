#!/usr/bin/env python3
"""Which kinds occupy proton's growth reserve, and how much each one makes it grow (#206).

Proton's reserve belongs to STRUCTURES, not kinds: each attribute's enum store,
dictionary, multi-value arrays, posting lists and per-document vector grows by its
own rule (docs/attribute-memory.md §7.2), and every kind that writes into a
structure shares its reserve. So a kind's part of the reserve is

    sum over structures s of  used_s(kind)  x  reserve_ratio_s

where used_s(kind) is what one document of that kind adds to s (KindCostIT,
measured per shape) and reserve_ratio_s is (allocated - used) / used for s,
averaged over a growth run (AttributeGrowthIT). The average is the honest unit:
a structure's reserve is a sawtooth, so the mean is what a byte of use costs over
time, and the max is the worst point of the cycle that was observed.

Every growth step is proportional to the structure's size, so a kind that adds
X used bytes to s makes s grow by X x (1 + reserve_ratio_s) over time: the same
number, read as "how much this kind forces memory to grow".

Inputs are the two ITs' JSON outputs (written under benchmark/build/measurements/)
and a corpus's event counts per kind. The defaults are the live relay's counts
of 2026-11-11 (COUNT with include:spam), mapped to the nearest measured shape;
the mapping and every assumption is printed.

    python3 benchmark/reserve_by_kind.py \\
        --growth benchmark/build/measurements/attribute-growth-stock.json \\
        --kinds benchmark/build/measurements/kind-cost.json --nodes 4
"""

import argparse
import json

# Live relay, 2026-11-11, COUNT ... include:spam. 816,612,436 events in all.
LIVE_TOTAL = 816_612_436
LIVE = [
    # (label, events, shape measured by KindCostIT, note)
    ("1 note [search]", 264_166_719, "NOTE", ""),
    ("30382 trust assertion", 291_136_893, "TRUST", "event: card about a subject other cards name"),
    ("5 deletion", 80_658_281, "REACTION", "assumed reaction-shaped (e + p tags)"),
    ("0 profile [search]", 66_806_328, "PROFILE", ""),
    ("6 repost", 32_557_065, "REPOST", ""),
    ("30383 event assertion", 14_027_038, "ASSERTION", "assumed 30382-shaped"),
    ("10002 relay list", 12_860_994, "REACTION", "assumed reaction-shaped"),
    ("7 reaction", 10_804_459, "REACTION", ""),
    ("3 contact list", 8_464_421, "CONTACTS", "assumed 150 p tags"),
    ("9735 zap receipt", 6_976_849, "ZAP", ""),
]
# Cards signed by a service some kind-10040 names: each one is also a reputation cell.
ENROLLED_CARDS = 199_312_435

# Structures the growth run did not cover (or used under 1 MiB): the rule's own
# average over a cycle. A rule that grows capacity by g, fed at a steady rate, holds
# on average g / (2 + g) of its use free, and g at the top of the cycle.
HASH_DICTIONARIES = {"id", "pubkey", "owner", "tag_index"}  # dictionary { hash } in event.sd


# A structure's OWN measured ratio counts only if it was this big in the growth run.
# Below that it sits on its minimum buffer (measured: pubkey's enum store at 1.6 MiB
# held a flat 47%, min = max), which says nothing about the structure at scale; it
# then takes its CLASS's ratio, pooled over the structures of that class that were
# big enough, weighted by their use.
MIN_OWN_BYTES = 8 << 20


def structure_class(structure):
    attribute, component = structure.split(" :: ")
    if component == "dictionary":
        return "dictionary (hash)" if attribute in HASH_DICTIONARIES else "dictionary (btree)"
    return component


def rule(structure):
    attribute, component = structure.split(" :: ")
    g = 2.0 if component == "dictionary" and attribute in HASH_DICTIONARIES else 0.2  # hash shard capacity x3; else +20%
    return {"mean": g / (2 + g), "max": g}


def effective_ratios(measured):
    pooled = {}
    for s, r in measured.items():
        if r["used"] >= MIN_OWN_BYTES:
            c = pooled.setdefault(structure_class(s), [0.0, 0.0, 0.0])
            c[0] += r["used"] * r["mean"]
            c[1] += r["used"] * r["max"]
            c[2] += r["used"]
    classes = {k: {"mean": v[0] / v[2], "max": v[1] / v[2]} for k, v in pooled.items()}

    def ratio(s):
        if s in measured and measured[s]["used"] >= MIN_OWN_BYTES:
            return measured[s], "own"
        if structure_class(s) in classes:
            return classes[structure_class(s)], "class"
        return rule(s), "rule"

    return ratio, classes


def reserve_per_doc(used, ratio):
    mean = peak = 0.0
    by_struct = {}
    ruled = 0.0
    for s, u in used.items():
        if u <= 0:
            continue
        r, source = ratio(s)
        r_mean, r_max = r["mean"], r["max"]
        if source == "rule":
            ruled += u
        mean += u * r_mean
        peak += u * r_max
        by_struct[s] = u * r_mean
    return mean, peak, by_struct, ruled


def gib(b):
    return b / 2**30


def main():
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--growth", required=True, help="AttributeGrowthIT's attribute-growth-<variant>.json")
    ap.add_argument("--kinds", required=True, help="KindCostIT's kind-cost.json")
    ap.add_argument("--nodes", type=int, default=4, help="content nodes the events are split across")
    args = ap.parse_args()

    ratio, classes = effective_ratios(json.load(open(args.growth)))
    kinds = json.load(open(args.kinds))
    shapes = {name: s["used_per_doc"] for name, s in kinds["shapes"].items()}
    trust = kinds["trust"]
    # A card about a subject other cards already name (the second pass) is the common
    # case on a relay where 1,550 services score largely the same people.
    shapes["TRUST"] = trust[1]["event_used_per_card"]
    reputation_per_cell = trust[-1]["reputation_used_per_card"]  # the dense pass: subjects at ~50 cells

    rows = []
    for label, events, shape, note in LIVE:
        used = shapes[shape]
        mean, peak, by_struct, ruled = reserve_per_doc(used, ratio)
        top = max(by_struct.items(), key=lambda kv: kv[1])
        rows.append((label, events, sum(v for v in used.values() if v > 0), mean, peak, top, ruled, note, False))
    covered = sum(r[1] for r in rows)
    # The reputation cells: global, so each node holds all of them.
    mean, peak, by_struct, ruled = reserve_per_doc(reputation_per_cell, lambda s: (rule(s), "rule"))
    top = max(by_struct.items(), key=lambda kv: kv[1])
    rows.append(
        (
            "30382 -> reputation cells (GLOBAL)",
            ENROLLED_CARDS,
            sum(v for v in reputation_per_cell.values() if v > 0),
            mean,
            peak,
            top,
            ruled,
            "every node holds all of it; ratio by rule (tensor store not in the growth run)",
            True,
        )
    )

    per_node = lambda r, b: b if r[8] else b / args.nodes  # noqa: E731
    total_reserve_node = sum(per_node(r, r[1] * r[3]) for r in rows)
    print("\nclass ratios (structures >= 8 MiB in the growth run, weighted by use):")
    for k, v in sorted(classes.items(), key=lambda kv: -kv[1]["mean"]):
        print(f"  {k:<24} mean {100 * v['mean']:5.1f}%   max {100 * v['max']:6.1f}%")
    print(f"\nreserve by kind — {args.nodes} content nodes; {covered:,} of {LIVE_TOTAL:,} live events covered\n")
    print(
        f"{'kind':<36} {'events':>12} {'used B/doc':>11} {'reserve B/doc':>14} {'(peak)':>8} "
        f"{'reserve GiB/node':>17} {'share':>7}  largest reserve holder"
    )
    for r in sorted(rows, key=lambda r: -per_node(r, r[1] * r[3])):
        label, events, used, mean, peak, top, ruled, note, glob = r
        node = per_node(r, events * mean)
        print(
            f"{label:<36} {events:>12,} {used:>11.0f} {mean:>14.0f} {peak:>8.0f} {gib(node):>17.2f} "
            f"{100 * node / total_reserve_node:>6.1f}%  {top[0]} ({top[1]:.0f} B)"
        )
        if note:
            print(f"{'':<36} {note}")
    print(f"\n{'mean reserve per node, these kinds':<36} {gib(total_reserve_node):.2f} GiB")
    print(
        "\n'reserve B/doc' is the time-averaged reserve one document of the kind carries; it is also how far\n"
        "past its used bytes it makes memory grow. '(peak)' is the same at the most-reserved point observed\n"
        "for each structure. A structure under 8 MiB in the growth run takes its class's ratio; one the run\n"
        "did not have at all (the reputation tensors) takes its rule's average, g / (2 + g)."
    )


if __name__ == "__main__":
    main()
