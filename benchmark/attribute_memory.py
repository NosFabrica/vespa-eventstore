#!/usr/bin/env python3
"""Per-field attribute memory for the `event` schema — measure first, then model.

    # MEASURE a live deployment (the metrics-proxy on any node, port 19092):
    python3 benchmark/attribute_memory.py --metrics http://vespa:19092/metrics/v2/values

    # or from a saved dump / the per-node state API:
    curl -s http://vespa:19092/metrics/v2/values > m.json
    python3 benchmark/attribute_memory.py --json m.json

    # MODEL what a schema change would free (Vespa's own sizing formulas):
    python3 benchmark/attribute_memory.py --model --docs 176700000
    python3 benchmark/attribute_memory.py --model --docs 176700000 --params corpus.json

The measured mode answers "where did the RAM go" exactly. The model answers
"what would `paged` / dropping `fast-search` / dropping the field actually
buy", which measurement alone cannot: it needs the component split inside a
field, and Vespa reports one number per attribute.

The split the model exists for (verified against Vespa's source — see
docs/attribute-memory.md):

  PAGEABLE  document vector, multivalue mapping, enum store VALUES
            -> `attribute: paged` moves these to a memory-mapped file
  RESIDENT  enum store DICTIONARY, posting lists
            -> stays in RAM whatever you do, as long as the field is a
               fast-search attribute

Field types are parsed out of engine/app/schemas/event.sd, so this never
drifts from the shipped schema. The per-field U (unique values) / V (values
per document) / average string length are ASSUMPTIONS — the defaults below are
labelled and are the only guesses in the model. Override them with --params, a
JSON object of {"field": {"u": …, "v": …, "vw": …}}.

Formulas and abbreviations: https://docs.vespa.ai/en/attributes.html#sizing
"""
import argparse
import json
import re
import sys
import urllib.request

ROF = 6 / 5  # resize overhead factor (structures are 5/6 full on average)
EIW = 4  # enum index width
PIW = 4  # posting index width
MIW = 4  # multivalue index width
WW_ARRAY = 0  # weight width — 0 for arrays, 4 for weighted sets
NUMERIC_FW = {"byte": 1, "int": 4, "long": 8, "float": 4, "double": 8, "bool": 1}

# The only guesses in this file. Every one is a property of the CORPUS, not of
# the schema, so it cannot be derived from the schema or from Vespa's metrics —
# but each is checkable against the store itself, and the check is named.
#
#   u   distinct values in the corpus
#   v   average values per document, averaged over ALL documents (an empty
#       array still costs its document-vector slot, which is the point)
#   vw  average string length in bytes (0 for numerics)
#
# Defaults describe a large mirrored Nostr corpus (the shape issue #69 reports:
# 176.7M events). Measure yours: `u` for tag_index is
# `buildDistinctCount(tag_index)`, `u` for pubkey is the distinct-author
# grouping, `v` is total values / documents from a visit sample.
DEFAULT_PARAMS = {
    # every id is unique by construction — u = D is exact, not a guess
    "id": {"u": None, "v": 1, "vw": 64},
    "pubkey": {"u": 5_000_000, "v": 1, "vw": 64},
    "owner": {"u": 5_000_000, "v": 1, "vw": 64},
    "created_at": {"u": 60_000_000, "v": 1, "vw": 0},
    # a reference attribute enum-stores the parent GID (one per distinct
    # author). Vespa publishes no formula for the reverse mapping it also keeps
    # (parent -> child local ids), so this row is a FLOOR, low by ~1 GiB.
    "author_ref": {"u": 5_000_000, "v": 1, "vw": 0},
    "kind": {"u": 300, "v": 1, "vw": 0},
    "expires_at": {"u": 200_000, "v": 1, "vw": 0},
    "tag_index": {"u": 120_000_000, "v": 3.0, "vw": 60},
    # the merged near columns (2026-08-11). `v` is the two granularities' sum
    # MINUS their overlap, and the overlap is measured, not guessed:
    # NearMergeSizingTest derives both from real extraction and reports the
    # element reduction — 31.9% on a weighted mix of real name shapes (50% where
    # the two derive the identical list), 42.6% on article titles. Applied
    # conservatively here: the name column takes the mixed-shape figure rather
    # than the synthetic corpus's 50%.
    "name_near": {"u": 5_000_000, "v": 0.07, "vw": 10},
    "search_primary_near": {"u": 25_000_000, "v": 1.08, "vw": 9},
    "search_secondary_tokens": {"u": 15_000_000, "v": 1.2, "vw": 9},
    "affil_tokens": {"u": 6_000_000, "v": 0.12, "vw": 10},
}

# Not an attribute in the schema, but the largest implicit one: Vespa keeps a
# document meta store per subdb. ~30 B/doc documented, 52 B/doc in Vespa's own
# worked example — the range is the honest uncertainty.
DOC_META_STORE_BYTES = (30, 52)


# ---------------------------------------------------------------- schema ----
FIELD_RE = re.compile(r"^\s*field\s+(\w+)\s+type\s+([\w<>]+)\s*\{", re.M)


def parse_schema(path):
    """(name, type, collection, fast_search, is_attribute) for every attribute field."""
    text = open(path, encoding="utf-8").read()
    out = []
    for m in FIELD_RE.finditer(text):
        name, decl = m.group(1), m.group(2)
        body = _body(text, m.end() - 1)
        if not re.search(r"^\s*indexing:.*\battribute\b", body, re.M):
            continue
        coll, base = ("array", decl[6:-1]) if decl.startswith("array<") else ("single", decl)
        if base.startswith("reference"):
            base = "reference"
        fast = bool(re.search(r"^\s*attribute:.*\bfast-search\b", body, re.M))
        out.append({"name": name, "type": base, "collection": coll, "fast_search": fast})
    return out


def _body(text, brace_at):
    """The text between the brace at [brace_at] and its match."""
    depth, i = 0, brace_at
    while i < len(text):
        if text[i] == "{":
            depth += 1
        elif text[i] == "}":
            depth -= 1
            if depth == 0:
                return text[brace_at + 1 : i]
        i += 1
    return text[brace_at:]


# ----------------------------------------------------------------- model ----
def model_field(f, docs, params):
    """Vespa's sizing formulas, split into what `paged` can move and what it cannot.

    Enum store entry = (FW + VW) + 4 + (EIW + PIW): the (EIW + PIW) tail is the
    DICTIONARY entry (enum index + posting index) and is never paged out; the
    (FW + VW + 4) head is the stored value plus its ref count, and is.
    """
    p = dict(params.get(f["name"], {}))
    u = p.get("u") or docs
    v = p.get("v", 1)
    vw = p.get("vw", 0)
    is_string = f["type"] == "string"
    is_ref = f["type"] == "reference"
    array = f["collection"] == "array"
    fast = f["fast_search"]
    fw = 1 if is_string else (12 if is_ref else NUMERIC_FW.get(f["type"], 4))
    # enum-backed when the value is a string, a reference, or fast-search;
    # otherwise the raw value sits in the document vector.
    enumerated = is_string or is_ref or fast

    doc_vector = docs * (MIW if array else (EIW if enumerated else fw)) * ROF
    mv_mapping = docs * v * ((EIW if enumerated else fw) + WW_ARRAY) * ROF if array else 0.0
    enum_values = u * (fw + vw + 4) * ROF if enumerated else 0.0
    dictionary = u * ((EIW + PIW) if fast else EIW) * ROF if enumerated else 0.0
    postings = docs * (v if array else 1) * (8 if array else 4) * ROF if fast else 0.0

    return {
        "field": f["name"],
        "decl": ("array<%s>" % f["type"]) if array else f["type"],
        "fast_search": fast,
        "doc_vector": doc_vector,
        "mv_mapping": mv_mapping,
        "enum_values": enum_values,
        "dictionary": dictionary,
        "postings": postings,
        "pageable": doc_vector + mv_mapping + enum_values,
        "resident": dictionary + postings,
        "total": doc_vector + mv_mapping + enum_values + dictionary + postings,
    }


# --------------------------------------------------------------- measure ----
SUFFIX = "attribute.memory_usage.allocated_bytes"


def measured_fields(payload):
    """{(field, scope): bytes} from a /metrics/v2/values or /state/v1/metrics dump."""
    out = {}
    for name, dims, value in _walk_metrics(payload):
        if not name.endswith(SUFFIX):
            continue
        field = dims.get("field")
        if not field:
            continue
        # content.proton.documentdb[.<subdb>].attribute.… — keep the subdb so a
        # `removed` subdb hoarding attributes is visible rather than summed away.
        head = name[: -len(SUFFIX)].strip(".")
        scope = head.split(".")[-1] if head.split(".")[-1] in ("ready", "notready", "removed") else "all"
        key = (field, scope, dims.get("documenttype", "?"))
        out[key] = max(out.get(key, 0), value)
    return out


def totals(payload, wanted):
    out = {}
    for name, _dims, value in _walk_metrics(payload):
        for w in wanted:
            if name.endswith(w):
                out[w] = max(out.get(w, 0), value)
    return out


def _walk_metrics(payload):
    """Yield (name, dimensions, last-value) from either metrics format."""

    def emit(container):
        for m in container.get("metrics", []) or []:
            dims = m.get("dimensions", {}) or {}
            for name, value in (m.get("values", {}) or {}).items():
                if isinstance(value, (int, float)):
                    yield name.removesuffix(".last"), dims, value

    for node in payload.get("nodes", []) or []:
        for svc in node.get("services", []) or []:
            for metric in svc.get("metrics", []) or []:
                yield from emit({"metrics": [metric]})
    # /state/v1/metrics: {"metrics": {"values": [{"name":…, "values":{"last":…}}]}}
    values = (payload.get("metrics") or {}).get("values")
    if isinstance(values, list):
        for m in values:
            dims = m.get("dimensions", {}) or {}
            vals = m.get("values", {}) or {}
            last = vals.get("last", vals.get("average", vals.get("max")))
            if isinstance(last, (int, float)):
                yield m.get("name", ""), dims, last


# ----------------------------------------------------------------- output ----
def gib(n):
    return n / (1024**3)


def bar(frac, width=18):
    filled = int(round(frac * width))
    return "#" * filled + "." * (width - filled)


def print_measured(rows, docs, schema):
    total = sum(rows.values())
    fast = {f["name"]: f["fast_search"] for f in schema}
    print(f"\nMEASURED attribute memory — {len(rows)} attributes, {gib(total):.2f} GiB allocated")
    if docs:
        print(f"{docs:,} documents -> {total / max(docs, 1):.1f} B/doc across all attributes")
    print(f"\n{'field':<26} {'subdb':<8} {'GiB':>8} {'B/doc':>8}  {'share':<20} fast-search")
    print("-" * 92)
    for (field, scope, _dt), value in sorted(rows.items(), key=lambda kv: -kv[1]):
        share = value / total if total else 0
        per_doc = f"{value / docs:8.1f}" if docs else " " * 8
        print(
            f"{field:<26} {scope:<8} {gib(value):8.2f} {per_doc}  {bar(share)} {share * 100:4.1f}%  "
            f"{'yes' if fast.get(field) else ('-' if field in {f['name'] for f in schema} else '')}"
        )
    print("-" * 92)
    print(f"{'TOTAL':<26} {'':<8} {gib(total):8.2f}")


def print_model(rows, docs):
    total = sum(r["total"] for r in rows)
    pageable = sum(r["pageable"] for r in rows)
    resident = sum(r["resident"] for r in rows)
    print(f"\nMODELLED attribute memory at {docs:,} documents (Vespa sizing formulas)")
    print(f"\n{'field':<26} {'decl':<15} {'total':>8} {'docvec':>8} {'mvmap':>8} {'enumval':>8} {'dict':>7} {'post':>8}")
    print(f"{'':<26} {'':<15} {'GiB':>8} {'GiB':>8} {'GiB':>8} {'GiB':>8} {'GiB':>7} {'GiB':>8}")
    print("-" * 96)
    for r in sorted(rows, key=lambda r: -r["total"]):
        print(
            f"{r['field']:<26} {r['decl']:<15} {gib(r['total']):8.2f} {gib(r['doc_vector']):8.2f} "
            f"{gib(r['mv_mapping']):8.2f} {gib(r['enum_values']):8.2f} {gib(r['dictionary']):7.2f} {gib(r['postings']):8.2f}"
        )
    print("-" * 96)
    print(f"{'TOTAL':<26} {'':<15} {gib(total):8.2f}")
    lo, hi = (docs * b for b in DOC_META_STORE_BYTES)
    print(f"\n+ document meta store (implicit attribute): {gib(lo):.2f}-{gib(hi):.2f} GiB at 30-52 B/doc")
    print(f"\nPAGEABLE (docvec + mvmap + enum values)  {gib(pageable):7.2f} GiB   <- `attribute: paged` can move this")
    print(f"RESIDENT (dictionary + posting lists)    {gib(resident):7.2f} GiB   <- stays in RAM while fast-search is on")
    print("\nPer-field ceiling on what each lever frees:")
    print(f"\n{'field':<26} {'paged frees':>12} {'no fast-search':>15} {'drop field':>12}")
    print("-" * 70)
    for r in sorted(rows, key=lambda r: -r["total"]):
        # dropping fast-search removes the postings and the posting-index half
        # of each dictionary entry; the values and document vector stay.
        no_fast = r["postings"] + (r["dictionary"] / 2 if r["fast_search"] else 0)
        print(
            f"{r['field']:<26} {gib(r['pageable']):11.2f}G {gib(no_fast):14.2f}G {gib(r['total']):11.2f}G"
            + ("" if r["fast_search"] else "   (already plain)")
        )


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--metrics", help="URL of a metrics endpoint (…:19092/metrics/v2/values)")
    ap.add_argument("--json", help="a saved metrics dump instead of --metrics")
    ap.add_argument("--model", action="store_true", help="print the formula model")
    ap.add_argument("--docs", type=int, default=0, help="document count (model; also used for B/doc)")
    ap.add_argument("--params", help="JSON of per-field {u, v, vw} overrides")
    ap.add_argument("--schema", default="engine/app/schemas/event.sd")
    args = ap.parse_args()

    schema = parse_schema(args.schema)
    if not schema:
        sys.exit(f"no attribute fields parsed from {args.schema}")

    payload = None
    if args.metrics:
        with urllib.request.urlopen(args.metrics, timeout=30) as r:
            payload = json.load(r)
    elif args.json:
        payload = json.load(open(args.json, encoding="utf-8"))

    if payload is not None:
        rows = measured_fields(payload)
        if not rows:
            sys.exit(
                "no per-field attribute metrics in this payload.\n"
                "The field-dimensioned metrics are not in the default consumer set — try\n"
                "  …/metrics/v2/values?consumer=Vespa\n"
                "or the content node's own …/state/v1/metrics."
            )
        docs = args.docs or int(
            totals(payload, ["documentdb.documents.ready"]).get("documentdb.documents.ready", 0)
        )
        print_measured(rows, docs, schema)
        t = totals(
            payload,
            [
                "documentdb.memory_usage.allocated_bytes",
                "documentdb.index.memory_usage.allocated_bytes",
                "documentdb.disk_usage",
            ],
        )
        if t:
            print("\nContext:")
            for k, v in t.items():
                print(f"  {k:<48} {gib(v):8.2f} GiB")

    if args.model:
        if not args.docs:
            sys.exit("--model needs --docs")
        params = dict(DEFAULT_PARAMS)
        if args.params:
            for k, v in json.load(open(args.params, encoding="utf-8")).items():
                params[k] = {**params.get(k, {}), **v}
        print_model([model_field(f, args.docs, params) for f in schema], args.docs)

    if payload is None and not args.model:
        ap.error("give --metrics/--json to measure, --model --docs N to model, or both")


if __name__ == "__main__":
    main()
