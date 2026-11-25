#!/usr/bin/env python3
"""Build the EXPERIMENT-ONLY application package for the embedding experiment.

    ./gradlew :engine:vespaAppZip
    python3 benchmark/embedding/build_app.py engine/build/vespa-app/vespa-app.zip <model-dir> <out-dir>

<model-dir> holds `model.onnx` and `tokenizer.json` of intfloat/multilingual-e5-small
(the `onnx/` folder of its Hugging Face repository). Read docs/embedding-experiment.md.

This never touches engine/app: it rewrites a COPY of the built package. What it adds:

  services.xml  a `hugging-face-embedder` component, `e5`, with e5's `query: ` / `passage: `
                prefixes, mean pooling and normalized output.
  event.sd      a synthetic `embedding` field: every naming and text field of the event,
                embedded at feed time, as a bfloat16 HNSW attribute (angular distance);
                and, on the `search` profile only, four knobs that are INERT AT THEIR
                DEFAULTS, so the shipped ranking is reproduced exactly until a config
                moves one:

     query(w_sem_tier)  a SEMANTIC RUNG. The first phase becomes
                        max(floored_text_score(), w_sem_tier * sem_norm()) x trust x recency,
                        so a document the word matcher never matched (a nearestNeighbor-only
                        hit) has a rung to stand on, and a weak lexical match that is strongly
                        on-topic can rise to it. 0 = max(x, 0) = x for every document.
     query(w_sem_mult)  ABOUT-NESS inside the bands: the second phase is multiplied by
                        (1 + w_sem_mult * sem_norm()). 0 = x1.
     query(sem_floor),  how the cosine maps onto 0..1, linear between the two, clamped.
     query(sem_ceil)    e5 similarities crowd together: measured on the 20k-event eval
                        corpus (docs/embedding-experiment.md §2.1), the best on-topic
                        notes reach 0.87..0.90 and the 100th-closest of ~11k sits near
                        0.84, so 0.84..0.90 spans "unrelated" to "about it".
     query(q_emb)       the query's embedding; rankAb sends `embed(e5, @sem_text)`.

  THE INTENT-AWARE LADDER (docs/embedding-experiment.md §8). query(topical) (declared by the
  shipped schema since §10, whose own topical_perfect_drop() this package replaces with
  intent_precision(), its superset), 0..1, says how
  much the query asks ABOUT a subject rather than for one entity; the caller decides it
  (rankAb: the case file's label as an oracle, or an entity probe). At 0, nothing changes.
  At 1:

     query(topical_cap)          the first-phase text score is capped here, so a document
                                 NAMED the word (an event, a place, an app called "Coffee",
                                 on the ~130 000 name rung) no longer outranks every
                                 document ABOUT it by construction. Linear in topical:
                                 floored - topical * max(0, floored - cap).
     query(topical_drop_precision)  1 (default): the second phase's words / exactness /
                                 perfect-match boosts, which all read NAMING fields, are
                                 scaled by (1 - topical). 0: only the perfect-match rung
                                 is, the one that pays a document for its naming field
                                 BEING the query (an app called "Coffee") and not for a
                                 title holding it ("On Freedom of Speech"). The body
                                 phrase and the phrase band stay either way.
     query(topical_keep_reference)  1: the perfect-match rung is kept on wiki (30818) and
                                 long-form (30023) documents even for a topical query: a
                                 reference page titled exactly the topic IS its answer.
     query(w_sem_tier_topical),  the semantic rung and the aboutness multiplier, added to
     query(w_sem_mult_topical)   w_sem_tier / w_sem_mult in proportion to topical, so a
                                 config can switch them on for topical queries only. The
                                 first run showed a uniform multiplier losing on names.
"""
import re, shutil, sys, zipfile
from pathlib import Path

DIM = 384  # multilingual-e5-small

COMPONENT = """
    <!-- EMBEDDING EXPERIMENT (benchmark/embedding/build_app.py): NOT the shipped package. -->
    <component id="e5" type="hugging-face-embedder">
      <transformer-model path="models/e5.onnx" />
      <tokenizer-model path="models/tokenizer.json" />
      <max-tokens>512</max-tokens>
      <pooling-strategy>mean</pooling-strategy>
      <normalize>true</normalize>
      <prepend>
        <query>query: </query>
        <document>passage: </document>
      </prepend>
    </component>
"""

FIELD = f"""
    # EMBEDDING EXPERIMENT (benchmark/embedding/build_app.py): NOT the shipped schema.
    # Every naming and text field of the event, embedded at feed time. Missing
    # fields contribute nothing (`||` takes the first non-null).
    field embedding type tensor<bfloat16>(x[{DIM}]) {{
        # One line: the indexing language does not continue an expression across lines.
        indexing: (input name || "") . " " . (input display_name || "") . " " . (input about || "") . " " . (input search_primary || "") . " " . (input search_secondary || "") . " " . (input search_text || "") | embed e5 | attribute | index
        attribute {{
            distance-metric: angular
        }}
        index {{
            hnsw {{
                max-links-per-node: 16
                neighbors-to-explore-at-insert: 200
            }}
        }}
    }}

"""

PROFILE_ADDITIONS = f"""
        # ---- EMBEDDING EXPERIMENT (benchmark/embedding/build_app.py) ----
        # Inert at these defaults: w_sem_tier 0 makes max(x, 0) = x on a
        # non-negative score, w_sem_mult 0 makes the second-phase factor 1.
        inputs {{
            query(q_emb) tensor<float>(x[{DIM}])
            query(w_sem_tier) double: 0.0
            query(w_sem_mult) double: 0.0
            query(sem_floor) double: 0.84
            query(sem_ceil) double: 0.90
            query(topical_cap) double: 550.0
            query(w_sem_tier_topical) double: 0.0
            query(w_sem_mult_topical) double: 0.0
            query(topical_drop_precision) double: 1.0
            query(topical_keep_reference) double: 0.0
            query(ref_30040) double: 0.0
        }}
        # Guarded twice. With no q_emb sent the query tensor is all zeros, so
        # nothing reads the cosine unless a knob is on. And an event with no
        # text at all (an empty kind 1, a bare reaction) gets no embedding, so
        # its cosine is NaN, which would poison max() and the drop limit; it
        # reads 0, as a document with nothing to be about should.
        function sem_cos() {{
            expression: cosine_similarity(attribute(embedding), query(q_emb), x)
        }}
        function sem_sim() {{
            expression: if(query(w_sem_tier) + query(w_sem_mult) + query(topical) * (query(w_sem_tier_topical) + query(w_sem_mult_topical)) > 0, if(isNan(sem_cos()) == 1, 0.0, sem_cos()), 0.0)
        }}
        function sem_norm() {{
            expression: max(0.0, min(1.0, (sem_sim() - query(sem_floor)) / (query(sem_ceil) - query(sem_floor))))
        }}
        function sem_tier_score() {{
            expression: (query(w_sem_tier) + query(topical) * query(w_sem_tier_topical)) * sem_norm()
        }}
        function sem_mult() {{
            expression: 1.0 + (query(w_sem_mult) + query(topical) * query(w_sem_mult_topical)) * sem_norm()
        }}
        # The intent-aware ladder: the name rungs capped, the naming-field
        # precision boosts off, both in proportion to query(topical).
        function intent_text_score() {{
            expression: floored_text_score() - query(topical) * max(0.0, floored_text_score() - query(topical_cap))
        }}
        # topical_drop_precision 1 drops every naming-field boost (the first
        # cut, §8.1); 0 drops only the perfect-match rung, the one that pays a
        # document for its naming field BEING the query (§8.2).
        function perfect_boost() {{
            expression: query(w_perfect_pop) * perfect_share() * pow(perfect_match(), query(w_perfect_pow))
        }}
        # reference_kind() is the shipped schema's (§10); REFERENCE_OLD below widens it for §11.
        function intent_precision() {{
            expression: precision_boost_pop() - query(topical) * (query(topical_drop_precision) * precision_boost_pop() + (1.0 - query(topical_drop_precision)) * (1.0 - query(topical_keep_reference) * reference_kind()) * perfect_boost())
        }}
"""

# §11: whether a publication index (30040) is a reference kind too. 0 = the shipped reference_kind().
REFERENCE_OLD = "if(attribute(kind) == 30023, 1.0, 0.0))"
REFERENCE_NEW = "if(attribute(kind) == 30023, 1.0, if(attribute(kind) == 30040, query(ref_30040), 0.0)))"

FIRST_OLD = "expression: floored_text_score() * wot_mult() * recency_mult()"
FIRST_NEW = "expression: max(intent_text_score(), sem_tier_score()) * wot_mult() * recency_mult()"
SECOND_OLD = "expression: (firstPhase + (precision_boost_pop() - topical_perfect_drop() + body_phrase_pop()"
SECOND_NEW = "expression: (firstPhase + (intent_precision() + body_phrase_pop()"
SECOND_TAIL_OLD = "* recency_mult()) * phrase_mult()"
SECOND_TAIL_NEW = "* recency_mult()) * phrase_mult() * sem_mult()"


def patch_schema(sd: str) -> str:
    # The synthetic field goes beside the other synthetic fields, ahead of the fieldsets.
    anchor = sd.index("    fieldset phrase_surface {")
    sd = sd[:anchor] + FIELD + sd[anchor:]

    start = sd.index("    rank-profile search inherits text_relevance {")
    end = sd.index("\n    rank-profile ", start + 10)
    body = sd[start:end]
    for old, new in ((FIRST_OLD, FIRST_NEW), (SECOND_OLD, SECOND_NEW), (SECOND_TAIL_OLD, SECOND_TAIL_NEW), (REFERENCE_OLD, REFERENCE_NEW)):
        if body.count(old) != 1:
            sys.exit(f"the search profile no longer reads `{old}` exactly once: update build_app.py to the schema")
        body = body.replace(old, new)
    head = "    rank-profile search inherits text_relevance {\n"
    body = head + PROFILE_ADDITIONS + body[len(head):]
    body = body.replace("        match-features {\n", "        match-features {\n            sem_sim\n            sem_norm\n            intent_text_score\n", 1)
    return sd[:start] + body + sd[end:]


def patch_services(xml: str) -> str:
    anchor = "<document-api />"
    if xml.count(anchor) != 1:
        sys.exit("services.xml no longer has exactly one <document-api />: update build_app.py")
    return xml.replace(anchor, anchor + "\n" + COMPONENT, 1)


def main():
    zip_path, model_dir, out = Path(sys.argv[1]), Path(sys.argv[2]), Path(sys.argv[3])
    if out.exists():
        shutil.rmtree(out)
    with zipfile.ZipFile(zip_path) as z:
        z.extractall(out)
    sd = out / "schemas" / "event.sd"
    sd.write_text(patch_schema(sd.read_text()))
    services = out / "services.xml"
    services.write_text(patch_services(services.read_text()))
    (out / "models").mkdir()
    shutil.copy(model_dir / "model.onnx", out / "models" / "e5.onnx")
    shutil.copy(model_dir / "tokenizer.json", out / "models" / "tokenizer.json")
    print(f"wrote the experiment package to {out}")


if __name__ == "__main__":
    main()
