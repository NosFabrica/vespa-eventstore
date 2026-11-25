#!/usr/bin/env python3
"""Write benchmark/rank_cases_publications.json: does a publication (kind 30040) titled exactly the query
deserve the perfect-match rung on a topical query?

    python3 benchmark/embedding/make_publication_cases.py > benchmark/rank_cases_publications.json

docs/embedding-experiment.md §11. The shipped topical intent keeps the rung on wiki (30818) and long-form
(30023) documents only. A NIP-62 publication index (30040) titled exactly the query is usually the
book itself, but on mixed pages publications never rank near page one: most are a public-domain mirror
the canonical lens trusts at 32. So the place to measure is a publications-only read, `kinds: [30040]`,
where a book-title search reads topical (no profile or app is named "Pride and Prejudice") and the
shipped rule takes the rung off the book that IS the answer.

Titles: classics the mirror carries, and books by authors the lens trusts at 70+ (the US Constitution,
Interior Castle, Mysticism and Logic). Topics: subjects a reader browses a book tab for. Eight titles
also run on the mixed page (`|mixed`), to confirm what §10.2 predicts there: almost nothing moves.
Every case is `split: publications`.
"""
import json

TITLES = ["Pride and Prejudice", "Moby Dick", "The Republic", "Meditations", "The Prince", "Walden",
          "Leviathan", "On Liberty", "The Wealth of Nations", "Frankenstein", "Dracula", "The Odyssey",
          "Common Sense", "Beowulf", "Don Quixote", "The Iliad", "Paradise Lost", "Treasure Island",
          "Leaves of Grass", "Sense and Sensibility",
          # by authors the canonical lens trusts at 70+
          "Interior Castle", "Mysticism and Logic", "The Declaration of Independence",
          "The Bill of Rights", "The United States Constitution"]
TOPICS = ["philosophy", "history", "economics", "poetry", "war", "science", "religion", "love",
          "linguistics", "biology", "bitcoin", "nostr"]
MIXED = ["Pride and Prejudice", "The Republic", "Meditations", "The Prince", "Leviathan", "Common Sense",
         "Interior Castle", "The Bill of Rights"]

NOTE = "PUBLICATIONS (2026-11-25): is a 30040 titled exactly the query a reference document? (benchmark/embedding/make_publication_cases.py)"


def case(query, intent, need, kinds, key=None):
    c = {"query": query, "expect": "", "intent": intent, "need": need, "split": "publications", "note": NOTE}
    if kinds:
        c["kinds"] = kinds
    if key:
        c["id"] = key
    return c


def title_need(t):
    return (f"KNOWN ITEM: the book \"{t}\". 3 = a publication of that work itself (any edition or translation, "
            f"including a publication of one of its parts); 2 = a study, commentary or collection substantially about it; "
            f"1 = a mention, or a different work sharing words of the title; 0 = unrelated.")


def main():
    out = []
    for t in TITLES:
        out.append(case(t, "navigational", title_need(t), [30040], key=f"{t}|publications"))
    for t in TOPICS:
        out.append(case(t, "topical",
                        f"TOPICAL, on a publications (book) tab: publications about {t}. 3 = a book or section substantially "
                        f"about it; 2 = clearly relevant; 1 = a mention or another sense of the word; 0 = unrelated.",
                        [30040], key=f"{t}|publications"))
    for t in MIXED:
        out.append(case(t, "navigational", title_need(t) + " On a mixed page, other documents about the book can be 2 as well.",
                        [], key=f"{t}|mixed"))
    print(json.dumps(out, ensure_ascii=False, indent=1))


main()
