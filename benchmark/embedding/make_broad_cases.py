#!/usr/bin/env python3
"""Write benchmark/rank_cases_broad.json: the regression set for the intent-gated perfect-match rung.

    python3 benchmark/embedding/make_broad_cases.py > benchmark/rank_cases_broad.json

docs/embedding-experiment.md §9. The 54-case file is mostly reported bad searches. This set
asks the opposite question: across ordinary queries of every shape, does the change make
anything WORSE? The queries were chosen without looking at any page. Hints came from the
names, apps and hashtags of trusted authors in the eval capture. No user's search log was used:
the relay's term log is admin-only and quotes what people typed.

Every case is `split: broad`, so `rankAb --split broad` never mixes it into the tuning topics.
"""
import json

PEOPLE = ["Alex Gleason", "Adam Curry", "Saifedean Ammous", "James Lavish", "Joe Burnett", "Fran Finney",
          "Frank Corva", "Kody Low", "jack mallers", "Jack Spirko", "Nathan Day", "Nick Anthony",
          "Leonard Pickard", "Terry Yiu", "Joe Carlasare", "Natalie Brunell", "Michael Saylor",
          "Will Casarin", "Derek Ross", "Edward Snowden"]
# Single-word handles: where the perfect-match rung matters most, and where the probe must say "navigational".
HANDLES = ["fiatjaf", "jb55", "hodlbod", "Gigi", "pablof7z", "rabble", "Snowden", "Kieran", "verbiricha", "Semisol"]
APPS = ["stacker news", "nostr.build", "nostr.watch", "zapstore", "snort", "coracle", "nostrudel", "habla",
        "highlighter", "wavlake", "olas", "yakihonne", "nostur", "ditto", "mutiny", "phoenix", "breez",
        "minibits", "SimpleX Chat", "No Bullshit Bitcoin"]
TOPICS = ["technology", "freedom", "photography", "sports", "travel", "security", "cryptography",
          "decentralization", "nature", "monero", "encryption", "woodworking", "history", "philosophy",
          "podcast", "ecash", "politics", "money", "memes", "surveillance", "censorship", "sovereignty",
          "business", "gardening", "cooking", "books", "health", "inflation", "mining", "education",
          "gaming", "football", "chess", "poetry", "science", "space", "economics", "wallet", "relay", "meditation"]
TOPICS_MULTI = ["human rights", "financial freedom", "austrian economics", "satoshi nakamoto", "open source",
                "freedom tech", "bitcoin mining", "self custody", "cold storage", "nostr relay", "machine learning",
                "social media", "central bank", "climate change", "mental health", "hard money", "game theory",
                "zero knowledge proofs", "lightning wallet", "sound money", "privacy tools", "artificial intelligence",
                "home cooking", "film photography", "value for value"]
PREFIXES = [("Saife", "Saifedean Ammous"), ("Alex Gle", "Alex Gleason"), ("James La", "James Lavish"),
            ("stacker n", "Stacker News"), ("nostr.bu", "nostr.build"), ("fiatj", "fiatjaf"),
            ("jack mal", "Jack Mallers"), ("wavla", "Wavlake"), ("zapst", "Zapstore"), ("Natalie Bru", "Natalie Brunell"),
            ("photogr", "photography (the topic)"), ("decentral", "decentralization (the topic)")]
MULTILINGUAL = [("liberdade", "Portuguese: freedom"), ("privacidad", "Spanish: privacy"), ("economía", "Spanish: economy"),
                ("dinheiro", "Portuguese: money"), ("café", "Portuguese/Spanish/French: coffee or a café"),
                ("música", "Portuguese/Spanish: music"), ("Freiheit", "German: freedom"), ("Wirtschaft", "German: economy"),
                ("ビットコイン", "Japanese: bitcoin"), ("怪談", "Japanese: ghost stories (kaidan)"), ("política", "Portuguese/Spanish: politics"),
                ("família", "Portuguese: family"), ("filosofia", "Portuguese/Italian: philosophy"), ("criptografia", "Portuguese: cryptography"),
                ("viagem", "Portuguese: travel"), ("futebol", "Portuguese: football"), ("Geld", "German: money"),
                ("sécurité", "French: security"), ("liberté", "French: freedom"), ("自由", "Japanese/Chinese: freedom")]

NOTE = "BROAD (2026-11-25): regression set for the intent-gated perfect-match rung, chosen before any page was seen (benchmark/embedding/make_broad_cases.py)."


def case(query, intent, need):
    return {"query": query, "expect": "", "intent": intent, "need": need, "split": "broad", "note": NOTE}


def main():
    out = []
    for p in PEOPLE + HANDLES:
        out.append(case(p, "navigational",
                        f"NAVIGATIONAL: the person or account \"{p}\". 3 = their own profile; 2 = their own posts or "
                        f"content substantively about them; 1 = an incidental mention; 0 = other accounts sharing part of "
                        f"the name, impersonators, unrelated."))
    for a in APPS:
        out.append(case(a, "navigational",
                        f"NAVIGATIONAL: the Nostr/bitcoin app or project \"{a}\". 3 = the app itself (its profile, app "
                        f"handler, official announcements); 2 = content substantively about it (reviews, guides); "
                        f"1 = a mention; 0 = the ordinary-word sense of the name, or unrelated."))
    for t in TOPICS + TOPICS_MULTI:
        out.append(case(t, "topical",
                        f"TOPICAL: content about {t}. 3 = substantively about it; 2 = clearly relevant; 1 = mentions it "
                        f"in passing, a bare hashtag, or a different sense of the word; 0 = unrelated or spam."))
    for q, target in PREFIXES:
        out.append(case(q, "prefix",
                        f"AS-YOU-TYPE toward \"{target}\". 3 = its profile / app / the topic's best content; 2 = closely "
                        f"related; 1 = a plausible other completion; 0 = unrelated."))
    for q, gloss in MULTILINGUAL:
        out.append(case(q, "multilingual",
                        f"{gloss}. 3 = content substantively about it in any language; 2 = clearly relevant; "
                        f"1 = a passing mention or another sense; 0 = unrelated."))
    print(json.dumps(out, ensure_ascii=False, indent=1))


main()
