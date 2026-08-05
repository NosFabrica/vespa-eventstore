# CJK body search — a measured recipe, and why it is not merged yet

CJK **body** text is not searchable. A Japanese or Chinese sentence in
`search_text` is one token to the default linguistics, so no query reaches
inside it. CJK profile **names** are unaffected and work today: they ride
`NearText`'s raw-byte attributes (`withCjkSuffixes`), which never pass through
Vespa's tokenizer at all.

`RankRegressionIT` pins the broken behaviour (doc 46) so that a fix trips the
assertion. This note records what a fix looks like, measured on a live Vespa on
2026-08-05, and the two things that stopped it going in beside the
`stemming: none` change.

## What works

Three parts, and all three are needed:

```xml
<!-- services.xml, inside <container> -->
<component id="com.yahoo.language.lucene.LuceneLinguistics" bundle="lucene-linguistics">
  <config name="com.yahoo.language.lucene.lucene-analysis">
    <analysis>
      <item key="en">
        <tokenizer><name>standard</name></tokenizer>
        <tokenFilters>
          <item><name>lowercase</name></item>
          <item><name>asciiFolding</name></item>
          <item><name>cjkWidth</name></item>
          <item><name>cjkBigram</name></item>
        </tokenFilters>
      </item>
    </analysis>
  </config>
</component>
```

```
// event.sd, FIRST field in `document event` — set_language must run
// before the fields that use it
field language type string {
    indexing: "en" | set_language
}
```

The bundle ships **inside** the `vespaengine/vespa` image
(`/opt/vespa/lib/jars/lucene-linguistics-jar-with-dependencies.jar`), so nothing
is vendored and the published artifact does not grow.

Why each part:

- **`cjkBigram`** is what makes CJK reachable. Nothing segments those scripts
  into words here, so runs become overlapping bigrams and a query bigram matches
  inside them.
- **The pinned document language** is what makes it *correct*. The config map is
  keyed by language with no wildcard, so any language left unconfigured falls
  back to Lucene's own analyzer for it — with a stemmer. Pinning every document
  to the single configured analysis is the only way the document side and the
  query side (which declares no language) can agree. `"en"` is a label for
  "the one configured analysis", not a claim about content.
- **No stemmer** in that analysis. Adding the component *without* pinning the
  language reintroduces exactly the bug the `stemming: none` change fixed, and
  reintroduces it on English: measured, `wolves` → 0 hits while `wolv` → 1, and
  `running` → 0 while `run` → 1, because Lucene stems at index time regardless
  of `stemming: none` while the query side still honours it.

## Measured

All four languages, one analysis, on a live Vespa:

| | queries | result |
| --- | --- | --- |
| ja | 中村太郎, 東京, 会社, 中村 | all hit |
| zh | 北京, 公司, 我们 | all hit |
| pt | esposa, divórcio, divorcio, desabafa | all hit |
| en | wolves, running, report, network, bitcoin | all hit |

## Why it is not merged

**1. It changes what a match MEANS for CJK.** With bigrams there is no way to
tell a substring from a whole token, so `中村` is an *exact* index match inside
`中村太郎` and lands in the token band — where Latin `dell` inside `ODELL` is
deliberately confined to the weak tier. CJK queries therefore lose the
exact > near > weak discrimination Latin gets and rank inside one band by trust
and bm25. `RankRegressionIT` caught this immediately (`"中村" -> 中村太郎`
arrived `name`, not `near`). It is arguably the right trade against a baseline
of no CJK results at all, but it is a semantic decision, not a bug fix.

**2. It changes tokenization corpus-wide, and one regression is unresolved.**
UAX#29 does not break letter-dot-letter, so `standard` keeps
`plentharn.invalid` and `vitorpamplona.com` whole where the current tokenizer
splits them. Measured effect: a `website`-only match dropped from the
`affiliation` tier to `weak`, because the exact clause no longer reaches the
domain segment. A `wordDelimiterGraph` filter with `preserveOriginal` is the
obvious repair and did **not** fix it on the first attempt — unresolved.

`SearchPrefixLadderIT` was never run against this configuration, and it is the
suite that pins as-you-type recall through `nip05`/`lud16`/`website` — precisely
the fields this disturbs.

**3. It needs a full re-index**, like any analysis change: a
`validation-overrides.xml` allowing `indexing-change` on an existing cluster,
then Vespa's reindexing. Fresh deploys are unaffected (verified).

## If picking this up

Its own change, with its own measurement — not a rider on a ranking PR. In
order: resolve the `wordDelimiterGraph` behaviour and re-verify the identity
columns; run **both** `RankRegressionIT` and `SearchPrefixLadderIT` against it;
decide explicitly whether CJK substrings belong in the token band, and if not,
whether a separate CJK column (fed like `NearText`, gram-matched, with its own
rung) is the better shape — that keeps the analyzer untouched at the cost of a
feed-side projection and a new rung.
