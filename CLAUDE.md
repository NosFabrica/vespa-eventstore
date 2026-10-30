# CLAUDE.md

See **[AGENTS.md](AGENTS.md)** — one rules file, so Claude Code and Codex read the same guidance
and the two cannot drift apart (they had). Every rule lives there; add new ones there, not here.

It holds what this store is and where the live system is, every command (including the
`-PquartzDir` local-quartz build), the module and package layering `ModuleBoundariesTest`
enforces, what goes where between quartz, this store and its consumers, the engine port and its
executable spec, recency planning, the Nostr write rules, the trust projection, the operator
settings (`EngineTuning`), the search and search-expansion design, and the conventions this
codebase holds itself to. The long forms live in `docs/` and `benchmark/README.md`.
