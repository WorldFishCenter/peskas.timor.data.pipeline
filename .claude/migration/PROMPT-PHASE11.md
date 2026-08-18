# Phase 11 — split into two prompts

**This file is a pointer. Do not send it.**

The original single Phase 11 prompt bundled two kinds of work whose risk profiles
are completely different, and that made it impossible to tell where the
production line was. It opened with "this is the only phase that changes
production", which was true of the phase and false of ~90% of its content.

It is now two prompts, sent in order:

| prompt | what | touches production |
|---|---|---|
| **`PROMPT-PHASE11A.md`** | cut ~1,000 lines of dead code, delete four Sheets tables and the legacy config keys, delegate the KoBo trio and the assets hub-mirror to `coasts::`, prove it green on `-dev` with **zero portal change** | **no** — branch pushes resolve `-dev` everywhere |
| **`PROMPT-PHASE11B.md`** | the v1 freeze against production, merge to `main`, watch one production run, delete the 45 leaked objects, re-enable the three disabled workflows | **yes** |

Send `PROMPT-PORTAL-CORRECTIONS.md` first, then 11a, then 11b.

Everything the original file contained is in one of the two, with three
instructions corrected against the 2026-08-18 alignment audit — most importantly
the `timor_assets()` → `country` swap, which was listed as verified-equivalent
and would have silently emptied the `sites` and `geo` label joins. See
`ALIGNMENT-AUDIT.md` §2 and §13.
