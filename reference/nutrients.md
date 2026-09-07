# Nutrient composition and RDI — Timor's own, deliberately

Renamed from `R/calculate-nutrients.R` in migration Phase 8. The plan's
§4b test was "delete Timor's if it agrees with
[`coasts::enrich_taxa()`](https://rdrr.io/pkg/coasts/man/enrich_taxa.html)",
and it fails on four counts, measured in Phase 4 and restated in
`.claude/migration/COASTS-4.6.0.md`: coasts emits **six** nutrients
against Timor's seven, has **no selenium**, does **no unit conversion**
(Timor's `convert = TRUE` normalises mg/μg/g per 100 g to grams), and
carries **no FAO food-composition override** for the six invertebrates
FishBase cannot estimate (`OCZ`, `IAX`, `COZ`, `PEZ`, `CRA`, `SLV`) or
the hardcoded `FLY` row. Nothing below changed with the rename;
upstreaming it is Phase 10.
