# Feature Spec — Serve LightGBM with SHAP Explanations

Status: implemented; see `update/LIGHTGBM_SHAP_UPDATES.md` for the
phase-by-phase log.

---

## The honest problem this fixes

Right now:
- The **live** pipeline scores with the **Isolation Forest** (PR-AUC 0.457).
  `detection/pulseguard_detection/score.py` loads only `isolation_forest.joblib`.
- **LightGBM** (PR-AUC 0.864) is trained and saved (`artifacts/lightgbm.txt`), but it's
  only an offline benchmark.
- "Top features" in the console are **z-scores** against the non-fraud population. The
  docstring says so: "not a SHAP value".

Your resume and skills mention LightGBM and SHAP, so an interviewer will assume LightGBM
runs live and explanations are SHAP. This spec makes that true. You end up with a
**champion/challenger setup** that you can defend in detail.

## Design

- **Champion (alerts)**: LightGBM probability, combined with the rules as today
  (`rules fired OR p >= threshold`).
- **Shadow**: the Isolation Forest keeps scoring every transaction. Its score is stored
  on the alert, but it doesn't trigger alerts. It's still useful for catching *new*
  fraud patterns no label covers yet, so the console shows it.
- **Explanations**: per-alert **TreeSHAP** values from LightGBM's built-in
  `predict(..., pred_contrib=True)`. This returns exact SHAP values for tree models
  with no extra dependency in the Spark image. Use the `shap` library only in the
  offline notebook/report, to cross-check that the values match.
- **Threshold**: recalibrate to analyst capacity using the existing
  `select_threshold_for_capacity`, now on LightGBM probabilities.

## Phase 1 — Training + artifacts (`detection/`)

1. In `train.py`, run the existing capacity-based threshold selection on **LightGBM
   holdout probabilities** (keep the IF numbers too, for comparison). Save:
   - `lightgbm.txt` (already saved)
   - `threshold.json`: `{ "threshold": <lgbm>, "iso_threshold": <iso>, "model": "lightgbm" }`
   - `metrics.json`: new rows for LightGBM-only and hybrid (rules OR LightGBM) precision,
     recall and alert rate **at the capacity threshold**, next to the existing IF rows
2. Re-run training and **record the new numbers**. Expect the hybrid precision to rise a
   lot from the current 0.040. Whatever the result, write it down honestly.
3. Address the leakage caveat the README already raises: LightGBM is high partly because
   the injected patterns are learnable. Keep the time-based split, and add per-pattern
   recall for LightGBM to the existing per-pattern table.

## Phase 2 — Live scoring (`score.py`, `alert_scoring.py`)

1. `DetectionArtifacts` gets `lgbm_model` (a `lightgbm.Booster` loaded from `lightgbm.txt`)
   and `iso_threshold`.
2. `score_transaction`:
   - `p = booster.predict(row)` → alert if rules fired or `p >= threshold`
   - `contrib = booster.predict(row, pred_contrib=True)`: one SHAP value per feature,
     plus a bias term in the last column
   - `topFeatures`: top 5 by |SHAP|, each `{ name, value, shap, direction: "up"|"down" }`
     ("up" means it pushed toward fraud)
   - also include `baseValue` (the bias) and `isoScore` (shadow)
   - `severity_for` uses `p` against the threshold. Keep the rule-severity logic.
3. Batch it: inside `score_partitions`, call `predict` and `pred_contrib` **once per
   pandas batch**, not per row. It's much faster.
4. Add `lightgbm` to the Spark image requirements (`streaming/Dockerfile`).
   **Check ARM/x86 wheels build** in the image.
5. Keep the old z-score function as `zscore_features()` and include its top 3 as
   `unusualFeatures`. Analysts like both views: "why the model flagged it" (SHAP) and
   "what's statistically odd" (z-score).

**Tests** (`detection/tests/test_score.py`):
- SHAP values + base value sum to the model's raw margin (log-odds) for a sample row.
  This is the defining SHAP property, so assert it.
- Missing values (`secondsSincePrevTxn = null` on a customer's first transaction) don't
  crash. That NaN bug happened before. LightGBM handles NaN natively, so pass NaN,
  **not 0.0**, and document that decision.
- Rules-only alerts still fire when `p` is low.

## Phase 3 — Alert service + console (`serving/`)

1. `kafka-alert-message.ts` / `alert.entity.ts`: new `TopFeature` shape
   `{ name, value, shap, direction }`, plus `baseValue`, `isoScore` and `unusualFeatures`.
   Keep reading old alerts that only have `zScore`, so existing rows don't break the
   console. Add a DB migration for the new columns.
2. Console alert detail page:
   - Replace `TopFeaturesBars` with a **SHAP waterfall-style bar chart**: base value →
     each feature's push (red toward fraud, blue away) → final score. Hand-roll it in the
     same style as the existing component.
   - A small "Model: LightGBM p=0.93 · Isolation Forest (shadow) 0.61" line
   - A collapsed "Statistically unusual" section showing the z-score features
3. Update the existing spec tests (`alerts.service.spec.ts`, `kafka-alert-message.spec.ts`).

## Phase 4 — Offline SHAP report (`detection/`)

`python -m pulseguard_detection.explain_report`:
- Uses the `shap` library (`TreeExplainer`) on the holdout set
- Saves a global feature-importance plot (mean |SHAP|) and a beeswarm plot to `artifacts/`
- Asserts that `shap`'s values match `pred_contrib` (they should, within float error)
- Add the plots to the README

## Phase 5 — Verify live + README

1. Rebuild the stack and stream a burst with fraud patterns. Confirm alerts arrive with
   SHAP features and the waterfall renders.
2. Re-check end-to-end latency at 20 tx/s and compare to the old numbers. Note any change.
3. README: update the detection section (champion/challenger, threshold, new metrics
   table, SHAP), regenerate the demo GIF if the console changed visibly, and update the
   architecture diagram.

---

## Acceptance checklist

- [x] LightGBM drives live alerts; Isolation Forest scores in shadow
- [x] Threshold recalibrated to analyst capacity; new metrics recorded in `metrics.json` and the README
- [x] SHAP additivity test passes; the NaN test passes
- [x] Console shows the SHAP waterfall, and old alerts still render
- [x] Offline SHAP report + plots generated
- [x] All existing tests still pass; latency re-measured

## Resume line after this

> "Served a LightGBM fraud model (PR-AUC 0.864) as champion with an Isolation Forest in
> shadow mode, with per-alert TreeSHAP explanations rendered as waterfall charts in the
> analyst console; recalibrated the alert threshold to analyst capacity, raising hybrid
> precision from 0.040 to X at Y% recall."

## Know these for interviews

- What a SHAP value means, and the additivity property (base + Σ SHAP = model output, in log-odds)
- Why TreeSHAP is exact and fast for trees
- Why keep the Isolation Forest in shadow (it catches unlabelled/novel patterns)
- Why the threshold comes from analyst capacity, not F1
