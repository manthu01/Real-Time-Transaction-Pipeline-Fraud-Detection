# LightGBM + SHAP — Change Log

Running log of the work done against `LIGHTGBM_SHAP_SPEC.md`: what changed, the
problems hit along the way, how each was fixed, and the measured numbers. Every
number here comes from an actual run, including the ones that didn't go the way
the spec expected.

---

## Phase 1 — Training + artifacts (`detection/`)

### What changed

**`detection/pulseguard_detection/train.py`**
- Capacity-based threshold selection (`select_threshold_for_capacity`) now runs
  **per model**: once on LightGBM holdout probabilities (the new champion) and
  once on Isolation Forest scores (kept for comparison and for shadow scoring).
- New `operating_point()` helper: for one model at one threshold, returns
  model-only and hybrid (`rules OR score >= threshold`) precision / recall /
  alert rate, plus the false-positive rate at matched recall. Used for both
  models so the comparison is computed the same way.
- `detection_rate_per_pattern()` generalized from two fixed columns to a
  `{detector_name: flags}` dict. The per-pattern table now has
  `rules_only`, `isolation_forest_hybrid`, `lightgbm_only` and
  `lightgbm_hybrid` recall for every injected fraud pattern.
- New `lightgbm_feature_matrix()`: the feature matrix as float64 with missing
  values kept as **NaN** (no `fillna(0.0)`) and booleans as 0/1.
- `scale_pos_weight` removed from LightGBM (see problem 2 below).
- Trade-off curve CSV now has a `model` column; the PNG plots both curves.
- A warning prints if a model's actual alert rate ends up more than 2x its
  capacity target (the failure in problem 2 would now be loud, not silent).

**`detection/artifacts/threshold.json`** — new shape:
```json
{
  "model": "lightgbm",
  "threshold": 3.30e-05,
  "iso_threshold": 0.5403,
  "target_alerts_per_day": 200.0,
  "target_alert_rate": 0.02213,
  "actual_alert_rate": 0.02560,
  "iso_actual_alert_rate": 0.02215
}
```

**`detection/artifacts/metrics.json`** — new keys: `champion_model`,
`threshold_selection.{lightgbm,isolation_forest}`, `at_capacity_threshold`
(IF-only, IF-hybrid, LightGBM-only, LightGBM-hybrid),
`false_positive_rate_at_matched_recall.{lightgbm,isolation_forest}`, and the
wider per-pattern table. `isolation_forest.pr_auc` is unchanged because the
nightly analytics retrain check reads it.

**`detection/tests/test_train_metrics.py`** — per-pattern test updated to the
new signature; new `test_operating_point_hybrid_adds_rule_only_catches`.

### Problems hit and how they were fixed

**1. Training/serving skew on missing values.**
LightGBM was trained on `fillna(0.0)`, but the spec (Phase 2) requires the live
scorer to pass NaN. Training on 0 and serving NaN means the model sees inputs at
serving time it never saw in training. Worse, 0.0 is the wrong value:
`secondsSincePrevTxn = 0` on a customer's first-ever transaction says "zero
seconds since the last transaction", which is exactly what card testing looks
like. **Fix:** LightGBM now trains on NaN (it learns a default split direction
for missing values), and serving will pass NaN too. The Isolation Forest still
uses `fillna(0.0)` because sklearn's IF can't take NaN.

**2. The capacity threshold flagged 100% of transactions.**
First retrain: LightGBM's capacity threshold came out at 0.0017, with an actual
alert rate of **100%**. Digging in, the saved model was a **single tree**, and
on the holdout it produced only 7 distinct probabilities: ~1.0 for 274 rows and
0.0017 for the other 46,716. A quantile over 7 values can only land on "almost
nothing" or "everything", and it landed on everything.

Root cause: `scale_pos_weight = n_neg/n_pos ≈ 560`. The first heavily-weighted
tree pushed its fraud leaves straight to p≈1.0, and early stopping on
validation PR-AUC then chose that one round. The old docstring had described
this as "the patterns are so separable one round is enough", but it was
really caused by the reweighting.

Configs compared **on the validation split only** (tail of train, 58 fraud),
so the holdout stayed untouched:

| Config | Best iter | Val PR-AUC | Distinct scores |
|---|---|---|---|
| scale_pos_weight = full (old) | 1 | 0.979 | 7 |
| scale_pos_weight = sqrt | 1 | 0.993 | 10 |
| **no scale_pos_weight** | **83** | **0.990** | **809** |
| no spw, lr=0.1 | 1 | 0.952 | 9 |
| no spw, leaves=15, min_child=50 | 65 | 1.000 | 1205 |

Validation PR-AUC is saturated (0.95–1.0 on 58 positives), so it can't rank
these. The decision was structural: drop `scale_pos_weight` and change nothing
else. PR-AUC only depends on how scores are ranked, so reweighting added no
ranking quality. What it did was break the capacity threshold and make `p`
meaningless as a probability shown to analysts. The `leaves=15` variant was
**not** chosen: its 1.0 is noise on 58 positives, and picking it would be
tuning towards a lucky number.

**3. Isolation Forest pickle version.** The local environment has scikit-learn
1.7.2 while the Spark image pins 1.5.2. Checked the existing
`isolation_forest.joblib`: it was already pickled with 1.7.2, so retraining
doesn't change the current situation. Still an open item (see below).

### Results (holdout: 46,990 txns, 62 fraud, ~5.2 days, ~9,036 txns/day)

| | PR-AUC | Model-only P / R | Hybrid P / R | Hybrid alert rate |
|---|---|---|---|---|
| Isolation Forest | 0.457 | 0.055 / 0.919 | 0.040 / 0.919 | 3.02% |
| LightGBM (champion) | **0.906** (was 0.864) | 0.050 / **0.968** | **0.036** / **0.968** | 3.52% |

LightGBM early stopping: **83 rounds** (was 1).

False-positive rate at matched recall — LightGBM: model-only 1.20% vs hybrid
3.40%; Isolation Forest: 1.46% vs 2.90%.

Per pattern (recall):

| Pattern | n | Rules only | IF hybrid | LightGBM only | LightGBM hybrid |
|---|---|---|---|---|---|
| ACCOUNT_TAKEOVER | 2 | 1.00 | 1.00 | 1.00 | 1.00 |
| CARD_TESTING | 31 | 0.94 | 1.00 | 1.00 | 1.00 |
| GEO_IMPOSSIBLE_TRAVEL | 4 | 0.50 | 0.50 | **1.00** | **1.00** |
| MERCHANT_CATEGORY_ANOMALY | 2 | 1.00 | 1.00 | 1.00 | 1.00 |
| MULE_ACCOUNT | 16 | 0.88 | 1.00 | 1.00 | 1.00 |
| SALAMI_SLICING | 7 | 0.29 | 0.57 | **0.71** | **0.71** |

Leakage caveat (still true): the injected patterns are learnable by
construction, so LightGBM's numbers are an upper bound on what a real dataset
would give. The time-based split is kept, and the per-pattern table shows
where it's strong (5 of 6 patterns at 100%) and where it isn't (salami
slicing at 71%). Several patterns have only 2–4 holdout examples, so
those 100%s are thin evidence.

### Finding: hybrid precision did NOT rise (spec expected it to)

The spec expected hybrid precision to "rise a lot" from 0.040. It went to
**0.036**. This is a property of the operating point, not a model failure:

- The holdout has **~12 frauds/day**; the default capacity is 5 analysts × 40
  = **200 alerts/day**. Even a perfect model's precision is capped at ~6%
  when you fill 200 slots with 12 frauds.
- To fill those 200 slots, the LightGBM threshold falls to **3.3e-5**, deep
  into transactions the model is confident are fine.
- The rules alone add ~120 alerts/day at 8% precision, which caps hybrid
  precision further.

At smaller budgets, LightGBM's advantage is clear:

| Alerts/day | LightGBM-only P / R | IF-only P / R | LightGBM hybrid P / R |
|---|---|---|---|
| 15 | **0.71 / 0.90** | 0.52 / 0.66 | 0.09 / 0.94 |
| 25 | 0.44 / 0.92 | 0.37 / 0.77 | 0.09 / 0.94 |
| 50 | 0.22 / 0.92 | 0.20 / 0.84 | 0.07 / 0.94 |
| 100 | 0.10 / 0.92 | 0.11 / 0.89 | 0.06 / 0.94 |
| 200 | 0.05 / 0.97 | 0.06 / 0.92 | 0.04 / 0.97 |

Decision: the 200/day default is **kept** for now so the comparison with the
earlier IF numbers stays like-for-like. The useful takeaway is that capacity
is over-provisioned relative to fraud volume. In practice that means either
fewer reviewers or a higher threshold, and the table above shows what each
budget would buy.

### Open items carried into later phases
- `severity_for` escalates to HIGH at `score >= 1.5 × threshold`. With a
  LightGBM threshold of 3.3e-5 that rule becomes meaningless → Phase 2.
- `analytics/pulseguard_analytics/retrain_check.py` still compares an IF
  challenger against `metrics.isolation_forest.pr_auc` and calls it the
  "champion". The comparison still works, but the label is now wrong
  (LightGBM is the champion). Out of the spec's scope; left as-is.
- scikit-learn version mismatch (1.7.2 local vs 1.5.2 in the image) for the
  IF pickle — pre-existing.
- LightGBM 4.7 prints a deprecation warning for `eval_set=` in `fit()`;
  harmless, left alone.

Tests: detection 36 passed, analytics 24 passed.
