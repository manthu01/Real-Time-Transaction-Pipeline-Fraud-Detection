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

---

## Phase 2 — Live scoring (`score.py`, `alert_scoring.py`)

### What changed

**`detection/pulseguard_detection/score.py`** (rewritten around a batch scorer)
- `DetectionArtifacts` gains `lgbm_model` (a `lightgbm.Booster` loaded from
  `lightgbm.txt`) and `iso_threshold`. `load_artifacts` refuses to start if
  the booster's feature order doesn't match `feature_names.json`, because a
  mismatched column order would quietly produce wrong scores.
- New `score_batch(artifacts, feature_rows, raws)`: one `predict` call for
  probabilities over the whole batch. Then **only for rows that alert**, one
  `predict(pred_contrib=True)` call (TreeSHAP) and one Isolation Forest call.
  Most transactions never alert, and SHAP is the expensive step.
- `score_transaction` is now a one-row `score_batch`, so offline callers run
  exactly the same code path as the streaming job.
- Alert decision: `rules fired OR p >= threshold`, where `p` is LightGBM's
  probability. The Isolation Forest is shadow-only.
- Alert payload:
  - `score`: LightGBM p (6 decimal places, because the threshold is ~3e-5)
  - `topFeatures`: top 5 by |SHAP|, each `{name, value, shap, direction}`;
    `direction: "up"` = pushed toward fraud
  - `baseValue`: the SHAP bias term (log-odds)
  - `isoScore`: Isolation Forest shadow score
  - `unusualFeatures`: top 3 z-score features (the old explanation, kept
    as `zscore_features()`)
- `feature_matrix()` builds float64 rows with **NaN for missing values**
  (None or NaN both become NaN), matching how LightGBM was trained in Phase 1.
  The Isolation Forest still gets zeros because sklearn rejects NaN.
- `severity_for(fired_rules, probability)`: rule severity logic unchanged.
  Model-only alerts are HIGH when `p >= 0.5`, otherwise MEDIUM (see problem 1).

**`streaming/pulseguard_streaming/alert_scoring.py`**: `score_partitions`
hands each pandas batch to `score_batch` in a single call instead of looping
rows through `score_transaction`.

**`streaming/pulseguard_streaming/output_schema.py`**: `TOP_FEATURE_STRUCT`
is now `{name, value, shap, direction}`. New `UNUSUAL_FEATURE_STRUCT`
`{name, value, zScore}`. `ALERT_SCHEMA` gains `baseValue`, `isoScore` and
`unusualFeatures`.

**`streaming/Dockerfile`**: adds `lightgbm==4.7.0`, plus a build-time
`import lightgbm` check. **`detection/requirements.txt`**: `lightgbm` pinned
`==4.7.0` (was `>=4.5`) so the version that trains matches the version that
serves, the same policy already used for scikit-learn.

**`detection/tests/test_score.py`** (rewritten, 12 tests)
- **SHAP additivity**: base + Σ SHAP == raw margin (log-odds), and
  sigmoid(margin) == p, on a real LightGBM model trained in the test fixture.
- **NaN test**: `secondsSincePrevTxn = NaN` (and `None`) reaches the
  booster as NaN, not 0.0. Neither the decision path nor the explanation
  path crashes, and a NaN value serialises as `null`. The fixture's model
  treats first transactions as safe and fast ones as fraud, so the test
  also asserts `p(NaN) < 0.5 < p(0.0)`. That proves 0.0 would have been
  the wrong value, not just a different one.
- **Rules-only alert** still fires when p is far below the threshold.
- Batch scoring returns exactly the same results as row-by-row scoring.
- SHAP ranking, sign/direction, top-5 cap, bias column excluded.
- z-score ranking (old test kept), severity by probability.
- The fake Isolation Forest now rejects NaN the way sklearn does, so a NaN
  leaking into the shadow scorer fails a test instead of passing silently.

### Problems hit and how they were fixed

**1. `severity_for` stopped working once the threshold changed.**
The old rule was HIGH if `score >= 1.5 × threshold`. That made sense for
Isolation Forest scores around 0.54, but the LightGBM capacity threshold is
3.3e-5, so almost every model-only alert would have been HIGH. **Fix:**
model-only severity now uses the probability itself (HIGH at `p >= 0.5`).
That's only meaningful because Phase 1 removed `scale_pos_weight`, so p is
an actual probability again.

**2. Tiny SHAP values were labelled with a direction but displayed as 0.0.**
The new test caught it: a contribution like +0.00003 rounded to `shap: 0.0`
but still showed `direction: "up"`. **Fix:** contributions that round to
zero at 4 decimal places are dropped. They'd show up as zero-width bars in
the waterfall anyway.

**3. Model-only alerts all showed `score: 0.0`.** With a ~3e-5 threshold,
rounding p to 4 places would have shown 0.0 for every model-only alert near
it. **Fix:** 6 decimal places.

**4. The arm64 image failed to build under emulation.** The first Dockerfile
version ran `apt-get install libgomp1` (LightGBM's OpenMP runtime). On
arm64 under QEMU, that pulled in a `libc-bin` upgrade whose `ldconfig`
post-install step segfaulted (exit 139). It's a known QEMU problem: even a
plain `ldconfig -p` crashes under emulation, so it isn't an ARM problem as
such. Checked both base images: `libgomp1` 12.1.0 is **already installed**
on amd64 and arm64. **Fix:** dropped the apt step and added a build-time
`python3 -c "import lightgbm"` so a missing native library fails the build
instead of the first live micro-batch.

### Verification

**Wheels:** `lightgbm==4.7.0` publishes `manylinux_2_28_x86_64` and
`manylinux2014_aarch64` wheels. The base image `apache/spark-py:v3.4.0` is
Ubuntu 22.04 (glibc 2.35, Python 3.10.6) and is published for amd64 and
arm64.

**Image builds**, both architectures built and scored the same transaction
inside the container against the real artifacts:

| Arch | Build | `import lightgbm` | p | baseValue | top SHAP |
|---|---|---|---|---|---|
| x86_64 (native) | ok | 4.7.0 | 1.0 | -10.4243 | txnCount1Min +28.34 |
| aarch64 (QEMU) | ok | 4.7.0 | 1.0 | -10.4243 | txnCount1Min +28.34 |

**Real artifacts on the full holdout (46,990 txns), through the actual
`score_partitions` adapter in 500-row batches, without Spark:**
- Alert rate **3.524%**, which matches train.py's LightGBM hybrid alert rate
  (3.524%) exactly, so live and offline scoring agree.
- Fraud caught **60 / 62** (recall 0.968, same as Phase 1).
- SHAP additivity over every holdout row: max |base + Σ SHAP − margin| =
  **3.2e-14**.
- 35 NaN cells in the holdout matrix scored without error.
- Speed: **158 µs/txn** on the batch path vs **386 µs/txn** row-by-row
  (~2.4x). What's left in the batch path is mostly per-row rule evaluation
  and building the matrix in Python.
- Severity mix: 1,111 MEDIUM, 534 HIGH, 11 CRITICAL.

Sample alert (rule `AMOUNT_SPIKE` fired; p = 0.0007, below the 0.5 HIGH line):
```
score 0.000704  baseValue -10.4243  isoScore 0.5878
SHAP: txnSum5Min +1.60 up, distanceFromPrevKm -0.97 down,
      distanceFromModalCityKm +0.58 up, txnSum1Hour +0.53 up,
      distinctDevices7Day +0.51 up
z:    amountZScore 3.46, distinctDevices24Hour 2.95, distinctDevices7Day 2.89
```

Tests: detection 40 passed, streaming 21 passed, analytics 24 passed.

### Known gap until Phase 3
The Kafka alert message now carries `topFeatures` entries with `shap` /
`direction` instead of `zScore`. The current alert service and console still
expect `zScore` (`TopFeaturesBars` calls `f.zScore.toFixed`), so **don't
rebuild the live stack between this commit and Phase 3**. Phase 3 adds the
new shape and keeps old `zScore` alerts readable.
