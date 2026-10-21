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
  *(Resolved in Phase 2.)*
- `analytics/pulseguard_analytics/retrain_check.py` still compares an IF
  challenger against `metrics.isolation_forest.pr_auc` and calls it the
  "champion". The comparison still works, but the label is now wrong
  (LightGBM is the champion). Out of the spec's scope; left as-is.
  *(Resolved in the follow-up at the end: it now checks both models.)*
- scikit-learn version mismatch (1.7.2 local vs 1.5.2 in the image) for the
  IF pickle — pre-existing. *(Resolved at the end of the follow-up, with
  identical metrics.)*
- LightGBM 4.7 prints a deprecation warning for `eval_set=` in `fit()`;
  harmless, left alone. *(Resolved in the follow-up.)*

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
new shape and keeps old `zScore` alerts readable. *(Resolved in Phase 3.)*

---

## Phase 3 — Alert service + console (`serving/`)

### What changed

**Scorer payload (small addition to Phase 2)**: `rawScore`, the model's
log-odds output (= baseValue + every feature's SHAP, not just the top 5).
Added to `score.py`, `ALERT_SCHEMA` and the Spark adapter. See problem 1.

**Alert service (`serving/alert-service`)**
- `alert.entity.ts`: `TopFeature` is now `ShapFeature | ZScoreFeature`:
  - `ShapFeature` = `{name, value: number|null, shap, direction: "up"|"down"}`
  - `ZScoreFeature` = the old `{name, value, zScore}`

  Plus an `isShapFeature()` guard. New nullable columns: `baseValue`,
  `rawScore`, `isoScore`, `unusualFeatures` (jsonb).
- `kafka-alert-message.ts`: the new fields are **optional** on the wire
  contract. `fraud.alerts` keeps 30 days of messages, so a consumer replaying
  the topic still meets the old z-score-only shape. Validation got stricter
  too: if a new field is present it must be the right type, and every
  `topFeatures` entry must be an object with a `name`.
- `alerts.service.ts`: persists the new fields, `null` when absent.
- **Migrations replace `synchronize: true`** (`src/migrations/`):
  - `BaselineAlertsSchema1789516800000`: the exact schema `synchronize`
    created, including TypeORM's generated PK and index names, all
    `IF NOT EXISTS`. It's a no-op on existing databases and builds the
    identical schema on fresh ones.
  - `AddShapExplanation1792454400000`: adds the 4 nullable columns. No
    back-fill: old rows have no SHAP values to recover, and `null` is how
    the console recognises them.
  - `app.module.ts`: `synchronize: false`, `migrationsRun: true`. Migrations
    are listed explicitly, not globbed, so the same list works in ts-node,
    jest and the compiled build.

**Analyst console (`serving/analyst-console`)**
- New `ShapWaterfall.tsx` (hand-rolled, same grid style as the old bars):
  base value → each top feature's push (**red toward fraud, blue away**) →
  an "all other features" remainder (grey) → model output, all on one shared
  log-odds axis. A dashed line marks log-odds 0 (p = 0.5) when the axis
  crosses it, and the caption reads e.g. "output 20.39 → p = 1.000". The
  remainder step means the chart always adds up exactly: SHAP additivity,
  shown visually.
- Model line: "Model: **LightGBM p=1.000** · Isolation Forest (shadow) 0.59".
- Collapsed "Statistically unusual (z-score vs non-fraud customers)"
  `<details>` section shows `unusualFeatures`.
- `TopFeaturesBars.tsx` renamed to `ZScoreBars.tsx`. It's now used for the
  collapsed section and as the main view on legacy alerts.
- New `lib/alertModel.ts`: `isLegacyAlert` (= `baseValue == null`),
  feature-shape guards, `modelOutputLogOdds`, and number formatting.
- **Legacy alerts** (pre-LightGBM) render the old way: "Anomaly score",
  "Model: Isolation Forest 0.62 · scored before the LightGBM champion, so no
  SHAP explanation", and z-score bars.
- Live feed: the score column uses `formatAlertScore` (see problem 4) and
  tags legacy rows with "IF" (see problem 6).

**Tests**
- `alerts.service.spec.ts`: SHAP payload persisted (topFeatures, baseValue,
  rawScore, isoScore, unusualFeatures); a pre-LightGBM message still
  ingests, with nulls.
- `kafka-alert-message.spec.ts`: accepts the SHAP message and the legacy
  message; rejects wrongly-typed baseValue/rawScore/isoScore, non-object
  topFeatures entries, and a non-array unusualFeatures.
- `test_score.py`: `rawScore` matches the booster's raw margin; new
  severity test (problem 5).

### Problems hit and how they were fixed

**1. The waterfall couldn't end at the right number.** The payload only has
the top 5 SHAP values plus the base, and `score` is p rounded to 6 places.
Getting the end point from logit(score) breaks for real fraud, where p
rounds to 1.0 and the logit is infinite. One real alert had log-odds 20.39.
**Fix:** the scorer also sends `rawScore` (the log-odds output). The
console draws the gap between it and base + top 5 as "all other features",
so the chart adds up exactly. For a row written without `rawScore`, the
console falls back to a clamped logit(score).

**2. A naive migration would have duplicated every index.** The service
had been running with `synchronize: true`, so real databases already have
the table with TypeORM's hash-named indexes (`IDX_8cecd75b…`). A baseline
migration with hand-picked index names plus `IF NOT EXISTS` would have
quietly added a second copy of each index. **Fix:** the baseline DDL was
captured from TypeORM itself (`createSchemaBuilder().log()` against an empty
database using the pre-change entity), so the names match exactly.

**3. A one-off jest warning.** The first test run printed "a worker
process has failed to exit gracefully". Ran the original (pre-change) suite
for comparison, then the new suite twice more: no warning, 31/31 both
times. It was a slow first run, not a leak.

**4. Tiny probabilities showed as "0.000".** The feed used
`score.toFixed(3)`, and LightGBM probabilities near the threshold are
~2e-5. **Fix:** `formatProbability` switches to exponent form below 0.001
(`2.3e-5`, `7.0e-4`).

**5. A p = 1.0 fraud was rated MEDIUM (found on real alerts).** Publishing
real holdout alerts through Kafka showed an alert with **p = 1.000** that
also fired `AMOUNT_SPIKE` (a MEDIUM rule) ending up **MEDIUM**, while a
model-only p = 0.99 alert was HIGH. Any fired rule's severity replaced the
model's. **Fix:** severity is now the highest of the fired rules'
severities and HIGH when p ≥ 0.5. A rule can raise severity but can't pull
a confident model's alert down. Rules still set severity on their own when
the model isn't confident (an `AMOUNT_SPIKE` alert at p = 0.0007 stays
MEDIUM). On the holdout, 4 alerts moved from MEDIUM to HIGH and nothing
else changed.

**6. Two things you'd only notice by looking at the screen.**
- The z-score bars were blue, and directly above them in the waterfall,
  blue means "pushes away from fraud". `txnCount1Min` at z = +6 drawn
  blue reads as exculpatory. **Fix:** z-score bars are now neutral grey with
  the sign in the text (`z=+5.98`), and values are rounded like the
  waterfall's.
- The feed's single SCORE column mixed LightGBM probabilities with legacy
  Isolation Forest scores (a legacy `0.619` right below a `p = 0.365`), and
  those aren't comparable. **Fix:** legacy rows show `0.619 IF`, with a
  tooltip saying what the number is.

### Verification

**Migrations against real Postgres 16:**

| Database | How it was made | Migrations run | Remaining diff vs new entity | Rows kept | Indexes |
|---|---|---|---|---|---|
| test "legacy" | old entity + `synchronize`, 1 z-score alert | both | **0 statements** | 1 / 1 | 6 (PK + 5) |
| test "fresh" | empty | both | **0 statements** | — | 6 |
| test "legacy", second run | — | none (no-op) | 0 | 1 | 6 |
| **real dev DB** (`pulseguard_postgres_data`) | created by the earlier phases | both, at service startup | — | **107,592 / 107,592** | 6 |

"Remaining diff" is TypeORM's own `createSchemaBuilder().log()`, meaning
the SQL `synchronize` would still want to run. Zero means the migrations
produce exactly the entity's schema. The real dev DB was backed up with
`pg_dump` (44 MB) before the service touched it.

**End to end, through real Kafka:** 8 alerts generated by the real scorer
from holdout transactions (model-only fraud, rules + model, rules-only at
low p, and one with a missing feature) were published to `fraud.alerts`.
The new alert service consumed, validated and stored all 8 with their SHAP
fields. Then the console in Chrome, checking each case:
- p = 1.000 fraud: base −10.42 → `secondsSincePrevTxn = 0` +28.99, and four
  smaller red pushes → remainder −0.06 → output 20.39. Adds up exactly.
- p = 7.0e-4 rules-only alert: a blue −0.97 bar (`distanceFromPrevKm`), a
  grey remainder, and the p = 0.5 line correctly left out because the axis
  doesn't reach it.
- The "Statistically unusual" section expands and shows the z-score features.
- A legacy alert from the 107k old rows: "Anomaly score", the Isolation
  Forest model line, z-score bars, no crash.
- Live feed: new and legacy rows side by side, formatted correctly.

The 8 test alerts were deleted afterwards. The dev DB keeps its 107,592
original rows and is now on the new schema version. Containers were stopped
again.

Builds: alert-service `tsc` and `nest build` OK; console `tsc` and
`next build` OK. Tests: alert-service 31 passed (was 22), detection 41,
streaming 21, analytics 24.

### Notes
- Throughput measured during this phase (511 µs/txn) is not comparable with
  Phase 2's 158 µs/txn: Kafka, Postgres, the Node service and a Next dev
  server were all running on the same machine. Latency is re-measured
  properly in Phase 5.
- The waterfall's axis fits the data range, so on a low-p alert (axis −10.7
  to −7.0) small pushes look large. The caption always prints the axis
  range, so the scale is never hidden.

---

## Phase 4 — Offline SHAP report (`detection/`)

### What changed

**New `detection/pulseguard_detection/explain_report.py`**
(`python -m pulseguard_detection.explain_report --input-jsonl data/training_raw.jsonl`)
- Rebuilds the holdout with the **same time split** as `train.py`, so it's
  exactly the data the metrics come from and the model never trained on.
- **Cross-check:** compares SHAP values from an *independent* TreeSHAP
  implementation against LightGBM's `pred_contrib` (what the live scorer
  ships) over every holdout row, per feature and for the base value, plus
  additivity for both. It exits non-zero if they differ by more than 1e-6.
- Writes to `artifacts/`:
  - `shap_importance.png`: mean |SHAP| for the top 15 features, on fraud
    rows vs all holdout rows
  - `shap_beeswarm.png`: all 62 holdout fraud rows plus 4,938 sampled
    others
  - `shap_report.json`: cross-check numbers, the full importance tables,
    and the sample sizes

**New `detection/tests/test_explain_report.py`** (3 tests): a small model
trained so its splits use **both** LightGBM missing-value modes; independent
TreeSHAP == `pred_contrib` within 1e-9 including NaN rows; and a
non-circularity test (see problem 1).

**`detection/requirements.txt`**: `shap==0.52.0`, offline only and never in
the Spark image.

**`README.md`**: the outdated "Explainability, mechanically" paragraph
(which still described z-scores) is replaced with the SHAP version: how the
live explanation works, how the report checks it, both plots, and what they
show. Only that section was committed. The rest of the detection section
is Phase 5's rewrite. The other uncommitted README edits already in the
working tree were left unstaged.

### Problems hit and how they were fixed

**1. The first cross-check was circular, and it "passed" with a perfect 0.0.**
The first version did the obvious thing: `shap.TreeExplainer(booster)` vs
`booster.predict(pred_contrib=True)`. The difference was **exactly 0.0** on
all 46,990 rows. Two separate float64 TreeSHAP implementations don't agree
to the last bit, so that was a reason to look closer. Reading shap 0.52's
source: for LightGBM models, `TreeExplainer.shap_values` **calls
`booster.predict(..., pred_contrib=True)` itself**. It was checking LightGBM
against LightGBM.

**Fix:** pass shap a model *dictionary* instead of the `Booster`. Then shap
runs its own C++ TreeSHAP and never calls LightGBM. `booster_as_shap_model()`
parses `booster.dump_model()` into shap's generic tree format (flat
children/feature/threshold/value/sample-weight arrays per tree).

The tricky part is **missing values**. shap sends NaN to a node's
`children_default`; LightGBM decides per split by `missing_type`:
- `NaN`: NaN follows the learned `default_left`
- `None`: that split never saw a missing value in training, so LightGBM
  turns NaN into 0.0 and compares. The default is wherever 0.0 goes.
- `Zero` (0.0 *and* NaN both count as missing): has no equivalent in
  shap's format, so the converter **refuses** it rather than translating it
  wrong. (This model has no such splits.)

Categorical splits are refused the same way. Proof the new check is
independent: `test_check_is_not_circular` changes one leaf value in the
parsed trees and asserts the comparison then **fails**. If the values still
came from LightGBM, editing the parsed trees couldn't change them.

Result on the real model: max difference **7.1e-15**. That's float rounding
between two genuinely different implementations, not an identical-code 0.0.

**2. A plain random sample would have made the beeswarm useless.** At 0.13%
fraud, a 5,000-row random sample has ~7 fraud dots. **Fix:** the beeswarm
uses every holdout fraud row (62) plus a random sample of the rest, and
says so in its title. Importances are always computed on every row.

### Results

Cross-check, all 46,990 holdout rows (35 with missing values):

| Check | Max abs error |
|---|---|
| independent TreeSHAP vs `pred_contrib`, per-feature SHAP | **7.1e-15** |
| ... base value | 0.0 |
| additivity, `pred_contrib` (base + Σ SHAP vs margin) | 3.2e-14 |
| additivity, independent TreeSHAP | 2.5e-14 |

Base value: −10.4243 log-odds. Note this is the model's *average log-odds
output* over the training data, not the log-odds of the fraud rate (0.18% ≈
−6.3). Averaging in log-odds space is dominated by the ~99.8% of
transactions the model is very sure are normal, so it sits far below
logit(mean p). That's why the waterfall starts so far left: e.g. sigmoid(−10.42)
≈ 3e-5, not 0.18%.

Top features by mean |SHAP| on **fraud rows** (vs all rows):

| Feature | Fraud rows | All rows |
|---|---|---|
| txnCount1Min | 13.18 | 0.041 |
| distanceFromPrevKm | 5.23 | 0.029 |
| secondsSincePrevTxn | 4.01 | 0.035 |
| txnSum5Min | 3.76 | 0.050 |
| maxTxnsInAny60SecWindowToday | 1.03 | 0.016 |
| amountToTrailingMaxRatio | 0.96 | 0.056 |
| amountZScore | 0.44 | 0.017 |
| txnSum1Hour | 0.32 | 0.011 |

### What the plots revealed (written into the README)
- The model is mostly about **burst velocity** (`txnCount1Min`,
  `secondsSincePrevTxn`, `txnSum5Min`): card testing and mule fan-in, which
  are 47 of the 62 holdout fraud rows.
- **Low `distanceFromPrevKm` pushes hard toward fraud** (up to +12
  log-odds). Being where you last transacted isn't a fraud signal by itself;
  the model has learned that the generator fires card-testing bursts from a
  single location. This is the Phase 1 leakage caveat made visible: part of
  the 0.906 PR-AUC is the model learning the simulator's fingerprints. It's
  also exactly the kind of explanation a real analyst would challenge, which
  is an argument for shipping SHAP to the console.
- Geo features barely register, which fits there being 4 impossible-travel
  and 2 merchant-anomaly fraud rows in the holdout. The rules still carry
  those patterns.

Tests: detection 44 passed (41 + 3 new). Report runtime ~4 min, nearly all
of it rebuilding the 188k-row feature dataset.

---

## Phase 5 — Verify live + README

### How the stack was run
- **From a clean checkout of the pushed `main`** (a separate git
  worktree), not from the working folder. The working folder had
  uncommitted, unrelated streaming changes (a late-events side query and a
  `foreachBatch` bronze writer) that would otherwise have been baked into
  the Spark image and would have skewed the latency comparison. Same compose
  project name, so the same Kafka/Postgres/checkpoint volumes were used.
- The gitignored model binaries were copied into the worktree.
- **The console was published on port 3006** through a local-only compose
  override, because another app on this machine already holds port 3000.
  Nothing about that is committed.
- **Checked before starting Spark:** its checkpoint was at offset 112,196
  on every query, equal to the end of `raw.transactions`, so there was no
  backlog to replay into the measurements.

### 1. Live verification (fraud-heavy burst)
90 s at 20 tx/s with a 5% fraud-burst rate (1,801 transactions):
- **495 alerts; all 495 carry SHAP `topFeatures`, `baseValue` and
  `rawScore`**; 262 model-only, 233 with rules (all 6 rules fired at least
  once).
- The new alert service consumed them all, with migrations already
  applied.
- The console (built from the Docker image, not dev mode) rendered a live
  model-only card-testing alert as a waterfall: base −10.42 →
  `txnCount1Min = 4` +28.55 → `distanceFromPrevKm = 0` +5.15 → … → 24.99,
  p = 1.000, Isolation Forest shadow 0.58. Saved as `docs/shap-waterfall.jpg`.

### 2. Latency: a real 3x regression, found and fixed

The first clean LightGBM run looked bad, so both images were run back to
back as a fair A/B instead of comparing against the README's weeks-old
number. Each run: wait until the alerts-query checkpoint equals the topic's
end offset, then 180 s of seeded load-test traffic at 20 tx/s, with a
dedicated probe consumer (`latest` offsets) recording alert publish time
minus transaction time.

| Spark image | p50 | p95 | p99 | max | falling-behind batches (median) |
|---|---|---|---|---|---|
| Isolation Forest, pre-LightGBM `65f593e` | 12.2 s | 17.5 s | 18.9 s | 20.2 s | 9.1 s |
| LightGBM + SHAP, default threading | 33.6 s | 57.7 s | 64.6 s | 68.1 s | 22.8 s |
| LightGBM + SHAP, `OMP_NUM_THREADS=1` (env var only) | 14.3 s | 21.0 s | 22.7 s | 25.0 s | 10.1 s |
| LightGBM + SHAP, `PREDICT_THREADS = 1` in code (**shipped**) | **13.9 s** | 21.2 s | 23.2 s | 25.7 s | 9.3 s |

The Isolation Forest baseline (12.2 s) reproduces the README's original
10.6 s reasonably, so the setup is comparable.

**Problem: why was LightGBM 3x slower?** It couldn't be the model's actual
work: offline, the whole scoring path costs ~160 µs per transaction, under
a second for the entire run, and *every* query's batches slowed down, not
just the alerts query. Hypothesis: **OpenMP thread oversubscription**.
LightGBM's `predict` defaults to one OpenMP thread per core, and idle OpenMP
threads spin. The container sees all 12 host cores, so every Python worker
that scored a batch started a 12-thread spinning team alongside the JVM and
the other queries' workers, all on a `local[2]` budget.

**Test:** reran the identical image with `OMP_NUM_THREADS=1` and no code
change. p50 went from 33.6 s to 14.3 s, so the hypothesis was confirmed.

**Fix:** `score.py` passes `num_threads=PREDICT_THREADS` (= 1) to both
`predict` calls, with the measurement in a comment. It's done in code
rather than as a compose env var so it holds wherever the scorer runs. The
image was rebuilt *with no env var* and re-measured: p50 13.9 s. Batches
are a few hundred rows, too small for intra-batch threading to pay off, and
Spark already parallelises across partitions.

What's left: ~1.7 s at p50 over the Isolation Forest, in a run that
produced ~2.9x as many alerts, each paying for TreeSHAP and a Kafka write.

### 3. Alert volume on load-test traffic (open problem, documented)

The LightGBM runs alerted on a far larger share of load-test traffic than
the Isolation Forest run (one run: 2,983 alerts from 3,601 transactions).
Two things had to be separated:

**a) Is the live scoring wrong?** The exact feature rows Spark wrote to
bronze Delta were rescored offline with the same code (a one-off
spark-submit inside the container). **3,008 of 3,009 live alert scores
matched exactly.** The single mismatch also had a mismatched Isolation
Forest score, so its *feature row* differed between the bronze query and
the alerts query (each recomputes features in its own state store: the
documented three-consumers design), not a scoring bug.

**b) What does each detector do on identical rows?** Same 8,249 bronze rows:

| Detector at its capacity threshold | Load-test rows flagged | Calibrated holdout rate |
|---|---|---|
| Rules | 9.3% | 1.33% |
| Isolation Forest | 35.2% | 2.22% |
| LightGBM (200/day, p ≥ 3.3e-5) | **79.9%** | 2.56% |
| LightGBM at 50/day (p ≥ 9.8e-5) | 36.9% | 0.55% |
| LightGBM at 15/day (p ≥ 0.011) | 13.5% | 0.17% |

**A correction to my own first read:** I initially blamed the low 200/day
threshold, which sits just above the model's base rate (3.0e-5). But every
capacity threshold inflated by roughly 35-80x, so the main cause is that
the load-test firehose is far out of distribution (median gap between
transactions 315 s vs 1,840 s in training), and a supervised model
extrapolates much more aggressively than the unsupervised one. No threshold
fixes that. The fix belongs in monitoring: the PSI drift job already flags
this traffic, and it should drive an alert-volume circuit breaker. Written
into the README as the first thing to add; **not built**.

Also noted: rule counts varied a lot between runs (e.g. `IMPOSSIBLE_TRAVEL`
673 vs ~190) because I replayed the same seeded traffic several times and
each customer's rolling state kept accumulating. Alert *counts* across
runs aren't comparable for that reason; the identical-rows table above is
the fair comparison.

### 4. Demo GIF
First attempt: recording started (live feed with alerts arriving, the
drill-down, the expanded z-score section), then Chrome screenshots started
timing out intermittently on the detail page while the map iframe rendered,
and the browser extension disconnected. That commit kept the old GIF,
marked as predating SHAP. **Re-recorded in the follow-up below.**

### 5. README
- Intro, Status (Phase 3 bullet, with a link to this log), architecture
  diagram (LightGBM champion, TreeSHAP per alert, Isolation Forest shadow,
  SHAP waterfall in the console), repository layout (`explain_report.py`,
  migrations, test counts 44 / 31), and getting-started text (both
  thresholds, the SHAP report, the new drill-down).
- **Detection section rewritten:** champion/shadow design and severity
  rule; a side-by-side metrics table (LightGBM vs Isolation Forest at the
  capacity threshold); the per-pattern table with LightGBM columns; and
  honest notes (precision went *down*, and why; the one-tree
  `scale_pos_weight` model; NaN handling; leakage).
- **New:** the identical-rows alert-volume table and the circuit-breaker
  recommendation; the migration approach in the alert-service section; the
  charting note (waterfall, colour meaning).
- **Latency section:** the original numbers kept, plus the A/B table and
  the OpenMP finding.
- Only these sections were committed. The README's other uncommitted
  edits (streaming, exactly-once, 100x sections) stay unstaged in the
  working tree, byte for byte as they were.

### Cleanup
The stack was stopped, both temporary worktrees removed, and the temporary
image tags deleted. The dev database keeps everything the live runs wrote:
120,584 alerts in total, of which 11,352 are new SHAP alerts and 1,640 are
old-format alerts from the Isolation Forest A/B run. Those 1,640 are a
useful side result: the *new* alert service ingested genuine old-format
messages from the old Spark image with no errors, which is the
backward-compatibility path from Phase 3 working for real, not just in a
unit test.

---

## Acceptance checklist (from the spec)

- [x] LightGBM drives live alerts; Isolation Forest scores in shadow
- [x] Threshold recalibrated to analyst capacity; new metrics recorded in `metrics.json` and the README
- [x] SHAP additivity test passes; the NaN test passes
- [x] Console shows the SHAP waterfall, and old alerts still render
- [x] Offline SHAP report + plots generated
- [x] All existing tests still pass; latency re-measured (and a 3x regression found and fixed)
- [x] Demo GIF regenerated (in the follow-up, after the browser extension reconnected)

## Resume line, with the real numbers

> "Served a LightGBM fraud model (PR-AUC 0.906) as champion with an
> Isolation Forest in shadow mode, with per-alert TreeSHAP explanations
> (independently verified to 1e-14) rendered as waterfall charts in the
> analyst console; recalibrated the alert threshold to analyst capacity,
> raising recall from 0.919 to 0.968 on the same 200-alert/day budget, and
> found and fixed a 3x end-to-end latency regression caused by OpenMP
> oversubscription inside Spark."

The spec's template line ("raising hybrid precision from 0.040 to X") can't
be used honestly: hybrid precision went to 0.036, because at 200 alerts/day
for ~12 frauds/day precision is capped near 6% for any model. The
improvement is in recall, and in precision at a right-sized budget (0.71 vs
0.52 at 15/day).

---

## Follow-up — closing the remaining items

After Phase 5, the open items were the demo GIF (the one unchecked spec
item) and the Phase 1 "open items" list. All of them are handled here
except the scikit-learn pin, which is explained below.

### 1. Demo GIF re-recorded
With the browser extension reconnected, the stack was started from the
already-built images (no rebuild, so nothing uncommitted got baked in),
with a light live stream at 2 tx/s. Recorded 23 frames:
- the live feed receiving a batch of new alerts
- opening a live model-only alert (LightGBM p = 2.9e-4, IF shadow 0.54)
- its SHAP waterfall (`hourOfDay = 21` +0.87, `amountToTrailingMaxRatio
  = 2.83` +0.68, …, output −8.14)
- the expanded "statistically unusual" z-score section
- a real **False Positive** disposition with a note
- the metrics page

Exported **with the watermark turned off**, then checked frame by frame:
no branding anywhere. Saved over `docs/demo.gif`, and the README caption
now describes what this recording actually shows.

Small honesty note: I clicked a model-only alert in row 2, but the feed
shifted under the click as a new batch arrived, so a different live alert
opened. It's an equally typical alert, so the recording continued with it,
and the caption describes the one that's actually shown.

### 2. The nightly retrain-check now checks both deployed models
**Problem:** since Phase 1, LightGBM decides alerts, but the nightly check
still retrained only an Isolation Forest and labelled its comparison
"champion". So the model that actually alerts was never re-checked.
**Fix** (`analytics/pulseguard_analytics/retrain_check.py`): one challenger
per deployed model, each compared with its *own* number from `metrics.json`:

```
"models": {
  "lightgbm":         {"role": "champion", "challenger_pr_auc": …, "deployed_pr_auc": …, "challenger_better": …},
  "isolation_forest": {"role": "shadow",   ...}
}
```
The LightGBM challenger reports `SKIPPED` (with a reason) if its
early-stopping validation split has no fraud, instead of crashing; the
other check still runs. `analytics/Dockerfile` now installs
`lightgbm==4.7.0`. Its comment had explicitly said it wasn't needed,
which stopped being true once LightGBM became the champion.
Tests: both models evaluated, each compared against its own deployed
number, and the LightGBM skip path (analytics 25 passed).

### 3. A real bug the retrain-check run exposed: `time_split` assumed sorted input
The first real nightly run (built from a clean worktree, on the 133,068
bronze rows the live runs produced) reported a **78,694-row holdout: 59%,
not 25%**. `time_split` took its cutoff timestamp by row position
(`df["timestamp"].iloc[cutoff_idx]`). The training dataset is sorted, so
training was never affected, but bronze rows collected from Spark come back
in no particular order, so the nightly "75/25" split cut at an arbitrary
row's timestamp. This bug predates this work: the README's older
0.362-vs-0.378 retrain result was produced with it.

**Fix:** the cutoff now comes from the *sorted* timestamps
(`sort_values(kind="stable").iloc[cutoff_idx]`). Nothing is reordered, so
sorted input gives byte-identical splits. Verified: the training split is
still exactly 140,969 / 46,990, so every committed metric and model stays
valid without retraining. New test: a shuffled frame splits exactly
750/250 by time, identical to the sorted one.

Rerun on real bronze: holdout **33,267 rows = exactly 25%**.

| Deployed model | Challenger PR-AUC (drifted bronze) | Deployed PR-AUC | Promote? |
|---|---|---|---|
| LightGBM (champion) | 0.542 | 0.906 | no |
| Isolation Forest (shadow) | 0.237 | 0.457 | no |

PSI flagged 22 of 26 features as significantly drifted on the same data,
the same out-of-distribution load-test traffic Phase 5 documented. The
report-only design did its job: retraining on this data and auto-deploying
would have made both models much worse.

### 4. `eval_set` deprecation fixed
`train_lightgbm` now uses `eval_X=` / `eval_y=` (added in LightGBM 4.7.0,
exactly the pinned version) instead of the deprecated `eval_set=`.
Verified equivalent: the same data fitted both ways gives the same best
iteration and a maximum prediction difference of 0.0, so no retrain was
needed. Tests now pass with `-W error::DeprecationWarning`.

### 5. The scikit-learn pickle version
The Isolation Forest is pickled with scikit-learn 1.7.2 (the local
version) while the Spark image pins 1.5.2, which logs an
`InconsistentVersionWarning` at load. This predates this work (the
original artifact was already 1.7.2), it loads and scores correctly in
every live run here, and fixing it properly means retraining in a
1.5.2 environment, which would change the committed Isolation Forest
metrics this whole log compares against. The right moment for that is the
next deliberate retrain, not a clean-up pass.

**Resolved afterwards, without changing a single metric.** The full
`train.py` run was repeated in a clean venv with exactly the pinned
`detection/requirements.txt` versions (scikit-learn 1.5.2, LightGBM 4.7.0),
into a scratch directory, and compared with the committed artifacts:
- `metrics.json`, `threshold.json`, `threshold_tradeoff.csv` and
  `lightgbm.txt` came out **byte-identical**.
- The new `isolation_forest.joblib` loads under 1.5.2 with **no warnings**,
  and scores all 187,959 transactions with a **maximum difference of 0.0**
  from the old pickle.

So the retrain changed only the version stamp inside the pickle, and the
new file replaced the old one. (`*.joblib` is gitignored, so this shows up
as no repo diff; the next image build copies the new file in.) The
underlying cause was the host Python having scikit-learn 1.7.2 installed
instead of the version `requirements.txt` pins. Training from a venv built
from that file avoids it.

### Tests after the follow-up
detection 45, streaming 21, analytics 25 (91 total), alert-service 31
(re-run after the follow-up: 31 passed).
