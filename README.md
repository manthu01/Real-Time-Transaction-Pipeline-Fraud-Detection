# PulseGuard — Real-Time Transaction Monitoring & Fraud Detection Pipeline

Synthetic UPI, card, and NEFT transactions stream through Kafka, get scored
for fraud in real time by a hybrid rules + anomaly-model detection layer,
and surface to an analyst console with per-alert explainability. Analyst
decisions feed back into a label store for retraining, and a nightly batch
layer tracks fraud rate, per-rule precision, and feature drift.

The engineering emphasis is on streaming semantics that matter in a real
transaction-monitoring system: partition-by-customer for ordered stateful
processing, watermarked late-event handling, exactly-once delivery via
checkpointed offsets and idempotent sinks, and a single feature store shared
between training and serving so the two never drift apart.

## Status

This project is being built incrementally, in phases. Checked items are
implemented and tested; unchecked items are planned.

- [x] **Phase 1 — Generator & ingestion infra.** Behavioural customer
      population (5 archetypes), 6 labelled fraud patterns, a Kafka
      producer capable of both a realistically-paced historical stream and
      a rate-controlled load-test firehose, and the Kafka/Redis/Postgres
      Docker Compose stack with topics provisioned.
- [x] **Phase 2 — Streaming feature computation.** Spark Structured
      Streaming job computing 26 rolling behavioural features per customer
      via a stateful per-key operator, a watermarked sliding-window
      aggregation demo, dead-letter routing for malformed records, and a
      Redis online feature store — verified end-to-end against live Kafka.
- [x] **Phase 3 — Detection layer.** A safe (no-`eval`) versioned rule
      engine, an Isolation Forest anomaly model benchmarked against a
      supervised LightGBM model, an alert threshold calibrated to analyst
      review capacity, and per-alert explainability — wired live into the
      Spark job and verified producing real alerts to `fraud.alerts`.
- **Phase 4 — Serving & analytical layer.**
  - [x] NestJS alert service: consumes `fraud.alerts`, persists idempotently
        to Postgres, broadcasts over WebSocket, serves REST history/detail,
        and captures analyst disposition back to `analyst.dispositions` —
        verified end-to-end including live WebSocket push.
  - [ ] Next.js analyst console (live feed, drill-down, disposition UI).
  - [ ] Bronze/silver/gold Delta Lake layer with nightly PSI drift and
        per-rule precision jobs.
- [ ] **Phase 5 — Load testing & failure drills.** Throughput and latency
      numbers under load, plus documented recovery from executor kills,
      Redis outages, and malformed records.

## Architecture

```
Transaction Generator (Python, behavioural profiles + injected fraud)
        │
        ▼
Kafka topic: raw.transactions
(partitioned by customerId, 6 partitions)
        │
   ┌────┴────────────────────────┐
   │  Spark Structured Streaming │
   │  ├─ parse + validate        │
   │  ├─ 26 rolling features     │
   │  ├─ velocity rule engine    │
   │  └─ Isolation Forest score  │
   └────┬─────────────────────────┘
        │
  ┌─────┼───────────────┬──────────────────┐
  ▼     ▼               ▼                  ▼
Redis   Kafka topic    Delta Lake         Kafka topic
online  fraud.alerts   bronze/silver/gold dead.letter
feature   │             (Phase 4)
store     ▼                │
       NestJS alert    Nightly batch jobs
       service         (fraud rate, per-rule
         │              precision, PSI drift)  <- Phase 4
     WebSocket
         │
   Next.js analyst console      <- Phase 4
   (live feed, drill-down, disposition)
         │
   analyst.dispositions (Kafka) → retraining     <- retraining is Phase 4/5
```

*Delta Lake, the Next.js console, and PSI drift are planned; see Status.
Everything else above — including the NestJS alert service and the
disposition→Kafka feedback loop — is built and verified.*

## Why each piece matters

| Component | Why a bank cares |
|---|---|
| Partition by customer ID | All of a customer's events land in order on one partition. Stateful aggregation is meaningless otherwise. |
| Watermarking | Mobile transactions arrive late constantly. Dropping them silently corrupts velocity features. |
| Exactly-once semantics | Double-counting a transaction in a fraud aggregate creates false alerts; missing one creates false negatives. |
| Shared online feature store | Training/serving skew is the most common reason production fraud models underperform their offline metrics. |
| Rules alongside the model | Rules give zero-latency, fully explainable coverage of known patterns and keep working when the model is retraining. |
| Threshold tuned to analyst capacity | Alert volume is bounded by how many cases a human team can review. F1-optimal is operationally useless if it produces 4,000 alerts a day. |
| Analyst disposition capture | The only source of ground-truth labels in real fraud operations. Without it, the model never improves. |
| PSI drift monitoring | Fraud patterns shift weekly. A model with no drift monitor is a model nobody trusts. |

## Repository layout

```
common/             Shared package: city catalog + haversine distance,
  pulseguard_common/  used by both the generator and the streaming job so
                       "Mumbai" means the same coordinates on both sides
generator/          Python transaction generator
  pulseguard_generator/
    archetypes.py    customer behaviour profiles
    customers.py     synthetic customer population
    merchants.py     merchant/category/city catalog
    fraud_patterns.py  6 injected fraud patterns
    normal_transactions.py  legitimate transaction generation
    simulator.py     behavioural-pace and load-test stream engines
    sinks.py         Kafka / stdout / JSONL output sinks
    cli.py           `simulate` and `loadtest` commands
  tests/
streaming/          Spark Structured Streaming feature computation job
  pulseguard_streaming/
    feature_state.py  pure-Python per-customer feature logic (no Spark
                       dependency -- the same function will be reused by
                       the Phase 3 batch training feature backfill)
    state_codec.py     serializes that state to/from Spark's checkpoint
    feature_job.py     the actual Structured Streaming pipeline
    schema.py / output_schema.py   Spark schemas, kept in lockstep with
                                    feature_state.py's feature list by a test
    redis_writer.py    writes feature vectors to the online feature store
  tests/             17 tests, all Spark-free (pure Python + pandas)
  Dockerfile         apache/spark-py:v3.4.0 + this job's dependencies
detection/          Hybrid rules + anomaly-model detection layer
  pulseguard_detection/
    rule_engine.py   safe (no `eval`) AST-based expression evaluator
    rules.json       6 versioned velocity rules
    dataset.py       batch feature backfill -- replays history through
                      the *same* feature_state.apply_event the streaming
                      job uses, so there is one implementation of "what a
                      feature means," not two
    threshold.py     alert threshold selection from analyst capacity
    train.py         trains + evaluates Isolation Forest and LightGBM,
                      writes artifacts/ (models, threshold, metrics, chart)
    score.py         the live scoring function: rules + model + explainability
  artifacts/         trained models + evaluation evidence (gitignored
                      except metrics.json/threshold_tradeoff.*/threshold.json/
                      feature_names.json -- see Getting started)
  tests/             34 tests, no Spark/Kafka/trained-model required
serving/
  alert-service/     NestJS: consumes fraud.alerts, serves the REST/WebSocket
                      API the (future) analyst console will use
    src/alerts/        entity, service, controller, WebSocket gateway
    src/kafka/         consumer (fraud.alerts -> Postgres) and producer
                        (disposition -> analyst.dispositions), split into
                        separate modules to avoid a circular dependency
    src/metrics/       alerts/day, per-rule precision, mean time to
                        disposition, false-positive-rate trend
    *.spec.ts          22 tests (mocked repository, no live Postgres/Kafka)
infra/
  kafka/create_topics.sh   topic provisioning (partitions/retention)
docker-compose.yml   Kafka (KRaft), Kafka UI, Redis, Postgres, Spark job,
                     alert service
```

## Getting started

**1. Bring up the infra stack:**

```bash
docker compose up -d
```

This starts Kafka (KRaft mode, no ZooKeeper) on `localhost:9092`, creates
the topics below, starts Kafka UI at `http://localhost:8080`, Redis at
`localhost:6380`, and Postgres at `localhost:5434` (non-default Redis/Postgres
ports so this stack doesn't collide with other local projects — adjust in
`docker-compose.yml` if you don't need to avoid a clash), and builds/starts
the Spark feature job. First run needs internet access: `spark-submit`
resolves the Kafka connector jars from Maven Central on startup and caches
them in the `spark_ivy_cache` volume, so subsequent restarts are fast.

| Topic | Partitions | Retention | Key |
|---|---|---|---|
| `raw.transactions` | 6 | 7 days | `customerId` |
| `fraud.alerts` | 3 | 30 days | `alertId` |
| `dead.letter` | 1 | 30 days | — |
| `analyst.dispositions` | 1 | forever (compacted) | `alertId` |

**2. Install the generator:**

```bash
cd generator
python -m venv .venv && source .venv/bin/activate  # or .venv\Scripts\activate on Windows
pip install -r requirements.txt
```

**3. Generate a labelled historical dataset** (behaviourally paced, useful
for feature engineering and model training):

```bash
python -m pulseguard_generator simulate \
  --customers 5000 --days 30 --sink kafka --topic raw.transactions
```

Or without Kafka running, to a file:

```bash
python -m pulseguard_generator simulate --customers 500 --days 3 --sink jsonl --out sample.jsonl
```

**4. Load-test the pipeline** (rate-controlled firehose, for Phase 5):

```bash
python -m pulseguard_generator loadtest \
  --customers 5000 --sink kafka --topic raw.transactions --rate 3000 --duration 120
```

The command prints achieved throughput every 5 seconds and a final summary
— see [Common mistakes](#common-mistakes): never claim a number you have
not measured.

**5. The streaming feature job starts automatically** with `docker compose
up -d` (it's a service in the same compose file, built from
`streaming/Dockerfile`), and will:

- Parse and validate `raw.transactions`, routing anything missing a
  required field to `dead.letter` instead of crashing.
- Compute 26 rolling behavioural features per customer and write them to
  Redis at `feat:{customerId}`, TTL-refreshed on every update.
- Run a second, independent query that prints a 5-minute/1-minute sliding
  window aggregate to its own logs — a direct demonstration of the
  watermarked sliding-window idiom, kept separate from the main feature
  pipeline (see [below](#the-streaming-feature-job-in-more-detail)).

Watch it work:

```bash
docker compose logs -f spark-feature-job   # batch progress, window-agg demo output
docker exec pulseguard-redis redis-cli HGETALL feat:C000001
```

**6. Train the detection layer** (host-side, not in Docker — it needs
lightgbm/matplotlib the Spark image doesn't carry, and this is meant to be
a deliberate, evaluated step, not something that silently reruns on every
container start):

```bash
cd generator
python -m pulseguard_generator simulate --customers 2000 --days 21 --seed 7 --sink jsonl --out ../detection/data/training_raw.jsonl

cd ../detection
pip install -r requirements.txt
python -m pulseguard_detection.train --input-jsonl data/training_raw.jsonl
```

This writes `detection/artifacts/`: the trained Isolation Forest and
LightGBM models, the calibrated threshold, the alerts/day-vs-fraud-caught
trade-off chart, and `metrics.json` with every number in the section below.
`docker-compose.yml` bind-mounts `detection/artifacts/` into the Spark
container read-only — restart it (`docker compose restart
spark-feature-job`) after training to pick up a newly-trained model. If
the artifacts don't exist yet, the job logs a warning and runs with
detection disabled rather than crashing.

**7. Run the tests** (all pure Python/pandas — no live Spark session,
Kafka, Docker, or a trained model required to run them; `streaming/`'s
tests only import `pyspark.sql.types` for schema definitions, so `pip
install pyspark` locally to run them outside the container):

```bash
cd generator && python -m pytest tests/ -v
cd streaming && python -m pytest tests/ -v
cd detection && python -m pytest tests/ -v
cd serving/alert-service && npm test
```

**8. The alert service starts automatically** with `docker compose up -d`
too, at `http://localhost:3001`. It consumes `fraud.alerts` from the
beginning of the topic on first run (so it backfills whatever the Spark
job has already produced), persists every alert to Postgres keyed by
`alertId` (idempotent — re-consuming the same message twice updates
nothing), and broadcasts `alert.created` / `alert.updated` over WebSocket.

```bash
curl http://localhost:3001/alerts?limit=5
curl http://localhost:3001/alerts/<alertId>          # detail + the customer's other recent alerts
curl -X POST http://localhost:3001/alerts/<alertId>/disposition \
  -H "Content-Type: application/json" \
  -d '{"status":"CONFIRMED_FRAUD","note":"verified with customer callback"}'
curl http://localhost:3001/metrics/summary           # alerts/day, precision by rule, mean time to disposition, FP-rate trend
```

A disposition both updates Postgres and publishes to `analyst.dispositions`
(compacted, keyed by `alertId`) — the label-store feedback loop a real
retraining pipeline would consume from.

## The generator, in more detail

A fraud-detection project is only as good as its data — random noise
produces a model and a rule engine that learn / trigger on nothing
meaningful. Each synthetic customer is assigned one of five behavioural
archetypes:

| Archetype | Behaviour |
|---|---|
| Salaried urban | 3–8 txns/day, 5–10 recurring merchants, one home city, salary credit on the 1st |
| Small business | 20–60 txns/day, wide merchant spread, high variance amounts, weekend activity |
| Student | 2–5 txns/day, low amounts, food and transport heavy, one city |
| Senior | <2 txns/day, very few merchants, mostly branch and ATM |
| Frequent traveller | Multi-city legitimately — geo-velocity rules must not flood on these |

Six fraud patterns are injected at a configurable rate (default ~0.3% of
volume), each a labelled burst of transactions: card testing, account
takeover, mule account, geo-impossible travel, merchant-category anomaly,
and salami slicing. `isFraud`/`fraudPattern` are populated on the wire
format but must never be included in a downstream feature vector — that's
label leakage, and the tell is a suspiciously perfect AUC.

## The streaming feature job, in more detail

**Two independent stateful mechanisms, deliberately.** The build plan's
watermarked-window code sample (`groupBy(customerId, window(...))`) is the
right tool for a straightforward rolling count/sum over a fixed window, but
several features here don't fit that model at all — a 30-day trailing
mean, "distinct devices in the last 7 days," or "is this merchant category
new for this customer, ever" don't have a single window length, and
Spark's window-aggregation state is sized for tracking many concurrent
windows, not one arbitrarily-long customer history. So the job runs both:
a `groupBy(customerId, window(...))` query as a direct demonstration of
the watermarked sliding-window idiom (visible in `docker compose logs`),
and, for the actual feature vector, a `applyInPandasWithState` operator
maintaining one compact state object per customer.

**One state object, not a dozen counters.** Each customer's state is a
bounded event-history buffer (pruned to 7 days, hard-capped at 3000 events
regardless of age — the answer to "your state store grows unboundedly, how
do you fix it") plus a few O(1) running stats (EMA mean/variance, running
max, a city-frequency map for modal-city, a device last-seen map, an
hour-of-day histogram). Every velocity and diversity feature is a cheap
filter over that one buffer, computed fresh per event, rather than a
separate incremental counter per window per feature. A
`ProcessingTimeTimeout` of 10 idle days drops a quiet customer's state
entirely.

**Trailing mean/std is an EMA, not an exact 30-day window** — an O(1)-state
approximation rather than maintaining a 30-day ring buffer per customer.
Worth saying out loud in an interview: it trades a little statistical
precision for constant memory per customer regardless of transaction
volume.

**Why the state is a JSON string, not native Spark types.**
`applyInPandasWithState`'s state must conform to a declared `StructType`;
mapping the state's sets/nested-dicts/lists onto
ArrayType/MapType/StructType field-by-field is possible but multiplies the
surface area for Arrow/Spark type-coercion bugs. Instead, the Spark-side
state schema is a single `StringType` field holding a JSON blob
(`state_codec.py`), and everything on the other side of that boundary is
plain Python already covered by `feature_state.py`'s unit tests —
including a round-trip test asserting that a checkpoint-restore produces
byte-identical downstream features to never restoring at all, which is the
property that actually matters for correctness after a crash.

**Training/serving skew, mechanically.** `feature_state.apply_event` takes
a plain dict state and a plain dict event, returns a plain dict of
features, and has no import of `pyspark` anywhere in it. The Structured
Streaming job calls it inside `applyInPandasWithState`; the Phase 3 batch
feature-backfill script (for building a training set from historical data)
will call the *same function*, in order, over each customer's historical
records. It's not "the same logic reimplemented for training" — it's the
same function, imported twice.

## The detection layer, in more detail

**Rules are data, evaluated by a restricted interpreter — never `eval()`.**
`rule_engine.py` parses each rule expression with Python's `ast` module and
walks the tree through a whitelist of node types (boolean/comparison/
arithmetic operators, variable lookups). There is no attribute access, no
subscripting, no function calls — so a malicious or malformed rule string
has no code-execution surface at all, regardless of what it contains. The 6
rules in `rules.json` are adapted from the build plan's examples to the
feature names this pipeline actually computes (e.g. `CARD_TESTING` checks
`maxTxnsInAny60SecWindowToday` rather than a `txnCount1Min`/`distinctMerchants1Min`
combination, since a single bounded event-history buffer already gives an
exact burst count).

**Real numbers, measured on a 2000-customer/21-day synthetic dataset**
(192,073 transactions, 447 labelled fraud, time-based 75/25 train/holdout
split so nothing evaluates on data from before it was "trained"):

| Metric | Value |
|---|---|
| Isolation Forest PR-AUC (unsupervised, holdout) | 0.378 |
| LightGBM PR-AUC (supervised benchmark, holdout) | 0.936 |
| Rules-only: precision / recall / alert rate | 0.092 / 0.747 / 1.40% |
| Alert threshold, calibrated to 5 analysts × 40 reviews/day | 200 alerts/day → 2.17% alert rate |
| Model-only at that threshold: precision / recall | 0.064 / 0.807 |
| Hybrid (rules OR model≥threshold): precision / recall / alert rate | 0.048 / 0.819 / 2.98% |
| False-positive rate at matched recall (0.819): model-only vs hybrid | 3.68% vs 2.84% |

![Alerts/day vs fraud caught](detection/artifacts/threshold_tradeoff.png)

A few things worth saying honestly about these numbers rather than letting
them stand alone:

- **LightGBM's PR-AUC (0.936) is high because the injected fraud patterns
  are, by construction, extreme outliers** — an account takeover is 8-15x
  a customer's trailing mean at 2-4AM; that's an easy signal for a
  supervised model once it has labels. `best_iteration_: 1` (found via
  early stopping against a validation split carved from the *training*
  set, never the holdout) means a single boosting round already separates
  the classes about as well as 1000 would — more rounds overfit the ~300
  training examples of the minority class. This is a real, measured
  finding, not a hyperparameter picked because it flattered the holdout
  score.
- **The Isolation Forest's lower PR-AUC (0.378) is the expected cost of
  going unsupervised** — it has no labels to learn from and has to decide
  what's anomalous purely from the shape of normal behaviour. That gap
  *is* the answer to "what do you give up going unsupervised," and it's
  why the rules exist alongside it: rules give zero-latency, fully
  explainable coverage of the known patterns the unsupervised model is
  weakest on.
- **The hybrid system's false-positive rate (2.84%) beats pushing the
  model-only threshold down to the same recall (3.68%)** — this is the
  correct comparison (same recall, different false-positive cost), not
  "hybrid precision vs. model-only precision at the same threshold" (which
  looks worse for the hybrid simply because it alerts on more volume).
- **These are demo-scale numbers, not bank-scale ones.** The holdout spans
  5.2 days at ~9,231 txns/day; the build plan's own worked example assumes
  4M txns/day. The *mechanism* — calibrate to review capacity, don't pick
  F1-optimal — is what transfers, not the literal alert-rate percentage.

**Explainability, mechanically.** `score.py`'s `top_contributing_features`
ranks each feature by how many standard deviations it sits from the
non-fraud training population's mean (`feature_stats.json`) — a
deliberately simple signal, not a SHAP value, chosen so the live scoring
path (`alert_scoring.py`, wired into `feature_job.py` via `mapInPandas`)
has no extra heavy dependency. Every alert written to `fraud.alerts`
carries `triggeredRules`, a `severity` (the highest severity among fired
rules, or scaled from how far the anomaly score cleared the threshold if
no rule fired), and the top 3 unusual features — the build plan's point
that "an alert without an explanation is an alert an analyst can't act on."

**A real bug this caught: NaN vs. None.** The first live run of the wired-in
scoring stage crashed every micro-batch with `ValueError: Input X contains
NaN`. Spark represents a nullable `DoubleType` column (like
`secondsSincePrevTxn` on a customer's very first transaction) as `NaN` once
it crosses into pandas/Arrow, not Python `None` — so a `value is not None`
guard silently failed to catch it, and the raw NaN reached
`IsolationForest.score_samples`. Fixed with an explicit `_is_missing()`
check (`math.isnan`, guarded for non-numeric types) and covered by a
regression test (`test_scoring_survives_nan_features_without_crashing`) so
it can't come back silently.

**A real observation, not a bug: load-test traffic looks anomalous.**
Once wired in, the live pipeline flagged a very high fraction of the
raw.transactions backlog — because a large part of that backlog is the
Phase 1/5 load-test firehose (hundreds of transactions/second across a
small customer pool), and the model was trained only on realistically-paced
`simulate`-mode data. A customer doing 0.6 transactions/second looks
nothing like the salaried-urban/student/etc. archetypes it learned "normal"
from, so the anomaly score (correctly) says so. That's a genuine
train/serve distribution mismatch worth knowing about, not a defect in the
scoring code — and exactly the kind of thing PSI drift monitoring (Phase 4)
is meant to surface automatically instead of requiring someone to notice it
in the logs.

## The alert service, in more detail

**Idempotent ingestion, on purpose.** The alert entity's primary key is
`alertId` (the UUID the Spark job generates), not an auto-incrementing
one. Re-consuming the same Kafka message twice — a consumer restart before
offsets commit, a rebalance, a replay — upserts into the same row instead
of creating a duplicate. This is the same exactly-once-in-effect concern
as the streaming job's idempotent Delta writes, just at the serving layer:
a duplicate alert isn't just untidy, it would double-count in `alertsPerDay`
and skew per-rule precision.

**Two Kafka modules, not one, to avoid a cycle.** `AlertsController` needs
the producer (to publish a disposition); `KafkaConsumerService` needs
`AlertsService` and `AlertsGateway` (to persist and broadcast an incoming
alert). Putting both in one `KafkaModule` that `AlertsModule` imports
creates `AlertsModule → KafkaModule → AlertsModule`. Splitting into
`KafkaProducerModule` (no dependency on `AlertsModule`) and
`KafkaConsumerModule` (imports `AlertsModule`, not the other way around)
breaks the cycle without reaching for `forwardRef()`.

**Two real bugs found during live verification, both now regression-tested:**

1. **`jsonb_array_elements_text()` inside TypeORM's `QueryBuilder.innerJoin()`
   generated invalid SQL** (`zero-length delimited identifier`) — the query
   builder's join API expects a relation path or a subquery, not an
   arbitrary set-returning expression. Per-rule precision (unnesting each
   alert's `triggeredRules` array to group by individual rule) is exactly
   the kind of Postgres-specific query that's clearer as raw SQL than
   forced through an ORM abstraction built for a different shape of query
   — fixed by dropping to `repo.query()` with the classic
   `FROM alerts, jsonb_array_elements_text(...)` lateral-unnest pattern.
2. **Mean time to disposition was measured from the wrong timestamp.**
   It used the alert's `timestamp` field (the underlying transaction's
   *event time*) instead of `ingestedAt` (when the alert actually appeared
   in this service). For live traffic those are seconds apart and the bug
   was invisible; testing against the Spark job's backlog — where event
   time can be hours or days before ingestion — produced a **negative**
   mean time to disposition, which is what surfaced it. "Time to
   disposition" is an analyst-workflow metric (how long did this sit in
   the queue before someone looked at it); it has to be measured from
   ingestion, not from whenever the underlying transaction happened.

**What "drill-down" can and can't show yet.** The build plan's console
mock-up includes "the customer's last 20 transactions for context" and a
map of recent geo activity. This service can offer a customer's other
*alerts* (`GET /alerts/:alertId` returns `recentForCustomer`), but not
their full transaction history — that requires a queryable transaction
store, which is the Delta Lake bronze layer's job (still Phase 4/planned),
not something to fake with an ad-hoc Kafka read here.

## Common mistakes

These are the traps this project is deliberately built to avoid — see the
build plan for the full list:

- **Random data** instead of behavioural profiles.
- **Label leakage** — `isFraud`/`fraudPattern` in the feature vector.
- **Reporting accuracy** at <1% fraud prevalence (meaningless — precision/recall/PR-AUC only).
- **Unbounded state** in the streaming job.
- **Claiming throughput/latency numbers that were never measured.**
