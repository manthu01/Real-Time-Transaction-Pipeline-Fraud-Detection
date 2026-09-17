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

![Live feed, drill-down, disposition, and metrics against the running stack](docs/demo.gif)

*Recorded against the actual `docker compose up` stack: the live feed,
opening an alert's drill-down (triggered rule, top contributing features,
map, the customer's other recent alerts), submitting a real disposition
(note + Confirmed Fraud), and the metrics page updating from it —
`THRESHOLD_STRUCTURING`'s precision reflects that submission.*

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
  - [x] Next.js analyst console: live feed (WebSocket-pushed, severity
        colour-coded), drill-down (transaction, triggered rules, top
        feature contributions, an OpenStreetMap embed, the customer's
        other recent alerts), and disposition capture — verified in an
        actual browser against the live stack, including a real
        disposition round-trip and the metrics dashboard updating from it.
  - [x] Delta Lake bronze layer (skipping silver, per the build plan's own
        short-on-time cut) written continuously by the Spark job, plus a
        nightly batch job (a plain Python script standing in for Airflow)
        computing gold aggregates, PSI drift, data quality gates, and a
        report-only champion-vs-challenger retrain check — verified against
        real bronze data, including two real bugs it surfaced along the way.
- [x] **Phase 5 — Load testing & failure drills.** Real measured throughput
      and latency numbers (including a full-pipeline bottleneck that isn't
      what the raw producer numbers suggest), a driver-kill drill with a
      global zero-loss/zero-duplication check, a Redis-outage drill that
      exposed and fixed a real crash-vs-degrade bug, and a dead-letter
      drill — see below for all of it, including two more real bugs found
      by actually running these tests rather than assuming they'd pass.

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
Redis   Kafka topic    Delta Lake bronze  Kafka topic
online  fraud.alerts   (26 features +     dead.letter
feature   │             labels, by date)
store     ▼                │
       NestJS alert         ▼
       service         Nightly batch job (cron-triggered script)
         │              ├─ gold: fraud rate, risk profiles,
     WebSocket          │   per-rule precision, alert funnel
         │              ├─ PSI drift vs. reference window
   Next.js analyst       ├─ data quality gates (fail loudly)
   console               └─ retrain-check: challenger vs.
   (live feed,               champion, report only
   drill-down,
   disposition)
         │
   analyst.dispositions (Kafka) → retrain-check     <- Phase 5: real retraining loop
```

*Everything above is built and verified against the live stack; see
Status. Only the load-testing/failure-drill numbers (Phase 5) are still
outstanding.*

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
                      API the analyst console uses
    src/alerts/        entity, service, controller, WebSocket gateway
    src/kafka/         consumer (fraud.alerts -> Postgres) and producer
                        (disposition -> analyst.dispositions), split into
                        separate modules to avoid a circular dependency
    src/metrics/       alerts/day, per-rule precision, mean time to
                        disposition, false-positive-rate trend
    *.spec.ts          22 tests (mocked repository, no live Postgres/Kafka)
  analyst-console/   Next.js: live feed, drill-down, disposition capture
    src/app/           live feed (/), drill-down (/alerts/[alertId]), metrics
    src/components/    severity/disposition badges, hand-rolled feature bars,
                        an OpenStreetMap iframe embed, the disposition form
    src/lib/           REST client, a shared socket.io singleton, shared types
analytics/          Nightly batch job: bronze reads, gold, PSI, quality, retrain-check
  pulseguard_analytics/
    psi.py             population stability index -- pure numpy/pandas
    data_quality.py     null-rate/range/volume gates -- pure pandas
    gold.py              daily fraud rate, risk profiles, per-rule precision,
                          alert funnel, from bronze Delta + a batch Kafka read
    retrain_check.py     challenger vs. champion, reusing pulseguard_detection's
                          already-tested training functions -- report only
    nightly_job.py        the 5-step orchestrator (the build plan's
                           "cron-triggered Python script" in place of Airflow)
  tests/              24 tests, all Spark-free (pure Python + pandas)
infra/
  kafka/create_topics.sh   topic provisioning (partitions/retention)
docker-compose.yml   Kafka (KRaft), Kafka UI, Redis, Postgres, Spark job,
                     alert service, analyst console, nightly-batch (profile:
                     "batch" -- not started by `up`, run via `docker compose
                     run --rm nightly-batch`)
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
cd analytics && python -m pytest tests/ -v
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

**9. The analyst console starts automatically too**, at
`http://localhost:3000`:

- **Live Feed** (`/`) — alerts streaming in over WebSocket, newest first,
  colour-coded by severity, filterable, click through to any alert.
- **Drill-down** (`/alerts/<alertId>`) — the transaction, which rules
  fired (or "flagged by the anomaly model alone"), the top 3 contributing
  features as bar charts, an OpenStreetMap embed of where it happened, the
  customer's other recent alerts, and the disposition form.
- **Metrics** (`/metrics`) — the same alerts/day, per-rule precision, mean
  time to disposition, and false-positive-rate trend the API exposes.

Verified in an actual browser against the live stack end-to-end, including
submitting a real disposition and watching the metrics page update from it.

```bash
cd serving/analyst-console
npm install
NEXT_PUBLIC_API_BASE_URL=http://localhost:3001 npm run dev   # local dev, without Docker
```

**10. The Spark job also writes a Delta bronze table** continuously (the
same enriched, feature-attached stream that goes to Redis and
`fraud.alerts`, partitioned by date) — no separate step needed, it starts
with `docker compose up -d` alongside everything else.

**11. Run the nightly analytics batch job by hand** (it has a `profiles:
["batch"]` tag specifically so it does *not* start with a plain `docker
compose up` — in a real deployment cron or Airflow would invoke it on a
schedule; here it's a deliberate manual step):

```bash
docker compose run --rm nightly-batch
```

Five steps, printed as they run: data quality gates over bronze (exits
non-zero if any fail — "fail loudly"), gold aggregates written to
`delta/gold/*` (daily fraud rate, customer risk profiles, per-rule
precision, alert funnel), PSI drift (an early bronze window vs. the most
recent one), a report-only champion-vs-challenger retrain check, and a
JSON report written to `analytics/output/`.

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

**The watermark (2 minutes) only actually gates the window-demo query,
not the feature path — and that's deliberate, not an oversight worth
hiding.** `applyInPandasWithState` here uses `GroupStateTimeout.ProcessingTimeTimeout`
for idle-state cleanup, not an event-time timeout, so Spark's watermark
doesn't filter rows out of the per-customer feature computation the way it
does for the `groupBy(customerId, window(...))` demo query — an event
arriving after the 2-minute mark still updates that customer's rolling
features. On a real fraud-detection critical path that's the right
trade-off (a late mobile transaction should still update the customer's
velocity state, not vanish), but it does mean this project has not built
the specific mechanism the build plan asks for -- counting watermark drops
and routing them to a side output -- because on the feature path there is
nothing to route: nothing gets dropped there. The one place a 2-minute-late
event is genuinely excluded is the window-demo aggregation, and this
project does not currently count or expose that number. That is a real,
named gap against the build plan's explicit ask, not a mechanism that
exists but goes unmentioned.

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
(187,959 transactions, 333 labelled fraud, time-based 75/25 train/holdout
split so nothing evaluates on data from before it was "trained"):

| Metric | Value |
|---|---|
| Isolation Forest PR-AUC (unsupervised, holdout) | 0.457 |
| LightGBM PR-AUC (supervised benchmark, holdout) | 0.864 |
| Rules-only: precision / recall / alert rate | 0.082 / 0.823 / 1.33% |
| Alert threshold, calibrated to 5 analysts × 40 reviews/day | 200 alerts/day → 2.21% alert rate |
| Model-only at that threshold: precision / recall | 0.055 / 0.919 |
| Hybrid (rules OR model≥threshold): precision / recall / alert rate | 0.040 / 0.919 / 3.02% |
| False-positive rate at matched recall (0.919): model-only vs hybrid | 1.46% vs 2.90% |

![Alerts/day vs fraud caught](detection/artifacts/threshold_tradeoff.png)

**Detection rate per injected fraud pattern**, on the same holdout
(hybrid = rules OR model≥threshold; each pattern's `n` is how many holdout
rows carry that label — small by construction, since only ~0.3% of
generated volume is fraud in the first place):

| Fraud pattern | n (holdout) | Rules-only recall | Hybrid recall |
|---|---|---|---|
| CARD_TESTING | 31 | 0.936 | 1.000 |
| MULE_ACCOUNT | 16 | 0.875 | 1.000 |
| SALAMI_SLICING | 7 | 0.286 | 0.571 |
| GEO_IMPOSSIBLE_TRAVEL | 4 | 0.500 | 0.500 |
| ACCOUNT_TAKEOVER | 2 | 1.000 | 1.000 |
| MERCHANT_CATEGORY_ANOMALY | 2 | 1.000 | 1.000 |

Two things worth saying about this table rather than just the numbers:
the model closes real gaps the rules leave open on the higher-volume
patterns (SALAMI_SLICING goes from catching 2 of 7 to 4 of 7; MULE_ACCOUNT
and CARD_TESTING both reach 100% once the anomaly score backs up the
rules), and GEO_IMPOSSIBLE_TRAVEL is the one pattern neither layer fully
closes — with only 4 holdout examples, "50% recall" is one missed
transaction, not a statistically meaningful weakness, but it's also not
something to round up to a stronger claim than the data supports. The
`ACCOUNT_TAKEOVER`/`MERCHANT_CATEGORY_ANOMALY` rows (n=2 each) are too
small to draw any conclusion from at all beyond "both examples happened to
be caught" — they're reported because the build plan asks for detection
rate per pattern, not because n=2 tells you anything a bank would act on.

A few things worth saying honestly about these numbers rather than letting
them stand alone:

- **LightGBM's PR-AUC (0.864) is high because the injected fraud patterns
  are, by construction, extreme outliers** — an account takeover is 8-15x
  a customer's trailing mean at 2-4AM; that's an easy signal for a
  supervised model once it has labels. `best_iteration_: 1` (found via
  early stopping against a validation split carved from the *training*
  set, never the holdout) means a single boosting round already separates
  the classes about as well as 1000 would — more rounds overfit the ~250
  training examples of the minority class. This is a real, measured
  finding, not a hyperparameter picked because it flattered the holdout
  score.
- **The Isolation Forest's lower PR-AUC (0.457) is the expected cost of
  going unsupervised** — it has no labels to learn from and has to decide
  what's anomalous purely from the shape of normal behaviour. That gap
  *is* the answer to "what do you give up going unsupervised," and it's
  why the rules exist alongside it: rules give zero-latency, fully
  explainable coverage of the known patterns the unsupervised model is
  weakest on.
- **This retrain's honest surprise: at matched recall (0.919), the hybrid
  system's false-positive rate (2.90%) is *worse* than the model alone
  (1.46%), not better.** The model-only comparison sweeps every possible
  threshold to find the cheapest way to reach that recall; the hybrid
  number is rules-OR-model at the threshold actually deployed, and here
  the rules add alerts (they're the low-precision 0.082 layer from the
  table above) without adding recall the model didn't already have on its
  own at that operating point — so this retrain, ORing them in is pure
  false-positive cost, not a net win. That's a real result, not the one an
  earlier run of this same pipeline produced (which found the opposite),
  and the honest takeaway is that whether rules-alongside-a-model help or
  hurt precision at a given recall depends on how much incremental recall
  they're actually contributing that day — worth checking per retrain, not
  something to assume permanently holds in either direction. The reason to
  keep rules at all is unchanged: zero-latency, fully explainable coverage
  that keeps working when the model is down or retraining, independent of
  whether they help or hurt the blended precision number this week.
- **These are demo-scale numbers, not bank-scale ones.** The holdout spans
  5.2 days at ~9,036 txns/day; the build plan's own worked example assumes
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
map of recent geo activity. The geo map is covered (the alert payload was
extended end-to-end -- Spark's `alert_scoring.py` through to the
console's `GeoEmbed` component -- to carry amount, channel, merchant, city,
and lat/lon, not just the score and top features). Full transaction
history isn't: this service can offer a customer's other *alerts*
(`GET /alerts/:alertId` returns `recentForCustomer`), but not their
non-alerted transactions too, which needs a queryable transaction store --
the Delta Lake bronze layer's job, still planned.

## The analyst console, in more detail

**Next.js pinned to 15.x, not 14.x.** `npm audit` flagged Next 14.2.5's
transitive dependencies with a critical/high advisory range; upgrading to
the latest 15.x line (rather than the `npm audit fix --force`-suggested
jump to 16, a breaking change not worth taking mid-build) resolved all but
one moderate/high advisory in PostCSS's CSS-stringify/sourcemap handling —
a build-time-only code path that never processes attacker-controlled CSS
in this project (`globals.css` is static and developer-authored), so it's
accepted rather than chased into a major-version migration.

**No charting library, on purpose.** Three bars showing top feature
z-scores, and a handful of stat tiles and a sparkline on the metrics page,
don't need Recharts or D3 -- `TopFeaturesBars` and the metrics
sparkline are a few dozen lines of divs scaled by percentage width/height.
"Functional over beautiful" cuts both ways: it also means not reaching for
machinery a raw `<div style={{width: pct+'%'}}>` already does.

**No mapping library either.** `GeoEmbed` is a plain OpenStreetMap iframe
(`/export/embed.html?bbox=...&marker=...`) — no API key, no
react-leaflet, no tile-server config. A drill-down that needed multiple
markers, clustering, or interaction would justify a real mapping library;
"where did this one transaction happen" doesn't.

**`useParams()`, not the `params` prop.** Next.js 15 made a page
component's `params` prop a `Promise`, intended for `async` Server
Components that can `await` it. The drill-down page is a Client Component
(it owns a WebSocket subscription and the disposition form's interactive
state), and client components can't be `async` functions -- reaching for
`params` as a prop here would mean unwrapping a Promise with `React.use()`
for no benefit. `useParams()` from `next/navigation` reads the same route
param synchronously from client router state instead, which is both less
code and the more direct tool for a component that was never going to be
a Server Component in the first place.

**One socket, not one per page.** `getSocket()` is a module-level
singleton, not a connection opened inside a `useEffect` on every page that
needs live updates. Navigating from the live feed to a drill-down page (or
opening a second tab) doesn't multiply WebSocket connections to the alert
service -- every consumer just adds and removes its own listeners on the
one shared connection.

## The nightly analytics job, in more detail

**Bronze, not bronze+silver+gold.** The build plan's own short-on-time cut
("Delta Lake layers — cut to bronze + gold, skip silver") is taken here
literally: bronze is the Spark job's already-enriched output (raw fields +
all 26 features + the isFraud/fraudPattern labels), partitioned by date --
what a strict medallion architecture would call silver, folded into
bronze. Every gold computation, PSI check, and quality gate reads this one
table. That's a real simplification, not a euphemism for "we didn't do
it" -- it's the specific trade the plan explicitly sanctions.

**Gold is computed twice, on purpose.** Per-rule precision and the alert
funnel already exist live in the NestJS alert service, computed from
Postgres. The nightly job recomputes them independently from Delta bronze
+ a batch read of `fraud.alerts`/`analyst.dispositions` from Kafka --
deliberately a second, structurally different code path (SQL over
Postgres vs. Spark over Delta+Kafka) rather than the same query run twice.
When I ran both, they agreed exactly (1 `CONFIRMED_FRAUD` on
`ATO_SIGNATURE`, 100% precision; the same disposition counts by severity
in the funnel) -- which is the actual point: two independent
implementations agreeing is a much stronger correctness signal than one
implementation being fast.

**Two real bugs found running this against actual bronze data, both fixed:**

1. **`DataFrame.toPandas()` crashed with `TypeError: Casting to unit-less
   dtype 'datetime64' is not supported`.** This Spark version's
   Arrow-optimized conversion path falls back to a legacy per-column
   route that calls pandas' `astype('datetime64')` without a unit --
   removed in pandas 2.0. Rather than chase a fragile pyarrow/pandas
   version pin that could silently break again on the next bump, every
   `toPandas()` call in the nightly job was replaced with
   `spark_df_to_pandas()`: collect rows, build the DataFrame directly.
   Slower than Arrow conversion, irrelevant at this job's batch scale.
2. **The null-rate quality gate false-failed on `secondsSincePrevTxn`**
   (4.03% null against a 1% threshold). That field is null exactly once
   per customer -- their first-ever transaction -- so a blanket threshold
   across all 26 features flags a structural property of the data as a
   defect. Fixed by excluding it from the null-rate gate specifically
   (range/other checks are unaffected), with the reasoning left in a
   comment rather than just silently narrowing the check.

**Real findings from running it, not synthetic ones.** Against the bronze
data accumulated across this project's own testing (49,644 rows):

- **PSI flagged 16 of 26 features as significantly drifted**
  (`hourOfDay` PSI=8.62, `maxTxnsInAny60SecWindowToday` PSI=2.08, and
  more), comparing an early bronze window against the most recent one.
  This independently confirms, via a completely different mechanism, the
  same finding the detection layer's README section already documents:
  a large share of this project's own load-test firehose traffic doesn't
  resemble the realistically-paced data used for training, and the drift
  monitor correctly says so. Two unrelated pieces of the system agreeing
  is more convincing than either alone.
- **The retrain-check correctly said no.** A challenger Isolation Forest
  trained on the (firehose-skewed) accumulated bronze data scored PR-AUC
  0.362 against the champion's 0.378 on a comparable holdout --
  `challenger_better: false`. Exactly the scenario "evaluate against
  champion, don't auto-promote" exists to catch: more accumulated data
  isn't automatically better data, and a nightly job that blindly
  retrained-and-deployed on whatever showed up would have made the
  production model worse.

## Load testing and failure drills

Everything in this section is a number or an outcome actually observed
running against the live Docker stack on this machine (`local[2]` --
2 Spark cores, one broker, one Redis instance), not an estimate. Per the
build plan's own admonition, and because it turned out to matter: none of
these numbers are what I would have guessed going in.

### The headline finding: three consumers cost roughly 15-40x, not 3x

`feature_job.py`'s own code comment already documents the tradeoff --
`feature_stream` is consumed by three independent `writeStream` queries
(Redis, Delta bronze, `fraud.alerts`), and Spark re-executes
`applyInPandasWithState` separately for each, recomputing the same 26
features three times rather than sharing one state store. I assumed that
meant "roughly 3x the cost." Measuring it said otherwise:

| Configuration | Rate tested | Result |
|---|---|---|
| Full pipeline (Redis + bronze + alerts) | 20 tx/s (accurately paced) | Batches took 7-24s against a 5s trigger -- falling behind immediately, unboundedly |
| Isolated (Redis only) | 20, 100, 300 tx/s | Zero "falling behind" batches at any of these |
| Isolated (Redis only) | 800 tx/s | Two batches marginally over (5.4s, 5.3s vs. 5.0s trigger) -- the first sign of strain |

The full pipeline can't sustainably clear even 20 tx/s; removing two of
three consumers and keeping everything else identical clears 300 tx/s
without a single missed trigger and only starts to strain around 800.
That's not a 3x difference, it's closer to 15-40x -- Python UDF overhead,
GIL contention across `local[2]`'s two cores, and per-partition Redis I/O
each get paid three times over, and at this deployment's resource scale
that compounds rather than adding linearly. **What this means, honestly:**
the "recompute vs. materialize once" tradeoff documented in `feature_job.py`
is real and worth being able to explain in an interview, but at production
scale the fix is exactly what the code comment says it isn't doing yet --
materialize `feature_stream` to one intermediate topic or table once, and
have Redis/bronze/alerts all read from *that*, or give the job more cores
than a laptop's `local[2]`. Both are legitimate next steps; neither was
free to skip.

### End-to-end latency (full pipeline, 20 tx/s, accurately paced)

Measured as `alertPublishTime - transactionTimestamp` from a Kafka
consumer probe reading `fraud.alerts` directly (its own consumer group,
`auto.offset.reset=latest`, so it only sees new alerts, not backlog
replay):

| Percentile | Latency |
|---|---|
| p50 | 10.6 s |
| p95 | 15.1 s |
| p99 | 16.2 s |
| max | 16.8 s |

Ten-plus seconds of median latency at a rate the pipeline can't actually
sustain is the expected shape of a growing backlog, not a flat processing
cost -- it's the same underlying finding as the throughput numbers above,
seen from the consumer's side instead of the producer's side.

### A real bug the load test itself was built on: the generator's own rate limiter

Before any of the numbers above could be trusted, the generator's
`loadtest` pacing loop turned out to be wrong. It slept for
`min(next_due - now, 0.01)` -- capped at 10ms -- but then advanced the
schedule by a full interval regardless of whether that sleep actually
closed the gap:

```python
if now < next_due:
    time.sleep(min(next_due - now, 0.01))   # capped at 10ms...
next_due += target_interval                  # ...but scheduled as if it wasn't
```

For any target rate below 100 tx/s (where the true required gap exceeds
10ms), this silently under-slept every iteration and the loop raced ahead
of its own schedule. `--rate 20` measured **84-93 achieved tx/s** before
the fix -- an inaccuracy that would have gone completely unnoticed if
"looks roughly load-test-shaped" had been the bar instead of "achieved_tps
printed at the end must match what was asked for," which the plan's own
"never claim a number you haven't measured" instruction exists to force.
Fixed by looping the sleep until `next_due` is actually reached; a
regression test now runs a short real load test and asserts the achieved
rate lands within tolerance of the target, at both a rate the old bug
broke badly (20) and one it broke less obviously (60).

### Failure drills

**1. Kill Redis mid-stream.** Stopped the Redis container while ~100 tx/s
of traffic was flowing. Before this drill, this would have taken the
*entire* pipeline down with it: `redis_writer.write_partition` had no
error handling, so a `RedisError` would propagate out of
`foreachPartition`, fail the Spark task, fail the "features → Redis"
query, and -- because `feature_job.main()` used
`spark.streams.awaitAnyTermination()` with no exception check afterward --
unblock that call and let the driver process exit cleanly-looking, even
though a query had actually crashed, silently taking the still-healthy
alerts and bronze queries down with it. Fixed in two places:
`write_partition` now catches `redis.exceptions.RedisError` specifically
and logs a warning instead of raising (four new tests cover this,
including one confirming a *non*-Redis exception still propagates --
only the expected, recoverable failure mode is swallowed), and `main()`
now checks every query's `.exception()` after `awaitAnyTermination()`
returns and exits non-zero if any query actually failed, so a genuinely
unexpected crash still surfaces loudly instead of looking like a clean
stop.

With the fix: killing Redis produced zero exceptions, the container
stayed up, and `fraud.alerts` grew by ~9,650 messages *during* the
outage -- rule/model scoring reads the Spark-managed checkpointed state
directly, never Redis, so alerting kept running at full fidelity
throughout. Restarting Redis resumed feature-store writes automatically,
no manual intervention. The one honest cost: batches slowed down while
Redis was unreachable (DNS resolution failing per partition adds real
wall-clock time before the 2-second connect timeout gives up), so
"degrades, doesn't crash" is accurate, but "degrades for free" isn't --
a production system would want a circuit breaker to stop attempting
Redis writes after the first few failures rather than retrying every
partition every batch.

**2. Publish malformed records.** Sent a burst with a 10% deliberately
malformed rate (a required field dropped per record, reusing the
generator's existing `--malformed-rate` flag from Phase 1/2). `dead.letter`
grew by 82 messages against ~88 expected; the job logged zero exceptions
and kept processing everything else. This was already demonstrated in
Phase 2's initial build; re-run here specifically to confirm it still
holds after everything else that changed since.

**3. Kill the driver mid-batch, verify no loss or duplication.** `local[2]`
runs executors as threads inside the driver JVM, so there's no separate
executor process to kill independently the way a multi-node cluster would
have -- the equivalent, and arguably more relevant, failure mode for this
deployment is killing the whole driver container. Sent a burst (1,201
transactions), `docker kill`'d the container mid-processing (not a
graceful stop), sent a second burst while it was down (801 more --
confirming Kafka's durability doesn't care whether anything is consuming),
then restarted it. The checkpoint's last committed offset, once caught up,
matched the topic's end offset **exactly**, and a global check across the
*entire* bronze table (not just this drill's narrow time window, which
turned out to have its own timestamp-boundary measurement noise) gave:

```
raw.transactions total:  70,588
dead.letter total:          135
bronze total rows:       70,453   =  70,588 − 135, exactly
bronze distinct txnIds:  70,453   =  zero duplicates
```

Every non-malformed transaction ever produced across this project's
entire testing history landed in bronze exactly once, including across an
abrupt kill mid-batch. That's the practical meaning of "exactly-once" this
project can actually demonstrate, not just assert.

**4. Stop the consumer, restart, verify lag recovery.** Mechanically the
same evidence as drill 3 -- the checkpoint resumed from its last committed
offset per partition and caught up to the new topic end without
intervention or reprocessing. The two are the same recovery mechanism
regardless of whether the interruption was graceful or a `SIGKILL`; drill
3 demonstrates the harder case.

### Exactly-once, precisely

The build plan's own advice is to be able to state this mechanism exactly,
not gesture at it -- and having now tested it against a real abrupt kill,
here's what's actually true, including the parts that are "effectively
exactly-once" rather than transactionally guaranteed:

- **Kafka source offsets are committed to the Spark checkpoint, not to a
  Kafka consumer group.** Verified directly, not assumed: `checkpoints/bronze/offsets/<N>`
  contains a literal per-partition offset map, and after drill 3, that map
  matched the topic's end offsets exactly.
- **Redis writes are naturally idempotent.** Each write is a full
  `HSET` of a customer's current feature snapshot, keyed by `customerId`,
  not an append or increment -- replaying the same micro-batch twice
  produces the same end state, not a duplicate.
- **Delta bronze writes are atomic per micro-batch**, via Delta's
  transaction log -- a batch either fully commits or leaves no partial
  trace, which is what prevents a crash mid-write from leaving bronze in
  a half-written state. It is *not* configured with Delta's explicit
  `txnAppId`/`txnVersion` idempotent-write API, which is the piece that
  would guarantee a *retried* batch (Spark's checkpoint advances the
  offset, then crashes before recording that this specific batch's Delta
  write already succeeded) can't double-write. That's a real, named gap,
  not a hidden one -- the global zero-duplicate result above shows it
  didn't happen in this project's testing (including one abrupt kill),
  but "didn't happen in testing" and "can't happen" are different claims,
  and only the first one is true here today.
- **The `fraud.alerts` and `dead.letter` Kafka sinks are at-least-once**,
  not exactly-once, at the write level -- Spark's plain Kafka sink isn't
  wrapped in a transactional producer the way the Delta sink gets atomic
  commits for free. What actually makes the *effect* exactly-once is
  downstream: the alert service's `ingestFromKafka` upserts by `alertId`
  (see `serving/alert-service`), so a duplicate alert message -- if a
  retry ever produced one -- is a no-op on arrival, not a second row.

Put together: this system does not have one unified transactional
guarantee spanning every hop from Kafka to Postgres. It has checkpoint-
ordered offset commits, one genuinely atomic sink (Delta), one naturally
idempotent sink (Redis), and one at-least-once sink whose duplicates are
absorbed by an idempotent consumer (the alert service). That combination
is what "exactly-once" means in most real streaming systems that claim
it, and being able to say which piece provides which guarantee -- rather
than "we use checkpoints so it's exactly-once" -- is the difference the
build plan is pointing at.

## What breaks at 100x, and what I'd change

The build plan asks this explicitly, and it deserves an answer grounded in
what this session actually measured, not a generic "add more servers."
Nothing here was tested at 100x (that would be ~2,000-3,000 tx/s on top of
the ~20-300 tx/s range this project measured) -- these are the specific
bottlenecks this pipeline's own load tests exposed, extrapolated honestly
rather than a fresh list of textbook scaling advice:

- **The single biggest lever is the one Phase 5 already measured: stop
  recomputing the feature stream three times in one Spark driver.**
  Bronze, the Redis write, and the alert-scoring path are three
  co-located streaming queries sharing `local[2]`'s two cores and one
  JVM's Python-UDF overhead -- that's the actual reason three consumers
  cost 15-40x instead of 3x, and it's also the first thing to fix, since
  it's an architecture cost this deployment is paying for free, not a
  law of streaming systems. Splitting them into independently-scaled
  Spark applications (or at minimum separate executor pools) turns "three
  consumers reading the same topic" back into roughly three times the
  cost of one, not fifteen.
- **Six Kafka partitions on `raw.transactions`, one broker, no
  replication.** Partition count caps how much of the above fix actually
  parallelizes -- more consumer parallelism than 6 partitions is wasted.
  A single-broker KRaft cluster (this project's whole point was proving
  streaming *semantics*, not running HA infrastructure) also means a lost
  broker is data loss, not a processing pause, at any volume. Both need
  fixing before 100x, not after: repartition `raw.transactions` to
  something like 24-48, and run a 3-broker cluster with
  `replication.factor=3`.
- **Redis is a single instance.** The Phase 5 drill proved the pipeline
  *degrades* correctly when it's unreachable (an important property to
  keep), but at 100x volume a single Redis process is also a throughput
  ceiling on its own, not just an availability risk. Redis Cluster or at
  minimum a replica for read failover is the fix; the existing
  degrade-don't-crash behavior in `redis_writer.py` stays exactly as it
  is either way.
- **Delta bronze isn't configured for idempotent writes or compaction.**
  The `txnAppId`/`txnVersion` gap documented above matters more at 100x,
  where a checkpoint-then-crash race is proportionally more likely simply
  because there are more micro-batches per hour. Separately, continuous
  small per-batch commits at 100x the write rate will produce a small-file
  problem Delta's `OPTIMIZE`/compaction exists specifically to solve --
  this project never needed a compaction job because bronze never got
  large enough for it to matter, but it would at 100x.
- **The alert service does one Postgres insert per alert and one
  in-process WebSocket broadcast.** That's fine at this project's alert
  volume (a few thousand alerts total across all testing); at 100x traffic
  with a proportionally higher alert rate, this becomes batched inserts
  (or a queue in front of Postgres) and either sticky-session WebSocket
  routing or a pub/sub fan-out (e.g. Redis pub/sub) if the alert service
  itself needs to run as more than one instance -- untested here because
  one instance was never a bottleneck at this project's volume.
- **The generator itself is a single Python process.** After fixing its
  rate-limiter bug (see above), it hits its target rate accurately, but
  "accurately" tops out well short of the build plan's stated 3,000-5,000
  tx/s achievable from a batched-produce `confluent-kafka` process --
  this project's generator uses `confluent-kafka` but produces one record
  at a time per the behavioural-pacing design, which is the right choice
  for *realistic* traffic shape and the wrong one for *maximum* throughput.
  A pure firehose at 100x would need either batched produces or multiple
  producer processes partitioned by customer range, and that's a
  deliberate trade-off this project made in favor of realistic data over
  raw throughput, worth stating as a choice rather than a limitation
  discovered by accident.
- **State store growth was never stress-tested at a larger customer
  population.** `mapGroupsWithState` has a configured processing-time
  timeout, so inactive customers do get evicted, but this project's
  largest run used 2,000-5,000 synthetic customers -- confirming the TTL
  actually bounds RocksDB state size at, say, 500,000 active customers is
  a real gap between "the mechanism is correct" and "the mechanism was
  measured at scale," and the honest answer is this project has only
  verified the former.

## Common mistakes

These are the traps this project is deliberately built to avoid — see the
build plan for the full list:

- **Random data** instead of behavioural profiles.
- **Label leakage** — `isFraud`/`fraudPattern` in the feature vector.
- **Reporting accuracy** at <1% fraud prevalence (meaningless — precision/recall/PR-AUC only).
- **Unbounded state** in the streaming job.
- **Claiming throughput/latency numbers that were never measured.**
