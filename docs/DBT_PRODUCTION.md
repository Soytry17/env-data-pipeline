# Production-Ready dbt — A Practical Guide (using env-data-pipeline)

> **Prerequisite:** read `docs/LEARN_DBT.md` first. That teaches dbt fundamentals. **This** guide teaches how
> to take a working dbt project (like ours) and make it **production-grade**: reliable, scalable, testable,
> observable, and safe to deploy on a schedule.
>
> Each topic follows the same shape:
> **Concept → Why it matters → Where this project stands today → How to make it production-ready.**

---

## Table of Contents

1. [What "production-ready" means for dbt](#1-what-production-ready-means-for-dbt)
2. [Environments & targets (dev vs prod)](#2-environments--targets-dev-vs-prod)
3. [Secrets & credentials management](#3-secrets--credentials-management)
4. [Project structure: staging → intermediate → marts](#4-project-structure-staging--intermediate--marts)
5. [Incremental models — the key to scale](#5-incremental-models--the-key-to-scale)
6. [A real testing strategy (severity, thresholds, store_failures)](#6-a-real-testing-strategy-severity-thresholds-store_failures)
7. [Source freshness & SLAs](#7-source-freshness--slas)
8. [Data contracts & model versions](#8-data-contracts--model-versions)
9. [Orchestration: `dbt build`, selectors, tags, retries](#9-orchestration-dbt-build-selectors-tags-retries)
10. [CI/CD & Slim CI (state + defer)](#10-cicd--slim-ci-state--defer)
11. [Performance & cost](#11-performance--cost)
12. [Observability: artifacts, logging, alerting](#12-observability-artifacts-logging-alerting)
13. [Code quality: linting, pre-commit, conventions](#13-code-quality-linting-pre-commit-conventions)
14. [Packaging & deployment (Docker + version pinning)](#14-packaging--deployment-docker--version-pinning)
15. [A production checklist for this project](#15-a-production-checklist-for-this-project)

---

## 1. What "production-ready" means for dbt

A dbt project is "production-ready" when it can run **unattended on a schedule** and you can **trust the
output** and **sleep at night**. Concretely:

| Pillar | Question it answers |
|--------|---------------------|
| **Reliability** | Does it run the same way every time, and recover from failure? |
| **Scalability** | Will it still finish in time when data is 100× bigger? |
| **Correctness** | Are there tests that fail loudly *before* bad data reaches users? |
| **Safety** | Are dev and prod isolated? Are secrets protected? |
| **Observability** | When it breaks at 2 a.m., do you get alerted and know why? |
| **Maintainability** | Can a new teammate understand and change it safely? |

This project has a **solid foundation** (clear layers, `ref()`-based DAG, tests, dockerized Airflow). The
sections below are the gaps between "works on my machine / nightly" and "production."

---

## 2. Environments & targets (dev vs prod)

**Concept.** dbt uses **targets** (in `profiles.yml`) to point the *same code* at *different databases/schemas*.
Dev work must never write to the tables your dashboards read.

**This project today.** Two targets exist:

```1:21:dbt_project/profiles.yml
dbt_project:
  target: dev
  outputs:
    dev:
      type:     postgres
      host:     "{{ env_var('DB_HOST', 'host.docker.internal') }}"
      port:     "{{ env_var('DB_PORT', '5432') | int }}"
      dbname:   "{{ env_var('DB_NAME', 'weather_db') }}"
      user:     "{{ env_var('DB_USER', 'soytry_pipline') }}"
      password: "{{ env_var('DB_PASSWORD', 'soytry_pipline') }}"
      schema:   public
      threads:  4

    prod:
      type:     postgres
      host:     "{{ env_var('DB_HOST') }}"
      port:     "{{ env_var('DB_PORT') | int }}"
      dbname:   "{{ env_var('DB_NAME') }}"
      user:     "{{ env_var('DB_USER') }}"
      password: "{{ env_var('DB_PASSWORD') }}"
      schema:   public
      threads:  8
```

**The problem:** `dev` and `prod` use the **same `DB_*` env vars and the same `schema: public`** — so they
resolve to the *same physical location*. And Airflow's transform DAG runs `dbt run` **without `--target`**,
so scheduled production runs actually use the **`dev`** target.

**Make it production-ready:**

1. **Give each target its own destination.** Point prod at a different database *or* a different base schema so
   dev experiments can't clobber prod tables. A common pattern:
   - Prod writes to `silver` / `gold`.
   - Each developer writes to a personal schema like `dbt_alice_silver` / `dbt_alice_gold`.

   With our custom `generate_schema_name` macro (which uses the custom name verbatim), the cleanest approach is
   to drive the schema prefix from an env var, e.g.:

```yaml
# profiles.yml (illustrative)
dev:
  schema: "{{ env_var('DBT_SCHEMA_PREFIX', 'dev') }}"   # e.g. dbt_alice
prod:
  schema: "prod"
```

   and update the macro to compose `{{ prefix }}_{{ custom_schema_name }}` in non-prod. (Or simply run prod
   against a separate `DB_NAME`.)

2. **Make Airflow explicitly select prod.** Change the scheduled tasks to `dbt build --target prod` so
   production never silently runs the dev target. See §9.

3. **Separate database *roles*.** The prod role that Airflow uses should have the privileges it needs and no
   more; developers use their own role. Never share one superuser.

> **Rule:** *the target is the only thing that should change between environments.* The model code is
> identical; only where it lands differs.

---

## 3. Secrets & credentials management

**Concept.** Credentials must never be committed to git and should be injected at runtime.

**This project today.** Good instincts: `profiles.yml` is **gitignored** and reads `env_var(...)`. But note the
**fallback defaults are real-looking credentials** (`'soytry_pipline'`, `'weather_db'`) baked into the file.

**Make it production-ready:**

- **Remove real fallbacks for prod.** For prod, use `{{ env_var('DB_PASSWORD') }}` with **no default** so dbt
  fails fast if the secret is missing (it already does this for prod — keep it that way; scrub any real values
  from dev defaults too).
- **Inject secrets from a manager, not `.env` files**, in real production: Docker/Compose secrets, AWS Secrets
  Manager/SSM, GCP Secret Manager, Vault, or your orchestrator's secret backend. The `.env` approach is fine
  for local/dev only.
- **Least-privilege DB user.** The dbt prod role needs `CREATE`/`USAGE` on `silver`/`gold` and `SELECT` on
  `bronze`/`config` — not ownership of the whole database.
- **Rotate** credentials and keep them out of logs (avoid `--vars` with secrets; avoid echoing env in CI).

---

## 4. Project structure: staging → intermediate → marts

**Concept.** dbt Labs' recommended layering scales far better than free-form models:

| Layer | Prefix | Purpose | Materialization |
|-------|--------|---------|-----------------|
| **Staging** | `stg_` | 1:1 with a source; rename/cast/clean only. No joins/business logic. | usually `view` |
| **Intermediate** | `int_` | Reusable building blocks; joins & reshaping between staging and marts. | `view`/`ephemeral` |
| **Marts** | `fct_`/`dim_`/`agg_` | Business-facing entities consumed by BI/ML. | `table`/`incremental` |

**This project today** maps loosely onto this with **silver = staging** and **gold = marts**, which is
perfectly reasonable. But `stg_weather` currently does *a lot*: unpacking JSONB **and** joins to `config`
**and** business logic (heat index, comfort level, season) **and** dedup.

**Make it production-ready:**

- **Split responsibilities** as complexity grows:
  - `stg_weather` → only unpack JSONB + cast types + dedup (true staging).
  - `int_weather_enriched` → joins to `config`, adds `weather_description`, `season`, `heat_index_c`,
    `comfort_level` (intermediate business logic).
  - `agg_daily_weather` / `weather_features` → marts (unchanged).
- **Benefits:** each model is smaller and independently testable; enrichment logic is reused by both gold
  models instead of living inside the shared silver view; changes are lower-risk.
- **Keep the folder→config convention** you already use in `dbt_project.yml` (materialization + schema per
  folder). Add an `intermediate/` folder with `+materialized: ephemeral` if you don't want to persist it.

> This is a **refactor for maintainability**, not a correctness fix — do it when the single silver model starts
> feeling overloaded (it's close).

---

## 5. Incremental models — the key to scale

**Concept.** A `table` model is **fully rebuilt every run** (`CREATE TABLE AS SELECT` over *all* history). An
`incremental` model builds once, then only processes **new/changed rows** on subsequent runs. This is the
single biggest lever for keeping run time flat as data grows.

**This project today.** Both gold models are full-refresh `table`s. With 25 provinces × 24 hours × 365 days ×
many years, and the historical backfill from 2020, the daily rebuild reprocesses **all history every night** —
wasteful and slow at scale.

**Make it production-ready — convert `agg_daily_weather` to incremental:**

```sql
{{
    config(
        materialized   = 'incremental',
        unique_key      = ['location_id', 'date'],   -- identifies a row to update
        incremental_strategy = 'delete+insert',
        schema          = 'gold'
    )
}}

WITH silver AS (
    SELECT * FROM {{ ref('stg_weather') }}

    {% if is_incremental() %}
    -- only reprocess recent days on incremental runs.
    -- look back a few days to catch late-arriving readings.
    WHERE observation_date >= (
        SELECT COALESCE(MAX(date), '2000-01-01') - INTERVAL '3 days'
        FROM {{ this }}
    )
    {% endif %}
)
-- ... rest of the aggregation unchanged ...
```

Key pieces to teach:

- **`is_incremental()`** — true only when the table already exists *and* you didn't pass `--full-refresh`. The
  guarded `WHERE` shrinks the scan to recent data.
- **`{{ this }}`** — refers to the model's own existing table, so you can find the latest loaded date.
- **`unique_key`** — lets dbt replace existing rows (idempotent) instead of duplicating them.
- **Lookback window** — re-scan the last N days so **late-arriving** bronze data still gets folded in.
- **`--full-refresh`** — run `dbt build --full-refresh` to rebuild from scratch after logic changes.

> **Caution — `weather_features` is harder.** Its `LAG`/`LEAD`/rolling windows read *many prior days per
> province*, and `target_next_day_*` uses `LEAD` (needs the *next* day). An incremental version must reprocess
> a wide enough window (e.g. last ~95 days to cover the 90-day rolling average, plus handle the +1 day target
> boundary). A common production pattern: keep `weather_features` as a **full-refresh table** but build it from
> an **incremental daily aggregate** so only the cheap daily step scales. Start with `agg_daily_weather`
> incremental; tackle features later.

---

## 6. A real testing strategy (severity, thresholds, store_failures)

**Concept.** In production you tune tests so they (a) catch real problems, (b) don't page you for trivia, and
(c) help you debug fast.

**This project today.** Good coverage of `not_null` / `accepted_values` on keys and classifications. What's
missing is **severity control, thresholds, uniqueness/relationship tests, and failure storage.**

**Make it production-ready:**

- **Severity: `warn` vs `error`.** Not every failure should stop the pipeline. Warn on "suspicious," error on
  "corrupt."

```yaml
- name: temperature_c
  tests:
    - dbt_utils.accepted_range:
        min_value: -20
        max_value: 55
        config:
          severity: warn          # unusual but not necessarily wrong
```

- **Thresholds with `error_if` / `warn_if`.** Tolerate a tiny number of bad rows without failing the whole run:

```yaml
- name: humidity_pct
  tests:
    - dbt_utils.accepted_range:
        min_value: 0
        max_value: 100
        config:
          error_if: ">100"        # fail only if more than 100 rows violate
          warn_if:  ">0"
```

- **Add the tests you're missing:**
  - **`unique`** (or a `dbt_utils.unique_combination_of_columns`) on the grain of each gold table
    (`location_id + date` for `agg_daily_weather`; `location_id + feature_date` for `weather_features`). This is
    critical — it proves your dedup/incremental logic didn't create duplicates.
  - **`relationships`** from `stg_weather.location_id` → `config.locations.id` to guarantee referential
    integrity (your loader enforces this app-side; a dbt test verifies it end-to-end).
- **`store_failures: true`** — persist failing rows to a table so you can inspect them instead of just seeing a
  count. Set it project-wide in `dbt_project.yml`:

```yaml
# dbt_project.yml
tests:
  dbt_project:
    +store_failures: true
    +schema: dbt_test_failures
```

- **Test sources too** (you already do on `bronze`). Consider a **freshness** check (see §7).

> **Production mindset:** a test that never fails teaches you nothing; a test that always warns gets ignored.
> Calibrate severity so red always means "act now."

---

## 7. Source freshness & SLAs

**Concept.** `dbt source freshness` checks *how old* your raw data is, turning "is the pipeline behind?" into a
first-class, testable signal.

**This project today.** The `weather_transform` DAG has a **Python** `check_bronze_freshness` task (fails if no
rows in 25h). That works, but dbt can do this natively and consistently.

**Make it production-ready** — add freshness to the `bronze.weather` source:

```yaml
# models/sources.yml (illustrative addition)
sources:
  - name: bronze
    schema: bronze
    tables:
      - name: weather
        loaded_at_field: ingested_at
        freshness:
          warn_after:  {count: 2, period: hour}
          error_after: {count: 25, period: hour}
```

Then in orchestration:

```bash
dbt source freshness            # emits pass/warn/error per source
```

Benefits over the bespoke Python check: it's declarative, shows up in `dbt docs`, and integrates with
`dbt build --select source_status:fresher+` (only rebuild what has new data). You can keep the Python gate too,
but the dbt-native version is the production standard.

---

## 8. Data contracts & model versions

**Concept (dbt 1.5+).** A **contract** enforces a model's output schema (column names + data types) at build
time — dbt fails the run if the model would produce a different shape. This protects downstream consumers
(BI, the ML training step, an API) from surprise breaking changes.

**This project today.** `weather_features` is consumed by the (future) ML step, which depends on exact column
names like `target_next_day_temp`. There's no contract, so a careless edit could silently rename/drop a target.

**Make it production-ready** — add a contract to the ML-facing model:

```yaml
# models/gold/weather_features.yml (illustrative)
models:
  - name: weather_features
    config:
      contract:
        enforced: true
    columns:
      - name: location_id
        data_type: integer
        constraints: [{type: not_null}]
      - name: feature_date
        data_type: date
        constraints: [{type: not_null}]
      - name: target_next_day_temp
        data_type: numeric
      - name: target_will_rain_tomorrow
        data_type: integer
```

Now dbt refuses to build `weather_features` if a change would alter these columns' presence or type — a
guardrail for the ML contract. Pair with **model `versions`** if you ever need to evolve the schema while
keeping old consumers working.

> **Note on version:** contracts require dbt ≥ 1.5. This project pins `dbt-postgres==1.7.0`, so contracts are
> available. Upgrading dbt is itself a production task — pin versions and test upgrades in CI (§14).

---

## 9. Orchestration: `dbt build`, selectors, tags, retries

**Concept.** *How* you invoke dbt on a schedule matters as much as the models.

**This project today** runs two separate steps:

```121:136:dags/weather_transform_dag.py
    task_dbt_run = BashOperator(
        task_id      = "dbt_run",
        bash_command = (
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir ~/.dbt"
        ),
    )

    # ── Task 3: dbt test — validates all models
    task_dbt_test = BashOperator(
        task_id      = "dbt_test",
        bash_command = (
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir ~/.dbt"
        ),
    )
```

Two production issues: (a) `run` then `test` means **all models build even if an early one is bad, and tests
only run after everything is built**; (b) no explicit `--target`, so it uses `dev`.

**Make it production-ready:**

- **Prefer `dbt build`.** It runs each node's model **and its tests together, in DAG order**, and *stops
  descendants of a failed node*. So a bad `stg_weather` won't waste time building gold on top of garbage.

```bash
dbt build --target prod --profiles-dir ~/.dbt --fail-fast
```

- **Be explicit about target and profiles-dir** (prod, not dev).
- **`dbt deps` in the pipeline** (or bake into the image, §14) so packages are present.
- **Use selectors/tags** for partial or tiered runs. Tag models and run subsets:

```yaml
# in a model config
{{ config(tags=['ml']) }}
```
```bash
dbt build --select tag:ml            # rebuild only ML-facing models
dbt build --select stg_weather+      # a model and everything downstream
```

- **Retries:** your DAG sets `retries: 2` — good. Make sure dbt commands are **idempotent** (they are:
  full-refresh tables and `unique_key` incrementals can safely re-run).
- **One dbt invocation vs many:** running a single `dbt build` (instead of run+test as separate Bash tasks)
  also means dbt computes the DAG once and parallelizes across `threads`.

---

## 10. CI/CD & Slim CI (state + defer)

**Concept.** Every change to models should be **automatically tested before merge**, and you should only
rebuild **what changed** — not the whole warehouse — on each PR.

**This project today.** No CI is configured for dbt.

**Make it production-ready:**

- **Add a CI job** (GitHub Actions/GitLab CI) that, on every PR:
  1. `pip install dbt-postgres==1.7.0`
  2. `dbt deps`
  3. `dbt build --target ci` against a **CI/staging database** (never prod).
- **Slim CI with state comparison** — build only modified models and their children by comparing to the last
  production run's artifacts (`manifest.json`):

```bash
# using the previous production manifest stored in ./prod-artifacts
dbt build --select state:modified+ --defer --state ./prod-artifacts --target ci
```

  - **`state:modified+`** — only models that changed (and their descendants).
  - **`--defer` + `--state`** — unchanged upstream models are *read from prod* instead of rebuilt, so a PR
    doesn't have to rebuild the whole DAG.
- **Publish artifacts.** After each successful prod run, save `target/manifest.json` and `run_results.json`
  somewhere durable so CI and observability can use them.

> Slim CI is how teams keep dbt PRs fast (minutes, not hours) even with large projects.

---

## 11. Performance & cost

**Concept.** dbt performance = SQL performance + smart materialization + parallelism. On Postgres (this
project) the main levers are:

- **Materialization choice** — views for cheap/always-fresh, tables for reused heavy queries, **incremental**
  for large history (§5). This is your #1 lever.
- **Indexes.** dbt-postgres lets you declare indexes in `config()`. You already do on silver:

```1:7:dbt_project/models/silver/stg_weather.sql
{{ config(
    materialized = 'view',
    indexes = [
        {'columns': ['location_id', 'observation_date']},
        {'columns': ['observation_year']},
    ]
) }}
```

  ⚠️ **But `stg_weather` is a `view`** — indexes only apply to *tables/materialized objects*, so these index
  hints don't do anything on a view. Either (a) move these indexes to the **gold tables** that are actually
  queried, or (b) rely on the **partitioning + GIN index** already on `bronze.weather`. This is a real,
  fixable production detail.
- **Partitioning.** Your bronze table is **partitioned by year** — great; queries filtered by date can prune
  partitions. Keep the yearly-partition automation working (`get_or_create_partition`).
- **`threads`** in `profiles.yml` — dbt builds independent models in parallel up to this number. Tune to your
  Postgres CPU/connection limits (dev=4, prod=8 here is reasonable; watch connection pool).
- **Avoid rebuilding everything nightly** once data is large — this is exactly what incremental solves.

---

## 12. Observability: artifacts, logging, alerting

**Concept.** In production you must know *when* a run failed, *what* failed, and *how long* things take —
without reading raw logs.

**This project today.** Airflow captures task logs and the `verify_gold_tables` task prints row counts. That's
a start, but there's no structured dbt-level monitoring or alerting.

**Make it production-ready:**

- **Use dbt artifacts.** Every invocation writes to `target/`:
  - `run_results.json` — status, timing, and rows for each node.
  - `manifest.json` — the full project graph (also used for Slim CI, §10).
  - `sources.json` — freshness results.
  Ship these to storage and/or parse them to build dashboards ("slowest models," "test failure trends").
- **Alerting.** Wire failures to Slack/email/PagerDuty. In Airflow, set `email_on_failure`/`on_failure_callback`
  (currently `email_on_failure: False`). At minimum, alert on: dbt build failure, any `error`-severity test,
  and source-freshness `error`.
- **`store_failures`** (from §6) makes failed-row inspection possible after the fact.
- **Freshness + row-count anomaly checks** catch "the pipeline ran green but produced 0 rows" — a classic
  silent failure. Your `check_bronze_freshness` guards the input; add an output check that gold row counts are
  within an expected range.

---

## 13. Code quality: linting, pre-commit, conventions

**Concept.** Consistent, reviewed SQL is easier to maintain and less bug-prone.

**Make it production-ready:**

- **SQL linting** with **SQLFluff** (dbt-aware). Enforces style (casing, indentation) and can catch mistakes:

```bash
pip install sqlfluff sqlfluff-templater-dbt
sqlfluff lint dbt_project/models --dialect postgres --templater dbt
```

- **`pre-commit` hooks** to run SQLFluff, `dbt parse`, and YAML checks before every commit.
- **Conventions to standardize** (write them in a CONTRIBUTING doc):
  - Model naming: `stg_` / `int_` / `fct_`/`dim_`/`agg_` (§4).
  - One `SELECT` per model; CTE pipeline style (import → logic → final select) — you already follow this.
  - Every model has a `.yml` with a description and at least a uniqueness + not-null test on its grain.
  - Always `ref()`/`source()`, never hard-coded table names.
- **`dbt parse`** in CI catches broken refs/Jinja before they hit the warehouse.

---

## 14. Packaging & deployment (Docker + version pinning)

**Concept.** Reproducible builds: the same dbt + adapter + package versions everywhere.

**This project today.** `dbt-postgres==1.7.0` is pinned in `requirements.txt` and installed into the Airflow
image; `~/.dbt` is mounted for `profiles.yml`; packages are pinned via `packages.yml` + `package-lock.yml`.
That's a good baseline.

**Make it production-ready:**

- **Run `dbt deps` at image build time** (not at runtime) so the container is self-contained and startup is
  fast/offline-safe. Add to `docker/Dockerfile`:

```dockerfile
# after copying the project
RUN cd /opt/airflow/project/dbt_project && dbt deps
```

- **Commit `package-lock.yml`** (you have it) so everyone resolves identical package versions.
- **Pin everything**: dbt-core/adapter (done), packages (done), and the Python base image
  (`apache/airflow:2.7.3-python3.11`, done). Upgrades become deliberate, tested changes.
- **Bake `profiles.yml` from env** in prod rather than mounting a host file, or mount it read-only from a
  secret. Relying on `~/.dbt` from the host is fine for local, less so for prod hosts.
- **Consider a dedicated dbt runner** (a `dbt` container/task) rather than running dbt inside the general
  Airflow worker, for cleaner resource isolation — optional but common at scale.

---

## 15. A production checklist for this project

Concrete, prioritized actions for `env-data-pipeline`. **P1 = do first.**

| # | Priority | Action | Section |
|---|----------|--------|---------|
| 1 | **P1** | Make Airflow run **`dbt build --target prod`** (not implicit `dev` run+test) | §2, §9 |
| 2 | **P1** | Truly separate **dev vs prod** destinations (schema prefix or separate DB) | §2 |
| 3 | **P1** | Add **uniqueness tests** on gold grains (`location_id+date`, `location_id+feature_date`) | §6 |
| 4 | **P1** | Scrub real-looking credential fallbacks from `profiles.yml` dev defaults | §3 |
| 5 | P2 | Convert **`agg_daily_weather` to incremental** with a lookback window | §5 |
| 6 | P2 | Add **`dbt source freshness`** on `bronze.weather` | §7 |
| 7 | P2 | Move/remove the **no-op index hints** on the `stg_weather` view | §11 |
| 8 | P2 | Add **CI** (`dbt build` on a CI DB; Slim CI with state+defer) | §10 |
| 9 | P2 | Add **failure alerting** (Slack/email) + ship dbt artifacts | §12 |
| 10 | P3 | Add a **contract** to `weather_features` (protect ML columns) | §8 |
| 11 | P3 | Split silver into **staging + intermediate** as logic grows | §4 |
| 12 | P3 | Add **SQLFluff + pre-commit**; write CONTRIBUTING conventions | §13 |
| 13 | P3 | Run **`dbt deps` at image build**; harden profiles delivery | §14 |

> **The 20% that gives 80%:** explicit prod target (#1), dev/prod isolation (#2), and uniqueness tests (#3).
> Those three alone move this project from "nightly job that usually works" to "trustworthy production
> pipeline."

---

*Cross-reference: fundamentals in `docs/LEARN_DBT.md`; overall architecture in `docs/TECHNICAL.md`.
When in doubt, the code is the source of truth — verify line references against the current files.*
