# Learn Apache Airflow — A Comprehensive Guide (using env-data-pipeline)

> **Who this is for:** students/engineers who know **Python** and want to learn Apache Airflow well enough to
> build **project-standard, production-quality** pipelines — not just toy DAGs.
>
> **How this guide works:** it teaches every core concept, then shows it **in this project's real DAGs**
> (`dags/weather_ingest_dag.py`, `weather_transform_dag.py`, `weather_ai_retrain_dag.py`), and finishes with
> **standards, best practices, and a production checklist**. Version note: this project runs **Airflow 2.7.3**
> with the **CeleryExecutor**; concepts are explained for 2.x with notes where 3.x differs.

---

## Table of Contents

1. [What Airflow is (and is not)](#1-what-airflow-is-and-is-not)
2. [The architecture: components that make Airflow run](#2-the-architecture-components-that-make-airflow-run)
3. [This project's deployment (Celery + Redis + Postgres)](#3-this-projects-deployment-celery--redis--postgres)
4. [Core concepts: DAG, Task, Operator, Executor](#4-core-concepts-dag-task-operator-executor)
5. [Anatomy of a DAG (weather_ingest)](#5-anatomy-of-a-dag-weather_ingest)
6. [Operators & Sensors](#6-operators--sensors)
7. [Scheduling: start_date, schedule, catchup, data intervals](#7-scheduling-start_date-schedule-catchup-data-intervals)
8. [Dependencies & control flow](#8-dependencies--control-flow)
9. [Dynamic task generation](#9-dynamic-task-generation)
10. [XComs — passing data between tasks](#10-xcoms--passing-data-between-tasks)
11. [Pools, concurrency & priorities](#11-pools-concurrency--priorities)
12. [Retries, trigger rules & failure handling](#12-retries-trigger-rules--failure-handling)
13. [Connections & Variables (secrets & config)](#13-connections--variables-secrets--config)
14. [The TaskFlow API (modern authoring)](#14-the-taskflow-api-modern-authoring)
15. [Data-aware scheduling (Datasets)](#15-data-aware-scheduling-datasets)
16. [Testing & local development](#16-testing--local-development)
17. [Production standards & best practices](#17-production-standards--best-practices)
18. [Known issues & improvements in this project](#18-known-issues--improvements-in-this-project)
19. [Glossary](#19-glossary)
20. [Where to go next](#20-where-to-go-next)

---

## 1. What Airflow is (and is not)

**Apache Airflow** is a platform to **author, schedule, and monitor workflows as code**. You describe a
workflow as a **DAG** (Directed Acyclic Graph) of tasks in Python; Airflow decides *when* to run each task,
runs them in the right order, retries failures, and gives you a UI to observe everything.

**The one-sentence definition:** *Airflow is a Python-based orchestrator that runs your tasks in dependency
order, on a schedule, with retries and monitoring.*

**Airflow IS:**
- An **orchestrator** — it decides *when* and *in what order* work runs, and tracks its state.
- **Workflow-as-code** — pipelines are Python files, so they're version-controlled, reviewable, testable.
- A **scheduler + UI + metadata store** for observing and re-running work.

**Airflow is NOT:**
- ❌ A data processing engine. Airflow shouldn't crunch big data *itself* — it should **trigger** the systems
  that do (a database, dbt, Spark, an API). In this project, Airflow triggers our **Python pipeline** and
  **dbt**; Postgres and Open-Meteo do the actual work.
- ❌ A streaming/real-time tool. It's built for **batch**, scheduled workloads (hourly, daily, etc.).
- ❌ A place to store data between runs (use a database/warehouse; XCom is for *small* metadata only).

> **Golden rule you'll see repeated:** *Airflow orchestrates; it does not compute.* Keep heavy logic in the
> systems Airflow calls, not in the DAG.

---

## 2. The architecture: components that make Airflow run

Airflow is several cooperating processes, not one program:

```mermaid
flowchart TB
    subgraph AUTH["You"]
        DAGS["DAG files (.py)<br/>in dags/"]
    end

    SCHED["Scheduler<br/>(reads DAGs, decides what runs)"]
    META[("Metadata DB<br/>Postgres — state of everything")]
    EXEC["Executor<br/>(how/where tasks run)"]
    QUEUE["Queue / Broker<br/>Redis (Celery)"]
    WORK["Worker(s)<br/>(actually run tasks)"]
    WEB["Webserver<br/>(UI on :8080)"]
    TRIG["Triggerer<br/>(async waits for deferrable ops)"]

    DAGS --> SCHED
    SCHED <--> META
    SCHED --> EXEC --> QUEUE --> WORK
    WORK <--> META
    WEB <--> META
    TRIG <--> META
```

| Component | Job |
|-----------|-----|
| **Scheduler** | Parses DAG files, creates DAG runs and task instances, and submits ready tasks to the executor. The brain. |
| **Metadata Database** | Stores the state of every DAG, run, task instance, connection, variable, XCom. The single source of truth. |
| **Executor** | Strategy for *where* tasks run (Sequential, Local, **Celery**, Kubernetes). Configured, not coded. |
| **Workers** | Processes that execute task code. With Celery, multiple workers pull tasks from a queue. |
| **Queue/Broker** | Message broker (here **Redis**) that hands tasks from scheduler → workers (Celery only). |
| **Webserver** | The UI: view DAGs, runs, logs, trigger/clear tasks, manage connections. |
| **Triggerer** | Runs async **triggers** for *deferrable* operators so long waits don't tie up workers (§6). |

**Key idea for beginners:** the **DAG file just *defines* the workflow.** The scheduler and workers *execute*
it later, possibly on a different machine. That's why "it worked when I ran the script" isn't the same as "the
DAG works."

---

## 3. This project's deployment (Celery + Redis + Postgres)

This repo ships a full Airflow stack in `docker-compose.yml`. The services map exactly onto §2:

| Service (container) | Airflow role |
|---------------------|--------------|
| `postgres-airflow` (Postgres 15) | **Metadata DB** (separate from the *pipeline* weather DB) |
| `redis` (Redis 7.2) | **Celery broker** (the queue) |
| `airflow-scheduler` | **Scheduler** |
| `airflow-worker` | **Celery worker** |
| `airflow-webserver` | **UI** on `http://localhost:8080` (admin/admin) |
| `airflow-init` | one-shot setup: `db upgrade`, create admin user, create the `weather_ingest_pool` |

Shared settings live in a YAML anchor so all Airflow containers get them:

```9:21:docker-compose.yml
  environment:
    &airflow-common-env
    # airflow core
    AIRFLOW__CORE__EXECUTOR: CeleryExecutor
    AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres-airflow:5432/airflow
    AIRFLOW__CORE__FERNET_KEY: ''
    AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'true'
    AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
    AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG: 1

    # celery
    AIRFLOW__CELERY__RESULT_BACKEND: db+postgresql://airflow:airflow@postgres-airflow:5432/airflow
    AIRFLOW__CELERY__BROKER_URL: redis://:@redis:6379/0
```

Things worth learning from this config:

- **`AIRFLOW__CORE__EXECUTOR: CeleryExecutor`** — tasks run on separate worker processes pulled from Redis.
  This is the multi-worker, horizontally-scalable setup (vs. `LocalExecutor` for single-machine).
- **Any setting can be an env var**: the pattern `AIRFLOW__<SECTION>__<KEY>` maps to `airflow.cfg`. Great for
  containers.
- **`DAGS_ARE_PAUSED_AT_CREATION: 'true'`** — new DAGs start **paused**; you unpause them in the UI. Prevents a
  brand-new DAG from immediately backfilling.
- **`LOAD_EXAMPLES: 'false'`** — don't clutter the UI with tutorial DAGs.
- **Two databases**: the Airflow **metadata** DB (`postgres-airflow`) is deliberately separate from your
  **pipeline** DB (the weather data). Never mix them.
- **Volume mounts** put your `dags/`, `logs/`, `plugins/`, the whole project, and `~/.dbt` into the containers,
  so editing a DAG on your host updates it inside Airflow.

> **Try it:** `docker compose up -d`, open `http://localhost:8080`, log in `admin/admin`, and you'll see the
> three weather DAGs (paused). Unpause `weather_ingest` to start.

---

## 4. Core concepts: DAG, Task, Operator, Executor

Four words you must internalize:

- **DAG** — the *whole workflow*: a set of tasks + their dependencies + a schedule. "Directed Acyclic" means
  dependencies flow one way and never loop.
- **Task** — a single *node* in the DAG; one unit of work (e.g. "ingest Phnom Penh").
- **Operator** — a *template* for a task. You instantiate an operator in your DAG and that instance becomes a
  task. E.g. `PythonOperator` (run a Python function), `BashOperator` (run a shell command).
- **Task Instance** — a specific *run* of a task for a specific date/data-interval. It has **state**
  (`running`, `success`, `failed`, `up_for_retry`, `skipped`, …). This is what you see as a colored square in
  the UI grid.

Relationship: **Operator (template) → instantiate in a DAG → Task (node) → runs as → Task Instance (state).**

The **Executor** is orthogonal: it decides *where/how* task instances physically run. Your DAG code doesn't
change if you switch from Celery to Kubernetes — that's a config change (§2/§3).

---

## 5. Anatomy of a DAG (weather_ingest)

Let's dissect a real DAG top to bottom. Start with the file header:

```1:9:dags/weather_ingest_dag.py
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from utils.db import get_connection
```

- The `sys.path.insert(...)` lets the DAG import this project's own packages (`utils`, `pipeline`) — necessary
  because DAGs run from Airflow's `dags/` folder but need the project root on the path.
- We import the `DAG` class and the `PythonOperator`.

**Default args** — settings applied to every task in the DAG:

```13:19:dags/weather_ingest_dag.py
default_args = {
    "owner":             "pipeline_user",
    "retries":           3,
    "retry_delay":       timedelta(minutes=5),
    "email_on_failure":  False,
    "depends_on_past":   False,
}
```

- `retries: 3` + `retry_delay: 5 min` → each task auto-retries up to 3 times, 5 minutes apart, before failing.
- `depends_on_past: False` → a run doesn't wait for the previous run's same task to succeed.

**The DAG definition** (a context manager):

```160:169:dags/weather_ingest_dag.py
with DAG(
    dag_id            = "weather_ingest",
    description       = "Hourly weather ingestion — 25 provinces → bronze.weather",
    default_args      = default_args,
    start_date        = datetime(2026, 7, 1),
    schedule_interval = "@hourly",
    catchup           = False,
    tags              = ["weather", "ingest", "bronze"],
    max_active_runs   = 1,       # only one run at a time
) as dag:
```

Every argument here is a lesson:
- **`dag_id`** — unique name shown in the UI.
- **`start_date`** — the first data interval Airflow considers (see §7). Use a *fixed* datetime, never
  `datetime.now()`.
- **`schedule_interval="@hourly"`** — run once per hour. (In Airflow 3.x the param is renamed `schedule`.)
- **`catchup=False`** — don't backfill all missed intervals since `start_date` (crucial — see §7).
- **`tags`** — UI filtering.
- **`max_active_runs=1`** — never run two `weather_ingest` runs simultaneously.

The **task body** — a plain Python function wrapped by `PythonOperator` (covered in §9), and finally the
**dependency wiring** (§8):

```202:202:dags/weather_ingest_dag.py
    province_tasks >> health_check
```

That's the whole shape of a DAG: imports → default_args → `with DAG(...)` → define tasks → set dependencies.

---

## 6. Operators & Sensors

An **operator** is a reusable task template. Categories:

- **Action operators** — *do* something. `PythonOperator`, `BashOperator`, `EmailOperator`, and provider
  operators like `PostgresOperator`, `SparkSubmitOperator`.
- **Sensors** — *wait* for something to be true (a file to land, a partition to exist, a time to pass).
- **Transfer operators** — move data between systems.

**This project uses two operators:**

**`PythonOperator`** — runs a Python callable. From the ingest DAG's task creation:

```177:187:dags/weather_ingest_dag.py
        task = PythonOperator(
            task_id         = f"ingest_{loc['name'].lower().replace(' ', '_')}",
            python_callable = ingest_province,
            op_kwargs       = {
                "location_id":   loc["location_id"],
                "location_name": loc["name"],
                "source_id":     1,
            },
            pool            = "weather_ingest_pool",  # max 10 at a time
            pool_slots      = 1,
        )
```

- `python_callable` = the function to run; `op_kwargs` = arguments passed to it.

**`BashOperator`** — runs a shell command. The transform DAG uses it to call dbt:

```121:127:dags/weather_transform_dag.py
    task_dbt_run = BashOperator(
        task_id      = "dbt_run",
        bash_command = (
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir ~/.dbt"
        ),
    )
```

**Sensors (not yet used here, but you should know them).** A sensor waits. Example: instead of the Python
`check_bronze_freshness` function, you could use a `SqlSensor` that polls until bronze has fresh rows.
Sensors have two efficiency modes:
- **`mode='reschedule'`** — frees the worker slot between pokes (good for long waits).
- **`deferrable=True`** — hands the wait to the **triggerer** process, freeing the worker entirely
  (best for long waits; requires a triggerer running). Deferral only works on class-based operators, not on
  `PythonOperator`/TaskFlow functions.

> **When to use a provider operator vs `PythonOperator`?** Prefer a purpose-built operator (e.g.
> `PostgresOperator`) when one exists — it's tested and handles connections/retries. Use `PythonOperator` for
> custom logic (as this project does, calling its own `pipeline` package).

---

## 7. Scheduling: start_date, schedule, catchup, data intervals

This is the most misunderstood part of Airflow — learn it carefully.

**Data interval mindset.** Airflow schedules **batches over time windows**, not "every N minutes from now." A
DAG run is associated with a **data interval** `[start, end)`. For `@hourly`, a run covers one hour and is
triggered **at the end** of that hour. So the run "for 13:00–14:00" starts *at 14:00*. The
`logical_date`/`data_interval_start` is a property of the run, not "wall-clock now."

**`start_date`.** The beginning of the first interval. **Use a static datetime** (`datetime(2026,7,1)`) —
never `datetime.now()`, which makes the start date move every parse and breaks scheduling.

**`schedule` (a.k.a. `schedule_interval` in 2.x).** Accepts:
- presets: `@hourly`, `@daily`, `@weekly`, `@once`, `None` (manual only)
- cron: `"0 0 * * *"` (midnight daily — the transform DAG), `"0 0 1 */3 *"` (quarterly — the retrain DAG)
- a `timedelta`

Our three DAGs:
| DAG | schedule | meaning |
|-----|----------|---------|
| `weather_ingest` | `@hourly` | every hour |
| `weather_transform` | `0 0 * * *` | daily at midnight |
| `weather_ai_retrain` | `0 0 1 */3 *` | 00:00 on day 1 of Jan/Apr/Jul/Oct |

**`catchup` — the beginner footgun.** If `catchup=True` and your `start_date` is far in the past, unpausing the
DAG makes Airflow **immediately run every missed interval** (potentially hundreds of runs!). All three DAGs
here correctly set **`catchup=False`**, so only future intervals run. Turn catchup on *only* when you truly
want historical backfill.

**`max_active_runs=1`** ensures runs don't overlap — important for the hourly ingest so two runs don't hammer
the API or double-insert.

> **Templating with the data interval.** Bash/SQL operators can use Jinja macros like `{{ ds }}` (the run's
> date) or `{{ data_interval_start }}`. This is how you write **idempotent** tasks that process *their own*
> window rather than "now."

---

## 8. Dependencies & control flow

You declare tasks, then wire their order with **bitshift operators**:

```python
a >> b        # a runs before b (a is upstream of b)
a << b        # b runs before a
a >> [b, c]   # fan-out: b and c both depend on a
[b, c] >> d   # fan-in: d waits for both b and c
```

The transform DAG is a clean linear chain:

```146:146:dags/weather_transform_dag.py
    task_check_bronze >> task_dbt_run >> task_dbt_test >> task_verify_gold
```

The ingest DAG fans **in**: all province tasks must reach a terminal state before the health check:

```202:202:dags/weather_ingest_dag.py
    province_tasks >> health_check
```

(`province_tasks` is a Python list of task objects; `list >> task` sets every item as upstream of `task`.)

The retrain DAG is again linear:

```155:155:dags/weather_ai_retrain_dag.py
    task_check_data >> task_retrain >> task_log
```

> **"Acyclic" matters:** you can't create `a >> b >> a`. Dependencies must form a DAG or Airflow rejects it.

---

## 9. Dynamic task generation

A powerful pattern: **generate tasks in a loop** so the DAG's shape adapts to data. The ingest DAG creates
**one task per province** by querying config at parse time:

```171:188:dags/weather_ingest_dag.py
    # load locations at DAG parse time
    locations = load_active_locations(source_id=1)

    # ── dynamically create one task per province
    province_tasks = []
    for loc in locations:
        task = PythonOperator(
            task_id         = f"ingest_{loc['name'].lower().replace(' ', '_')}",
            python_callable = ingest_province,
            op_kwargs       = {
                "location_id":   loc["location_id"],
                "location_name": loc["name"],
                "source_id":     1,
            },
            pool            = "weather_ingest_pool",
            pool_slots      = 1,
        )
        province_tasks.append(task)
```

Why this is nice: add a province to `config.locations` and the DAG automatically grows a task for it — no code
change. Each province is an **independent, separately-retryable task**, so one failing province doesn't block
the other 24.

**⚠️ The production caveat (important).** This loop calls `load_active_locations()` — a **database query** —
at **parse time**. The scheduler parses DAG files **constantly** (every few seconds), so this hits the DB on
every parse. That's a classic Airflow anti-pattern: **heavy work in top-level DAG code slows the scheduler and
can make the DAG fragile** (if the DB is down, the DAG fails to import).

Two better approaches:
1. **Dynamic Task Mapping** (Airflow 2.3+): `.expand()` generates mapped task instances at *run time* from an
   upstream task's output, keeping parse-time cheap:

```python
@task
def get_locations():
    return load_active_locations(1)      # runs at RUN time, not parse time

@task(pool="weather_ingest_pool")
def ingest(loc):
    ...

ingest.expand(loc=get_locations())
```

2. At minimum, make parse-time queries cheap/cached and defensive (timeout + fallback), so a DB blip doesn't
   break DAG import.

> **Rule:** *top-level DAG code runs on every parse.* Keep it light. Put real work inside task functions.

---

## 10. XComs — passing data between tasks

**XCom** ("cross-communication") lets one task push a small value that another task pulls. It's stored in the
metadata DB — so it's for **small metadata only** (counts, IDs, flags), **never** big datasets.

**Push:** returning a value from a `PythonOperator` callable auto-pushes it. The ingest task returns per-province
stats:

```99:104:dags/weather_ingest_dag.py
        # push metadata to XCom for health check task
        return {
            "location":    location_name,
            "extracted":   result.total_rows,
            "rejected":    result.rejected_rows,
            "inserted":    inserted,
        }
```

**Pull:** the health-check task reads each province task's XCom:

```123:129:dags/weather_ingest_dag.py
    for task_id in task_ids:
        meta = ti.xcom_pull(task_ids=task_id)
        if meta:
            results.append(meta)
            total_inserted += meta.get("inserted", 0)
        else:
            failed_locs.append(task_id)
```

It then computes a success rate and **fails the whole DAG if < 80% of provinces succeeded** — a nice
data-quality gate implemented purely with XCom + a trigger rule (next section).

> **XCom limits:** the metadata DB backend caps XCom size (and it's slow for big payloads). To pass large data,
> write it to storage/DB and pass a **path or key** via XCom, not the data itself.

---

## 11. Pools, concurrency & priorities

**Pools** limit how many tasks can run concurrently against a shared resource. This project protects the
Open-Meteo API with a pool of **10 slots**, created at init:

```146:146:docker-compose.yml
        airflow pools set weather_ingest_pool 10 "Open-Meteo API pool"
```

Every province task joins that pool (`pool="weather_ingest_pool"`, `pool_slots=1`), so even though there are 25
tasks, **at most 10 hit the API at once** — polite to the API and avoids rate limits.

Other concurrency knobs to know:
- **`max_active_runs`** (per DAG) — overlapping *runs* (set to 1 here).
- **`max_active_tasks`** (per DAG) — concurrent *tasks* within a DAG.
- **parallelism / worker concurrency** — cluster-wide limits (Celery worker settings).
- **`priority_weight`** — which queued task runs first when slots are scarce.

> **Design tip:** pools are the right tool whenever tasks share a fragile external resource (an API, a small DB
> connection pool, a licensed system).

---

## 12. Retries, trigger rules & failure handling

**Retries** (set in `default_args`) auto-recover from transient failures. Per DAG here:
- `weather_ingest`: 3 retries, 5-min delay
- `weather_transform`: 2 retries, 10-min delay
- `weather_ai_retrain`: 1 retry, 30-min delay

**Trigger rules** decide *when a task runs based on its upstream states*. Default is `all_success` (run only if
all upstream succeeded). The health-check task overrides this so it runs **even if some provinces failed**:

```191:199:dags/weather_ingest_dag.py
    health_check = PythonOperator(
        task_id         = "health_check",
        python_callable = run_health_check,
        op_kwargs       = {
            "task_ids": [t.task_id for t in province_tasks]
        },
        provide_context = True,
        trigger_rule    = "all_done",  # runs even if some provinces failed
    )
```

`all_done` = "run after every upstream finished, regardless of success/failure." That's exactly what a health
check needs — it must inspect failures, so it can't require success. Other useful rules: `all_failed`,
`one_success`, `none_failed`, `none_failed_min_one_success`.

**Other failure-handling tools:**
- **`on_failure_callback` / `on_success_callback`** — run code (e.g. send Slack alert) on state change.
- **`email_on_failure`** — currently `False` everywhere here; wire up alerting for production (§17).
- **SLAs / timeouts** — `execution_timeout` fails a task that runs too long; SLAs flag lateness.

---

## 13. Connections & Variables (secrets & config)

**Connections** store credentials for external systems (databases, APIs, cloud) in the metadata DB (encrypted
via the **Fernet key**), referenced by a `conn_id`. **Variables** store reusable config key/values.

**This project's approach (and its trade-off).** Instead of Airflow Connections, the DAGs call the project's
own `utils.db.get_connection()`, which reads `DB_*` **environment variables** injected by docker-compose:

```8:8:dags/weather_ingest_dag.py
from utils.db import get_connection
```

This is pragmatic (one connection method for both the pipeline scripts and the DAGs), but the **production
standard** is usually to define an Airflow **Connection** (`postgres_default`) and let operators/hooks use it,
so credentials are centrally managed, encrypted, and rotatable from the UI/API. Notice also:

```14:14:docker-compose.yml
    AIRFLOW__CORE__FERNET_KEY: ''
```

An **empty Fernet key** means connection secrets aren't encrypted at rest — fine for local dev, **not for
production**. Generate and set a real Fernet key before storing any secrets in Airflow.

> **Rule:** secrets belong in Connections/Variables (or a secrets backend like Vault/AWS Secrets Manager),
> **never hard-coded in DAG files**, and always with a real Fernet key in prod.

---

## 14. The TaskFlow API (modern authoring)

Airflow 2.x introduced the **TaskFlow API** — decorators that make Python-heavy DAGs cleaner. Instead of
`PythonOperator` + manual XComs, you write functions:

```python
from airflow.decorators import dag, task
from datetime import datetime

@dag(schedule="@hourly", start_date=datetime(2026, 7, 1), catchup=False, tags=["weather"])
def weather_ingest_taskflow():

    @task
    def get_locations():
        from dags... import load_active_locations
        return load_active_locations(1)

    @task(pool="weather_ingest_pool")
    def ingest(location: dict):
        # extract → validate → load for one province
        return {"location": location["name"], "inserted": 1}

    @task(trigger_rule="all_done")
    def health_check(results: list):
        ...

    results = ingest.expand(location=get_locations())
    health_check(results)

weather_ingest_taskflow()
```

What TaskFlow gives you vs. the current classic style:
- **Automatic XComs** — return a value, pass it directly to another `@task`; no manual `xcom_pull`.
- **Cleaner dependencies** — calling `b(a())` sets `a >> b` implicitly.
- **`.expand()`** for dynamic task mapping at run time (fixes the parse-time DB query from §9).

This project uses the **classic operator style** (perfectly valid and explicit). TaskFlow is the modern
recommendation for new, Python-centric DAGs. Note you can **mix** both styles in one project.

---

## 15. Data-aware scheduling (Datasets)

Beyond time-based schedules, Airflow 2.4+ supports **Datasets**: a DAG can run **because another DAG produced
data**, not because the clock struck a time.

In this project, `weather_transform` runs at midnight and *assumes* `weather_ingest` has been filling bronze.
A more event-driven design would let transform trigger **when ingestion updates bronze**:

```python
from airflow.datasets import Dataset
bronze_weather = Dataset("postgres://.../bronze.weather")

# producer DAG (ingest): declare it writes the dataset
@task(outlets=[bronze_weather])
def ingest(...): ...

# consumer DAG (transform): schedule on the dataset instead of cron
@dag(schedule=[bronze_weather], ...)
def weather_transform(): ...
```

Now transform runs **after** ingestion updates bronze — no fragile "hope it finished by midnight" timing. This
is the modern way to express **cross-DAG data dependencies** (an alternative to `TriggerDagRunOperator` or
`ExternalTaskSensor`).

> For this project, datasets would elegantly replace the `check_bronze_freshness` guard with a real
> producer→consumer link.

---

## 16. Testing & local development

Production-standard DAGs are tested like any other code.

- **DAG integrity / import test** — the cheapest, highest-value test. Ensure every DAG file imports with no
  errors and has no cycles:

```python
# tests/test_dag_integrity.py (illustrative)
from airflow.models import DagBag

def test_no_import_errors():
    dagbag = DagBag(dag_folder="dags/", include_examples=False)
    assert not dagbag.import_errors, dagbag.import_errors
```

- **Unit-test the business logic, not Airflow.** The heavy logic here lives in `pipeline/` and `utils/`, which
  are plain Python — test those directly (fast, no Airflow needed). Keep DAG callables thin so they're easy to
  test.
- **`airflow dags test <dag_id> <date>`** — execute a full DAG run locally without the scheduler, great for
  end-to-end checks.
- **`airflow tasks test <dag_id> <task_id> <date>`** — run a single task in isolation, printing logs; no state
  is written. Perfect for iterating on one task.
- **`python dags/weather_ingest_dag.py`** — if it runs without error, the file at least parses.
- **Lint for anti-patterns** with tools like `ruff`/`pylint`; check for top-level heavy code (§9).

> **CI idea:** run the DAG integrity test on every PR. It catches the majority of "broken DAG" incidents before
> they ever reach the scheduler.

---

## 17. Production standards & best practices

The rules that separate a hobby DAG from a project-standard one:

**DAG design**
1. **Idempotency.** Running the same task for the same interval twice must produce the same result. Process the
   task's **data interval** (`{{ ds }}`, `data_interval_start`), not `now()`. (Our hourly ingest is *nearly*
   idempotent but appends rows; dedup happens later in dbt's silver layer — a reasonable pattern.)
2. **Atomicity.** One task = one well-defined unit of work that either fully succeeds or fully fails.
3. **Keep top-level code light.** No DB queries / API calls / heavy imports at module scope (see §9). The
   scheduler parses files constantly.
4. **Static `start_date`, `catchup=False`** unless you deliberately want backfill.
5. **Small tasks, clear names, tags.** Independent, retryable units (like the per-province tasks) localize
   failure.

**Reliability & safety**
6. **Retries with sensible delays** for transient errors (done).
7. **Alerting** via `on_failure_callback` / email / Slack (currently off — add it).
8. **Real Fernet key** and secrets in Connections/Variables or a secrets backend, not in code (§13).
9. **Pools** for shared fragile resources (done — the API pool).
10. **Timeouts/SLAs** so a hung task doesn't run forever.

**Operations**
11. **Separate metadata DB from data DB** (done).
12. **Version-control DAGs**; deploy via image build/mounted volume (done).
13. **Monitor** scheduler health, task duration trends, and failure rates.
14. **Resource isolation**: heavy work runs in the target system (Postgres/dbt), not on the worker.

**Modern patterns to adopt as you grow**
15. **TaskFlow API** for new Python DAGs (§14).
16. **Dynamic Task Mapping** instead of parse-time loops (§9).
17. **Datasets** for cross-DAG data dependencies (§15).
18. **Deferrable operators/sensors** for long waits (§6).

---

## 18. Known issues & improvements in this project

Concrete, prioritized observations about the current DAGs. **P1 = do first.**

| # | Priority | Observation | Fix | Ref |
|---|----------|-------------|-----|-----|
| 1 | **P1** | `weather_ingest` runs a **DB query at parse time** (`load_active_locations()` in top-level loop) | Move to **Dynamic Task Mapping** (`.expand()`) or a cached/defensive fetch | §9 |
| 2 | **P1** | **No alerting** (`email_on_failure: False`, no callbacks) | Add `on_failure_callback` (Slack/email) at least for the health check & dbt tasks | §12, §17 |
| 3 | **P1** | **Empty Fernet key** in compose | Generate a real Fernet key for any non-local deployment | §13 |
| 4 | P2 | `weather_transform` scheduling **assumes** ingest finished by midnight (time-coupled) | Use a **Dataset** producer→consumer link (or keep the freshness guard) | §15 |
| 5 | P2 | dbt tasks defined **twice** in the transform file (module-level `task_dbt_run`/`task_dbt_test` and again inside the `with DAG`) with **different `--profiles-dir`** | Remove the unused module-level operators; keep one definition | — |
| 6 | P2 | dbt runs with implicit **`dev`** target (no `--target`) | `dbt build --target prod` (see `docs/DBT_PRODUCTION.md` §2/§9) | — |
| 7 | P3 | Classic operator style + manual XComs | Consider **TaskFlow API** for readability on new DAGs | §14 |
| 8 | P3 | `check_bronze_freshness` / `verify_gold_tables` are bespoke Python | Could use sensors / dbt source freshness | §6 |

> The module-level operators in `weather_transform_dag.py` (top of file) are **created but never added to the
> `with DAG(...)` block**, so they're dead code — Airflow only schedules the ones defined *inside* the DAG
> context. Cleaning this up avoids confusion.

---

## 19. Glossary

| Term | Meaning |
|------|---------|
| **DAG** | Directed Acyclic Graph — the whole workflow definition. |
| **DAG Run** | One execution of a DAG for a specific data interval. |
| **Task** | A node in a DAG (one unit of work). |
| **Operator** | A template that becomes a task when instantiated (`PythonOperator`, `BashOperator`, …). |
| **Sensor** | An operator that waits for a condition. |
| **Task Instance** | A specific run of a task (has state: running/success/failed/…). |
| **Executor** | Strategy for where tasks run (Sequential/Local/**Celery**/Kubernetes). |
| **Scheduler** | Process that parses DAGs and decides what to run. |
| **Worker** | Process that executes task code. |
| **Triggerer** | Process that runs async triggers for deferrable operators. |
| **XCom** | Small data passed between tasks via the metadata DB. |
| **Pool** | A concurrency limit for a shared resource. |
| **Trigger rule** | Condition on upstream states that decides if a task runs (`all_success`, `all_done`, …). |
| **Catchup** | Whether to backfill missed intervals since `start_date`. |
| **Data interval** | The time window `[start, end)` a DAG run processes. |
| **Connection / Variable** | Stored credentials / config in the metadata DB. |
| **TaskFlow API** | `@dag`/`@task` decorator style with automatic XComs. |
| **Dataset** | A named data artifact used for event-driven (data-aware) scheduling. |

---

## 20. Where to go next

Hands-on exercises using **this** project:

1. **Explore the UI.** Unpause `weather_ingest`, watch the 25 province tasks fill the pool 10 at a time, and
   read the `health_check` logs.
2. **Run one task in isolation:** `airflow tasks test weather_transform check_bronze_freshness 2026-07-12`.
3. **Add a DAG integrity test** (§16) and run it.
4. **Refactor the parse-time query** in `weather_ingest` to Dynamic Task Mapping (§9) — the single biggest
   production improvement.
5. **Add failure alerting** via `on_failure_callback` (§12).
6. **Prototype a Dataset link** so `weather_transform` triggers off `weather_ingest` instead of a fixed
   midnight cron (§15).

Authoritative docs:
- Core concepts: <https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/>
- Best practices: <https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html>
- TaskFlow tutorial: <https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html>
- Dynamic Task Mapping: <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html>
- Datasets: <https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/datasets.html>

> **One-sentence summary:** *Airflow runs your Python-defined tasks in dependency order on a schedule, with
> retries, concurrency control, and a UI — so keep the heavy work in the systems it triggers, keep DAG files
> light and idempotent, and make failures loud.*

---

*Cross-reference: architecture in `docs/TECHNICAL.md`; dbt fundamentals in `docs/LEARN_DBT.md`; dbt production
practices in `docs/DBT_PRODUCTION.md`. The code is the source of truth — verify line references against the
current files.*
