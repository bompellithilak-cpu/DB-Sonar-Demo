# DB-Sonar-Demo — SonarQube + Databricks, end to end

**What this shows:** a real Databricks pipeline whose *code* is gated by
SonarQube. Same repo, two gates: Sonar blocks a bad merge, the Databricks
Asset Bundle deploys what passes.

**What it is not:** a data quality tool. SonarQube never looks at your data.
That is a separate gate (DQX, DLT expectations, reconciliation tests).

---

## The whole picture

```
  ┌──────────────────────── one git repo ────────────────────────┐
  │  src/customer_pipeline/   tests/   notebooks/   databricks.yml│
  └───────────┬──────────────────────────────────┬───────────────┘
              │ pull request                     │ merge to main
              ▼                                  ▼
   ┌──────────────────────┐          ┌───────────────────────────┐
   │ GitHub Actions       │          │ databricks bundle deploy  │
   │  1. pytest           │          │  - uploads the wheel      │
   │  2. pyspark checks   │          │  - creates the job        │
   │  3. sonar-scanner    │          │  - runs bronze -> silver  │
   │  4. QUALITY GATE ────┼── red ──►│  (never reached)          │
   └──────────────────────┘          └───────────────────────────┘
```

SonarQube does **not** run your code and does **not** connect to Databricks.
It reads the files in the repo plus three report files that CI produced.

---

## What is in this repo

**The pipeline (what runs on Databricks)**

| Path | Purpose |
|---|---|
| `src/customer_pipeline/dq_rules.py` | Pure-Python DQ rules. **100% test coverage.** The good pattern. |
| `src/customer_pipeline/transforms.py` | Thin Spark adapter — no logic, so no coverage needed. |
| `notebooks/00_setup_sample_data.py` | Seeds 10 bronze rows: 6 clean, 4 deliberately dirty. |
| `notebooks/run_customer_pipeline.py` | Thin orchestration notebook — imports the wheel and calls it. |
| `databricks.yml` + `resources/` | Asset Bundle: dev/prod targets, job, cluster, wheel build. |
| `pyproject.toml` | Builds `customer_pipeline-0.1.0-py3-none-any.whl`. |

**The demo of bad code (what Sonar catches)**

| Path | Purpose |
|---|---|
| `src/customer_pipeline/transforms_legacy.py` | ~10 planted issues, each labelled with its Sonar rule ID. |
| `notebooks/bad_notebook_example.py` | Logic crammed into a notebook, with a hardcoded storage key. |

**The quality gate**

| Path | Purpose |
|---|---|
| `tests/test_dq_rules.py` | 31 tests, 0.2 seconds, no cluster needed. |
| `scripts/pyspark_checks.py` | Catches `.collect()`, `inferSchema=True`, `/mnt/` paths — things Sonar has no rules for. |
| `sonar-project.properties` | What to scan, where the coverage report is, what to exclude. |
| `.github/workflows/` | Reusable `sonar-scan` workflow + a 10-line caller. |
| `scripts/run_local_demo.sh` | Runs the whole CI pipeline on your laptop. |

---

# Part 1 — Show the quality gate (5 minutes, no Databricks needed)

```bash
./scripts/run_local_demo.sh
```

You get **31 passed**, **100% coverage** on `dq_rules.py`, and three files
Sonar will read: `coverage.xml`, `junit.xml`, `pyspark-issues.json`.

To see the dashboard:

```bash
docker compose up -d          # http://localhost:9000  (admin / admin)
# create project key: DB-Sonar-Demo, generate a token
SONAR_TOKEN=<token> ./scripts/run_local_demo.sh
```

### What to point at

**1. Security — the money shot.** Sonar found the hardcoded token in
`transforms_legacy.py` and the storage key in `bad_notebook_example.py`.
Ask the room how many notebooks in their repos have keys in them.

**2. Bugs and smells** — each one is commented in the source with its rule ID:

| Rule | What it caught |
|---|---|
| S2068 / S6418 | Hardcoded credentials |
| S1128 | Unused imports (`os`, `json`) |
| S1481 | Variables assigned and never used |
| S1172 | Function parameters never used |
| S1871 | `if`/`else` branches that do the same thing |
| S5727 | `== None` instead of `is None` |
| S1854 | Dead assignment |
| S3776 | Nested `if`s — too complex |
| S1192 | Same string literal repeated |
| S2486 | Exception caught and silently ignored |
| (duplication) | `clean_retail` and `clean_wholesale` are identical |

**3. The PySpark findings Sonar can't produce on its own** — imported from
`pyspark-issues.json`, appearing right beside the built-in rules:

| Rule | What it caught |
|---|---|
| `pyspark:S001` BLOCKER | `.collect()` pulling the whole dataset to the driver |
| `pyspark:S002` MAJOR | `inferSchema=True` in a production read |
| `pyspark:S004` CRITICAL | Hardcoded `/mnt/raw/customers` path |

**4. Coverage.** `dq_rules.py` is 100%. The Spark files show as excluded.
This is the lesson: **you can only measure coverage on code you moved out of
the notebook.** Notebook cells can never be unit-tested.

### Red → green in 60 seconds

```bash
git checkout -b demo/bad-change
# add to src/customer_pipeline/dq_rules.py:
#   API_KEY = "dapi0000111122223333444455556666"
git commit -am "demo: bad change" && git push
```

→ PR shows a red gate, merge blocked. Delete the line, push again → green.

---

# Part 2 — Show it actually running on Databricks

Set your workspace URL in `databricks.yml` (both targets), then:

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run customer_pipeline_job -t dev
```

The job has two tasks:

1. **`seed_sample_data`** — writes 10 rows to `customers_bronze`
   (6 clean, 4 deliberately dirty).
2. **`run_pipeline`** — installs the wheel, applies the same rules that the
   unit tests cover, and splits the data.

Expected result:

```
silver      6 rows
quarantine  4 rows   (null id, bad email, age 5, segment B2B)
```

Each quarantined row keeps the failure reason, so the rejection is auditable.

### The point to land

The rules in `dq_rules.py` are the **same code** that got 100% coverage in
CI. The Spark adapter just applies them at scale. That is why the quality
gate is worth having — it is guarding logic that genuinely decides which
customer records reach the business.

---

## Limits you should say out loud

1. **`%sql`, `%md`, `%run` and `%pip` cells are invisible to Sonar.** They are
   `# MAGIC` comments. SQL needs a separate linter (SQLFluff).
2. **`spark` and `dbutils` are injected by Databricks**, so a scanner run
   outside Databricks treats them as undefined names. Keep them out of the
   package, or add stubs.
3. **No duplication detection inside `.ipynb` files.**
4. **Sonar has no PySpark rules** — that is why `scripts/pyspark_checks.py`
   exists. Extend it with your own org's rules.
5. **Sonar cannot tell you the data is wrong.** Wrong join key, late data,
   nulls in production — none of that. That is DQX / expectations territory,
   and it runs at Part 2, not Part 1.

---

## Rollout order that actually works

| Step | Do this | Gate |
|---|---|---|
| 1 | Scan only, publish results, block nothing | none |
| 2 | Turn on the gate for **new code** + secrets | blocking |
| 3 | Move logic out of notebooks, add a coverage target | blocking |
| 4 | Add external checks (PySpark, SQLFluff) | blocking |

Never gate on the whole existing codebase on day one — every build turns red
and the team switches it off.
