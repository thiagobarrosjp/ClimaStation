Here’s a clean **learning\_plan.md** you can drop into your repo.

---

# ClimaStation — Learning Plan (Architect Track)

> Goal: understand the architecture, vocabulary, and quality practices you actually use in ClimaStation—so you can read code confidently, define good contracts, and run a tight process (without becoming a full-time coder).

## Outcomes (what you’ll be able to do)

* Sketch the pipeline and name each stage’s **contract**, **invariants**, and **failure modes**.
* Read Python diffs and CI output to approve/reject changes.
* Define **golden tests**, **logging standards**, and **ADRs** (Architecture Decision Records).
* Choose storage and schema strategies; explain **SQL vs PostgreSQL** trade-offs.

---

## Roadmap (4 modules, \~10–15 hours total)

### Module 1 — Core Vocabulary & Mental Models (2–3h)

* [ ] Read the **Glossary** below.
* [ ] Write one-paragraph notes (your own words) for each concept with a ClimaStation example.
* [ ] Create `docs/contracts/` with one **Contract Card** per stage (crawler, downloader, parser, writer).

**Done when:** you can explain *contract*, *invariant*, *idempotency*, *cohesion/coupling*, *modular monolith vs microservices*, *TDD*, *CI/CD*, *schema evolution*.

---

### Module 2 — Architecture & Data (3–4h)

* [ ] Draft **ADR-001**: JSONL now, Parquet later (why, risks, revisit trigger).
* [ ] Draft **ADR-002**: Modular monolith now; split criteria later.
* [ ] Draft **ADR-003**: Idempotent downloader/parser (how do we guarantee it?).
* [ ] Create a **schema evolution** note (URF v1 → v1.1 rules: only additive, deprecate via warning, etc.).
* [ ] Write a 1-page **Storage Map**: what lives in files vs (future) Postgres.

**Done when:** you can justify file formats, chunk sizes, and when to move some metadata/state into Postgres.

---

### Module 3 — Testing & Observability (3–4h)

* [ ] Prepare a **GOLDEN pack** (tiny real ZIP + tiny metadata slice).
* [ ] Add a **golden E2E test**: tiny input → exact JSONL output (byte compare).
* [ ] Add **unit tests** for one pure function (e.g., timestamp normalization).
* [ ] Define a **logging spec**: required fields per stage (dataset, counts, elapsed, errors).
* [ ] Add a **failure safari** test: broken CSV → clear error + non-zero exit.

**Done when:** you can run `pytest` and tell from failures whether code or spec must change.

---

### Module 4 — CI/CD & Review Workflow (2–3h)

* [ ] Add CI checks: Black, Ruff, MyPy (at least on `/app/core`), Pytest w/ coverage, `pip-audit`.
* [ ] Create `CONTRIBUTING.md` with: contracts-first tasks, design-notes.md per change, and a review checklist.
* [ ] Make **golden test** and **lint/type** checks **required** before merge.

**Done when:** a PR that breaks contracts or golden output cannot merge.


---

## Contract Cards (templates to fill)

Create one file per stage under `docs/contracts/`.

```
# Contract — <Stage>
**Purpose:** (one sentence)
**Inputs:** (paths/files, CLI flags)
**Outputs:** (files, naming, chunking, encoding)
**Invariants:** (bullets)
**Error Modes:** (what fails, exit codes)
**Perf Budget:** (e.g., memory < 500MB; stream rows)
**Idempotency:** (how ensured)
**Observability:** (required log fields)
```

---

## Review Checklist (use on every PR)

* [ ] Change stays inside intended module; no new hidden coupling.
* [ ] Contracts respected (inputs/outputs/errors/perf).
* [ ] Logs include dataset, counts, elapsed, and actionable errors.
* [ ] Tests added/updated: unit + golden; CI green.
* [ ] Design-notes.md: assumptions, edge cases, rollback plan.

---

## Practice Labs (apply as you learn)

* [ ] **Golden Test:** build tiny input → exact JSONL; wire into CI.
* [ ] **Failure Safari:** remove a CSV column; ensure graceful, loud failure.
* [ ] **Throughput Probe:** throttle 0.1 vs 0.5; record elapsed and request counts; pick a default.
* [ ] **Schema Drift Drill:** add one optional field to URF; confirm backward compatibility.

---

## Tracking

Use checkboxes above. Add a simple progress table if helpful:

| Module   | Status    | Notes |
| -------- | --------- | ----- |
| Module 1 | ☐ / ☐ / ☐ |       |
| Module 2 | ☐ / ☐ / ☐ |       |
| Module 3 | ☐ / ☐ / ☐ |       |
| Module 4 | ☐ / ☐ / ☐ |       |

---

### Tips

* Keep each learning session to **60–90 minutes**; end by committing one tangible artifact (contract card, ADR, test).
* Favor writing **small docs** you’ll reuse in prompts and reviews over reading long tutorials.

---

### Glossary: **Bilingual glossary (EN ⇄ DE)** tuned for real usage in German teams.
**Legend:** In the “German (used)” column, I only list terms that are *actually used*. If teams usually keep the English word, I mark it **EN üblich** (English customary). Where helpful, I add a literal DE translation in *( )* for understanding.


# Architecture & Design

| English                            | German (used)                                                         | Meaning (short)                                                              |
| ---------------------------------- | --------------------------------------------------------------------- | 
| Contract                           | **Contract** / **API-Spezifikation** / **Schnittstellenbeschreibung** | Explicit inputs, outputs, errors, performance for a module/API.              |
| Invariant                          | **Invariante**                                                        | Rule that must always hold (e.g., chunk ≤ 50k lines).                        |
| Idempotency                        | **Idempotenz**                                                        | Re-running an operation ends in the same final state (no duplicates).        |
| Determinism                        | **Determinismus**                                                     | Same input → same output every time.                                         |
| Cohesion                           | **Kohäsion**                                                          | How tightly responsibilities in a module belong together (higher is better). |
| Coupling                           | **Kopplung**                                                          | How much modules depend on each other’s internals (lower is better).         |
| Modular monolith                   | **Modularer Monolith**                                                | One deployable app with clear internal boundaries.                           |
| Microservice                       | **Mikroservice**                                                      | Small, independently deployed service with a narrow purpose.                 |
| Interface / API                    | **Schnittstelle / API**                                               | The callable surface (functions, flags, endpoints).                          |
| ADR (Architecture Decision Record) | **ADR** / **Architektur-Entscheidung**                                | Short doc recording a decision and its rationale.                            |

# Data & Storage

| English            | German (used)          | Meaning (short)                                           |
| ------------------ | ---------------------- | --------------------------------------------------------- |
| Schema             | **Schema**             | Defined shape of data (fields, types, required/optional). |
| Schema evolution   | **Schema-Evolution**   | Planned, compatible changes to a schema over time.        |
| JSON Lines (JSONL) | **JSONL**              | One JSON object per line; great for streaming.            |
| Parquet            | **Parquet**            | Columnar file format; fast analytics, compression.        |
| SQL                | **SQL**                | Query language & relational model.                        |
| PostgreSQL         | **PostgreSQL**         | SQL database engine with powerful extensions.             |
| Normalization      | **Normalisierung**     | Reduce duplication via related tables.                    |
| Denormalization    | **Denormalisierung**   | Duplicate for faster reads.                               |
| Index              | **Index**              | Structure to speed up lookups.                            |
| Transaction / ACID | **Transaktion / ACID** | Atomic, consistent, isolated, durable operations.         |

# Pipelines & Processing

| English      | German (used)                | Meaning (short)                             |
| ------------ | ---------------------------- | ------------------------------------------- |
| ETL / ELT    | **ETL / ELT**                | Extract–Transform–Load vs transform later.  |
| Streaming    | **Streaming**                | Process incrementally as data arrives.      |
| Batch        | **Stapelverarbeitung**       | Process in chunks or on schedule.           |
| Backpressure | **Backpressure (EN üblich)** | Slow producers when consumers lag.          |
| Manifest     | **Manifest**                 | List of items to process (URLs, checksums). |
| Checksum     | **Prüfsumme**                | Fingerprint to verify file integrity.       |
| Exit code    | **Exit-Code**                | Program status number (0 ok, ≠0 error).     |

# Reliability & Networking

| English         | German (used)                                     | Meaning (short)                               |
| --------------- | ------------------------------------------------- | --------------------------------------------- |
| Throttle        | **Drosselung** / **Rate Limiting**                | Intentional delay between requests.           |
| Retry           | **Retry (EN üblich)**                             | Try again after a failure.                    |
| Backoff         | **Backoff (EN üblich)**                           | Increasing wait time on repeated retries.     |
| Jitter          | **Jitter (EN üblich)**                            | Randomness added to backoff to avoid bursts.  |
| Timeout         | **Timeout (EN üblich)**                           | Stop waiting after a set time.                |
| Fault tolerance | **Fehlertoleranz**                                | Keep working despite partial failures.        |
| Observability   | **Observability (EN üblich)** *(Beobachtbarkeit)* | Understand internals via logs/metrics/traces. |
| Logging         | **Logging**                                       | Structured messages about program behavior.   |

# Testing & Quality

| English               | German (used)                                 | Meaning (short)                                     |
| --------------------- | --------------------------------------------- | --------------------------------------------------- |
| TDD                   | **TDD / testgetriebene Entwicklung**          | Red → Green → Refactor workflow.                    |
| Unit test             | **Unit-Test**                                 | Tests a small, isolated part.                       |
| Integration test      | **Integrationstest**                          | Tests multiple parts together.                      |
| End-to-end (E2E) test | **Ende-zu-Ende-Test**                         | Tests the whole flow on realistic input/output.     |
| Golden file test      | **Golden-Master-Test** / **Golden-File-Test** | Compare output byte-for-byte with a canonical file. |
| Fixture               | **Fixture (EN üblich)**                       | Reusable test setup/data.                           |
| Property-based test   | **Property-based Test (EN üblich)**           | Validates general rules/invariants.                 |
| Linting               | **Linting**                                   | Static checks for style & obvious errors.           |
| Type checking         | **Type Checking (EN üblich)**                 | Static type verification (e.g., MyPy).              |
| Coverage              | **Testabdeckung**                             | % of lines executed by tests (signal, not goal).    |

# Python & Code Literacy

| English              | German (used)                                      | Meaning (short)                                  |
| -------------------- | -------------------------------------------------- | ------------------------------------------------ |
| Context manager      | **Context Manager (EN üblich)** *(Kontextmanager)* | `with …:` that sets up/cleans up resources.      |
| Iterator / generator | **Iterator / Generator**                           | Lazy sequence; `yield` streams values.           |
| Side effect          | **Seiteneffekt**                                   | Function changes state outside its return value. |
| Pure function        | **Pure Function (EN üblich)** *(reine Funktion)*   | Output depends only on input; no side effects.   |
| Dependency           | **Abhängigkeit**                                   | External lib or internal module relied on.       |
| Namespace / package  | **Namespace / Paket**                              | Organized module grouping to avoid collisions.   |

# Dev Process & CI/CD

| English                             | German (used)                                  | Meaning (short)                               |
| ----------------------------------- | ---------------------------------------------- | 
| Continuous Integration (CI)         | **Continuous Integration (EN üblich)**         | Auto checks on every change.                  |
| Continuous Delivery/Deployment (CD) | **Continuous Delivery/Deployment (EN üblich)** | Automated, safe releases.                     |
| Code review                         | **Code-Review**                                | Peers (or you) inspect changes before merge.  |
| Style guide                         | **Styleguide**                                 | Agreed coding conventions (e.g., Black/Ruff). |

---

## Notes for your docs

* For **“Contract”**, use *Contract* for the concept and *API-Spezifikation / Schnittstellenbeschreibung* for the concrete artifact (e.g., OpenAPI, CLI spec).
* Terms marked **EN üblich** are commonly kept in English by German-speaking teams; you can add the German in parentheses the first time for clarity.







