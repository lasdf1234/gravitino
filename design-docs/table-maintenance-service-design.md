<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Design of Table Maintenance Service in Gravitino

## 1. Background

The Table Maintenance Service (TMS) in Gravitino is currently an alpha feature.
The `maintenance/optimizer` package already contains the execution core for statistics collection,
rule evaluation, strategy recommendation, metrics query, and job submission (`Updater`,
`Recommender`, providers, `JobSubmitter`).

Today that core is not hosted as a long-running Gravitino service. Without a server-side component:

1. There is no stable **server-side signal** after Iceberg commits. Engines would otherwise each need a
   TMS-specific listener, or operators must schedule maintenance externally.
2. Configuration, audit, and service-level metrics are hard to centralize when execution is
   ad hoc and process-local.
3. Each run creates its own runtime and provider instances instead of a shared service lifecycle.

This design turns TMS into a **main-server REST plugin** on port **8090** (same pattern as IdP
via `gravitino.server.rest.extensionPackages`) with colocated IRC, reusing the existing optimizer
execution core. Spark maintenance jobs stay on the Gravitino job framework (`jobId` boundary unchanged).

TMS uses a **dual trigger model** (§5.4–§5.6):

- **Commit path** — after each successful Iceberg commit, the IRC post-commit hook **INSERTs** one
  `table_maintenance_event` row (§6.3), then invokes an in-process TMS callback for **compaction
  only** (§5.4.1).
- **Scheduled path** — each Gravitino node runs a **`MaintenancePoller`** (`takePendingDue` per-row
  claim). When `next_due_at` is reached, any node may claim and run that policy. **All four policy
  types** use this path (§10: at-least-once latest-state).

---

## 2. Goals

1. **In-process plugin on the main server**: Load Table Maintenance through
   `gravitino.server.rest.extensionPackages` (Jersey 2 `Feature`, same pattern as IdP) so the IRC
   callback is registered in the main JVM. The commit path does **not** use HTTP. Operator calls that
   replace the optimizer CLI are the ops APIs in **§7**.
2. **Maintenance profile (convenience)**: A profile such as `standard` creates and attaches all four
   policies with sensible defaults in one step. Profiles are **not** a fifth policy type (§5.2).
3. **Precedence per maintenance type**: For each maintenance type, the **nearest** attachment along
   `table → schema → catalog → metalake` wins. Policies are **not** additive for maintenance (§5.2).
4. **Dual trigger model (option B)**: **Compaction** on **commit** (§5.4.1) **and** on the
   **poller** at wall-clock schedule (§5.4.2). **Manifest rewrite, snapshot expiry, and orphan
   cleanup** use the poller only (§5.5–§5.6). Policy **schedule** drives `next_due_at` for the
   **poller only** (§5.2.4).
5. **Wall-clock schedules in Gravitino**: Policy schedules live in Gravitino and are read by the
   poller, not by the commit hook.
6. **Reuse existing optimizer execution core**: Both paths invoke the same `Updater` /
   `Recommender` / job-submit paths in `maintenance/optimizer` as **in-process methods**.
7. **Job framework compatibility**: Spark maintenance work continues to use the Gravitino job
   framework. TMS returns or records submitted `jobId` values but does not own job status.
8. **Govern Policy reuse**: Maintenance policies stay on existing `policy_meta` and metalake Policy
   APIs (create / alter / enable / disable / associate). TMS does **not** introduce a parallel policy
   store or `/api/maintenance/table/policies` CRUD.
9. **Multi-node safe processing**: Shared DB **per-policy claims** so only one TMS replica runs
   evaluate → submit for a given `(table, policy)` at a time (§6). Gravitino replicas remain **peers**
   for IRC and commit-path compaction; there is no maintenance **leader node** (§5.3).
10. **Commit log**: The IRC post-commit hook **INSERTs** one `table_maintenance_event` row per
    successful commit (`table_identifier`, `created_at`) (§6.3).

---

## 3. Non-Goals

1. **Standalone maintenance daemon**: No separate process or
   `gravitino-iceberg-rest-server.sh`-style entrypoint.
2. **Dedicated auxiliary HTTP listener**: No `GravitinoAuxiliaryService`, no isolated
   `gravitino.maintenance.classpath`, and no dedicated TMS port (for example **9301**). TMS is not
   a dedicated listener like `iceberg-rest` / `lance-rest`.
3. **No maintenance leader node**: No single node that scans all tables each tick (§5.3). Timed
   maintenance uses **per-node pollers** and **per-row claims** instead (§5.3).
4. **Provider SPI rewrite**: Does not replace `StatisticsUpdater`, `StatisticsCalculator`,
   `StatisticsProvider`, `StrategyProvider`, `TableMetadataProvider`, or `JobSubmitter` contracts.
5. **Engine-side commit report path**: Engines that bypass Gravitino Iceberg REST are out of scope
   for the commit compaction path.
6. **Commit-path HTTP or Kafka**: No `POST …/events/iceberg-commit`, no health resource, and no Kafka
   produce/consume path. Commit handling is **in-process only** (§5.1.1).

---

## 4. Solution Investigations

### 4.1 Option A: Keep process-local execution only

Continue running all optimizer work in ad hoc local processes, with no TMS service endpoint.

**Pros:** No new listener. Minimal implementation work.

**Cons:** No IRC signal target; no centralized service for automated maintenance.

**Decision:** Rejected.

### 4.2 Option B: In-process plugin on the main server (Chosen)

Register Table Maintenance as a Jersey 2 `Feature` through
`gravitino.server.rest.extensionPackages` (same pattern as IdP) so it runs inside the main server
process. The commit path does **not** use HTTP. After each Iceberg commit, the **colocated** IRC hook
INSERTs `table_maintenance_event` (§6.3) and invokes an in-process callback; TMS runs compaction
evaluate → submit on a bounded executor (§5.4.1). Timed maintenance is driven by a built-in
`MaintenancePoller` on every node (§5.3).

**Pros:** No extra process or port; no remote event hop on the commit path; reuses Policy + Jobs on
the same server; matches plugin packaging; keeps Gravitino replicas peer-equal (no maintenance leader).

**Decision:** **Chosen**.

### 4.3 Option C: Independent long-running Table Maintenance Service process

**Pros:** Full JVM isolation.

**Cons:** Extra deployable; duplicates server lifecycle patterns already covered by the main
webserver plugin.

**Decision:** Rejected. Prefer the in-process plugin on the main server.

### 4.4 Option D: Dedicated aux Jetty listener (:9301)

Implement `GravitinoAuxiliaryService` with `shortName() = "maintenance"`, expose a dedicated Jetty
listener (default **9301**), and keep TMS off the main 8090 JAX-RS app.

**Pros:** Classpath isolation similar to `iceberg-rest` / `lance-rest`.

**Cons:** Extra port and aux enablement; diverges from plugins that already extend
**8090** via `extensionPackages`.

**Decision:** Rejected. Prefer Option B.

### 4.5 Industry survey: scheduled maintenance clocks

Most lakehouse maintenance products treat **snapshot expiry, manifest rewrite, and orphan cleanup** as
**time-driven** (scheduler, cron, or platform optimizer interval), not as post-commit hooks.

| Product                                                                                                | Clock mechanism                                                              | Typical cadence                                                                     | Operations on schedule                                                                                       |
| ------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| [AWS Glue table optimizers](https://docs.aws.amazon.com/glue/latest/dg/table-optimizers.html)          | Managed optimizer runs per table                                             | Default **24h** (`runRateInHours`)                                                  | Compaction, snapshot **retention**, **orphan file deletion** — each optimizer type has its own interval      |
| [Floe](https://github.com/nssalian/floe)                                                               | Per-policy `cronExpression` / `interval` **or** `POST …/maintenance/trigger` | Per-op schedules (e.g. compact every 4h, expire daily)                              | Compaction, expire snapshots, orphan cleanup, rewrite manifests — **independent schedules per operation**    |
| [Apache Amoro](https://amoro.apache.org/docs/latest/configurations/)                                   | AMS `PeriodicTableScheduler` executors                                       | Snapshot expire default **1h**; orphan clean **7d**; dangling deletes **24h**       | Snapshot expiration, orphan files, dangling delete files — **separate periodic executors**, not commit hooks |
| [OpenHouse](https://github.com/linkedin/openhouse/blob/main/ARCHITECTURE.md)                           | K8s **CronJob** data services                                                | Operator-defined                                                                    | Table maintenance jobs triggered by platform cron                                                            |
| [Databricks OPTIMIZE / VACUUM guidance](https://docs.databricks.com/aws/en/tables/operations/optimize) | Scheduled jobs or predictive optimization                                    | **Daily** recommended starting point for `OPTIMIZE`; predictive layer for UC tables | File layout (`OPTIMIZE`) and vacuum are **scheduled / platform-driven**, separate from write path            |

**Takeaway for TMS:** timed maintenance is **time-driven** in industry; TMS implements that with a
built-in poller plus per-row `next_due_at` / `minIntervalMs`. External cron clocks (Floe trigger API,
OpenHouse CronJob) are surveyed for context only — TMS does **not** adopt them.

### 4.6 Industry survey: commit / write-path triggers

**Compaction** (rewrite data files, small-file consolidation) is the operation most often tied to
**writes or commits**. Manifest rewrite, snapshot expiry, and orphan cleanup are usually **not**
run on every commit.

| Product                                                                                                                           | Write / commit trigger                                                                                                                                          | What runs on the write path                                                  | What stays scheduled                                                                                                                                |
| --------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| [Databricks Iceberg auto compaction](https://docs.databricks.com/aws/en/tables/tune-file-size)                                    | After a **successful write** when small-file thresholds are met                                                                                                 | **Compaction only** (`OPTIMIZE` with `operationParameters.auto = true`)      | Larger `OPTIMIZE`, manifest work, expire, vacuum — **scheduled or predictive**                                                                      |
| [Apache Amoro self-optimizing](https://amoro.apache.org/docs/latest/configurations/)                                              | **Minor** optimization when fragment + equality-delete file count **or** time interval threshold is met (planning is continuous; commit produces new snapshots) | Compaction tiers (minor / major / full)                                      | Snapshot expiration (`SnapshotsExpiringExecutor`), orphan clean (`OrphanFilesCleaningExecutor`), dangling deletes — **separate periodic executors** |
| [Amoro AIP-3](https://cwiki.apache.org/confluence/display/AMORO/AIP-3%3A+Event-Triggered+Optimization+of+Iceberg+Tables+in+Amoro) | Event-triggered **optimization** (file-count / metric signals)                                                                                                  | Compaction-style self-optimizing                                             | Expire / orphan remain lifecycle tasks outside the event path                                                                                       |
| [Floe `triggerConditions`](https://github.com/nssalian/floe/blob/main/docs/policies.md)                                           | Optional **health-based** triggers (`smallFilePercentageAbove`, `snapshotCountAbove`, …) with `minIntervalMinutes`                                              | Any enabled op **when conditions fire** (often compaction-first in examples) | Default path is still **cron per operation**; conditions augment, not replace, schedules                                                            |
| [AWS Glue compaction optimizer](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-table-optimizers.html)                    | Threshold-based (`minInputFiles`, `deleteFileThreshold`) inside **scheduled** optimizer runs                                                                    | Compaction during optimizer run, not inline on catalog commit                | Retention and orphan optimizers are **separate scheduled types**                                                                                    |

**Why TMS limits the commit path to compaction:**

| Concern            | Compaction on commit                                          | Manifest / expire / orphan on commit                                                     |
| ------------------ | ------------------------------------------------------------- | ---------------------------------------------------------------------------------------- |
| Commit latency     | Acceptable when bounded (executor + claim + async job submit) | Unacceptable — listing, expire, and orphan scans are heavy                               |
| Inactive tables    | Still benefit when they resume writes                         | **Never maintained** if commits stop                                                     |
| Failed writes      | N/A                                                           | **Orphan files** appear **without** a successful commit                                  |
| Stale metrics      | Recommender can use pre-commit statistics for compaction debt | Expire / orphan need **fresh** table-wide metadata; scheduler pass refreshes stats first |
| Industry alignment | Databricks auto-compact; Amoro minor optimizing               | Glue, Amoro, Floe schedule expire / orphan / manifest separately                         |

**TMS decision (option B):** IRC commit path runs **`system_iceberg_compaction` only** (§5.4.1) — IRC
INSERTs `table_maintenance_event`, then TMS handles compaction asynchronously. The
**`MaintenancePoller`** also runs **all four policy types** when their
wall-clock schedule fires (§5.4.2, §5.2.4). Manifest / expire / orphan are **poller-only** (not on
the commit path). Nightly compaction covers tables that stop receiving commits.

### 4.7 Industry: how defaults reach tables, and why TMS uses discovery

Products that support catalog- or scope-level maintenance defaults still have a **gap** before every
table is actually on the schedule. They close that gap differently:

| Product            | How a catalog / scope default becomes runnable work                                                                                                                                                                                                                                                                                                |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **AWS Glue**       | Catalog stores a **default template**. A table is not optimized until a **table-level optimizer** exists — usually copied from the catalog on **CreateTable / UpdateTable**. Enabling catalog defaults does **not** instantly arm every existing table.                                                                                            |
| **Apache Amoro**   | Catalog `self-optimizing.*` (and related) settings are **merged at runtime** into table config for tables AMS already manages. A table that is not yet in AMS / not yet seen by the periodic scheduler does not run expire / orphan. Catalog changes apply on the next config read for tables **without** table-level overrides (table props win). |
| **Floe / CronJob** | On each **cron tick**, the service **lists tables in the policy scope** from an external catalog (metadata need not live in Floe) and runs work. Tables or policies created between ticks wait for the **next** tick.                                                                                                                              |

**Gravitino constraint:** maintenance **policies** live in Gravitino, but the Iceberg **table inventory**
for an IRC Hive backend may live only in **HMS** (tables created outside IRC never appear in
`table_meta`). TMS therefore cannot assume Amoro-style “runtime merge over a complete in-process
table list,” and cannot rely on Glue-style Create/Update alone (bypassed creates would never copy).

**Alternatives considered for above-table attachments:**

| Approach                                                     | Pros                                      | Cons for Gravitino                                                                                                                            |
| ------------------------------------------------------------ | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Cron tick: list whole schema/catalog, resolve policy, submit | Conceptually simple (Floe-like)           | Every due cycle re-lists large scopes; multi-node needs a leader or duplicate submits; hard to share claim/`next_due_at` with the commit path |
| Glue-like copy on Create/Update only                         | Cheap when all creates go through one API | Misses HMS-only tables; existing tables not re-armed when catalog policy changes                                                              |
| Amoro-like runtime merge over Gravitino entities only        | No separate discovery loop                | Incomplete inventory when metadata is outside Gravitino                                                                                       |

**TMS decision — discovery mode (§5.2.5, §5.3.5):**

1. **Table attachment:** write `table_maintenance_state` immediately (O(1)).
2. **Above-table attachment:** periodic **discovery** lists the attachment scope from the
   **Iceberg/HMS** backend, resolves nearest-wins `effective_policy`, and UPSERTs missing state rows
   with `next_due_at`.
3. **`takePendingDue`:** cheap indexed claim on existing state rows (multi-node safe), separate from
   the slower discovery interval.

Discovery accepts a bounded lag (default one hour) before a newly visible table is scheduled — the
same class of gap Glue / Amoro / cron products already have — while keeping due-work execution
scalable and aligned with per-row claims.

### 4.8 Industry: multi-node schedule coordination without HA

Products that run timed work on **several peer nodes** (no maintenance leader) typically use one of
four patterns. Patterns **1** and **2** coordinate at **policy / job** grain (one lock or trigger per
scheduled job). Pattern **3** coordinates at **row** grain (one claimable unit per table×policy).
Pattern **4** leans on **external** clock and queue services: an outside Cron (K8s CronJob, cloud
Scheduler) only **enqueues** work; an outside broker (SQS, Kafka, …) fans out to many consumers.
Trigger and execution are decoupled, but the install must bring those components.

| Pattern                                       | Typical products                                                                                        | Pros                                                                                  | Cons                                                                                                                            | Chosen? Reason                                                                                                                                                  |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1. Per-node cron + distributed lock**       | ShedLock; Spring + Redis/DB lock                                                                        | Simple; no leader election; prevents double runs of the same job                      | Often a **single lock for the whole job** — hard to parallelize **per table**                                                   | **Rejected.** Coarse lock serializes all tables under one policy/job; TMS must run many tables concurrently across peers.                                       |
| **2. Shared DB compete for trigger**          | Quartz JDBC Cluster                                                                                     | Mature; peers share work **by job**; one winner per fire                              | Trigger is typically **policy/job-scoped**; locking can bottleneck many short fires; heavier stack                              | **Rejected.** Same grain as pattern 1: winner then lists the policy scope. Does not give per-table claim shared with the commit path.                           |
| **3. Due rows + row-level CAS**               | TMS / `IcebergCleanupManager`; Temporal lease; Hangfire; db-scheduler; SQS visibility timeout (analogy) | No leader; **row-level** parallelism; lease reclaim on crash; fits large table counts | Needs a **state table** + lease; needs **materialization / discovery** for above-table policies                                 | **Chosen** (§5.3). Matches in-tree cleanup; poller and commit path share the same `(table, policy)` claim; scales with due rows, not with a single job lock.    |
| **4. External cron enqueue + multi-consumer** | OpenHouse CronJob; Floe; cloud Cron → SQS                                                               | Decouples trigger from execution; consumers scale on the **external** queue           | Requires **extra components** (external Cron and/or message queue); duplicate-enqueue and consumer **idempotency** still needed | **Rejected.** TMS must not introduce other runtime components beyond Gravitino and its entity DB. Pattern **3** keeps coordination in-process + existing store. |

**TMS mapping:** discovery (§4.7 / §5.3.5) materializes due rows; `takePendingDue` + CAS is
pattern **3** (same choice as §5.3).

---

## 5. Proposal

### 5.1 Architecture

```text
Spark / Flink / Trino
        │  Iceberg REST commit
        v
Gravitino Iceberg REST (IRC, typically :9001)
        │  commit succeeded (same JVM as main server)
        │
        └─ IRC post-commit hook (§5.1.1, §5.4.1)
                │
                ├─ INSERT table_maintenance_event (§6.3)
                └─ in-process callback → bounded executor → compaction only

MaintenancePoller (every Gravitino node — §5.3)
        │  workerLoop: takePendingDue → claim row → evaluate → submit
        v
        ├─ Compaction due rows (§5.4.2):
        │     e.g. nightly_compaction Daily · 02:00 — inactive / catch-up tables
        │
        ├─ Track A — hot pipeline (§5.5):
        │     due rows: one claim per policy (manifest | expire)
        │     soft order: prefer manifest before expire for the same table
        │     refresh statistics (Updater); worst-first within Track A
        │
        ├─ Track B — orphan cleanup (§5.6):
        │     due rows: orphan policy (e.g. Weekly)
        │     oldest-cleanup-first; olderThan enforced server-side
        │
        ├─ shared: maintenance window (optional) + maxConcurrentJobs
        │     (count RUNNING state rows with job_id; unclaimed due rows wait)
        v
Gravitino Job framework + job_run_meta (every run — §6.5)
```

#### 5.1.1 In-process commit callback

Commit signals are delivered **only in-process**. After a successful Iceberg commit, the **IRC
post-commit hook**:

1. **INSERTs** one `table_maintenance_event` row (§6.3) — the only durable write on the IRC thread.
2. Invokes the main-server-registered **in-process callback** (`IcebergCommitEventHandler`), which
   enqueues compaction handling on the bounded executor (§5.4.1).

The hook does **not** resolve non-compaction policies or run Recommender / submit on the IRC thread.

| Requirement     | Detail                                                                                                  |
| --------------- | ------------------------------------------------------------------------------------------------------- |
| Deployment      | IRC (`iceberg-rest`) and the main Gravitino server share **one JVM**.                                   |
| Transport       | In-process callback / SPI only — **no** HTTP, **no** Kafka.                                             |
| Payload         | Normalized `table_identifier` (`catalog.schema.table`); event row in §6.3.                              |
| Commit scope    | **`system_iceberg_compaction` only** (§5.4.1).                                                          |
| IRC thread cost | One `INSERT` into `table_maintenance_event` + callback hand-off; evaluate + submit on bounded executor. |

---

### 5.2 Policy model

#### 5.2.1 Four built-in policy types

Each activity is a **separate** built-in policy type with its own `content`, `minIntervalMs`, and
attachment grain:

| Maintenance type | Illustrative policy type             | Built-in job template                 | Typical attachment | Trigger path                              |
| ---------------- | ------------------------------------ | ------------------------------------- | ------------------ | ----------------------------------------- |
| Compaction       | `system_iceberg_compaction`          | `builtin-iceberg-compaction`          | Table / pattern    | **Commit** (§5.4.1) **+ Poller** (§5.4.2) |
| Manifest rewrite | `system_iceberg_rewrite_manifests`   | `builtin-iceberg-rewrite-manifests`   | Table / pattern    | **Poller** (§5.3, §5.5)                   |
| Snapshot expiry  | `system_iceberg_snapshot_expiration` | `builtin-iceberg-expire-snapshots`    | Catalog / pattern  | **Poller** (§5.3, §5.5)                   |
| Orphan cleanup   | `system_iceberg_orphan_file_removal` | `builtin-iceberg-remove-orphan-files` | Catalog / pattern  | **Poller** (§5.3, §5.6)                   |

See [iceberg-compaction-policy](../docs/iceberg-compaction-policy.md),
[iceberg-expire-snapshots-maintenance-job](./iceberg-expire-snapshots-maintenance-job.md),
[iceberg-rewrite-manifests-job](./iceberg-rewrite-manifests-job.md), and
[iceberg-remove-orphan-files-maintenance-job](./iceberg-remove-orphan-files-maintenance-job.md).

#### 5.2.2 Maintenance profile (one-step setup)

A **profile** such as `standard` creates four policy instances and attaches them with sensible
defaults. It is **not** a fifth `policyType`.

   ```bash
   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     -d '{
    "profile": "standard",
    "target": "catalog.rest_catalog",
    "overrides": {
      "compaction": { "enabled": true },
      "snapshot-expiry": { "olderThanDays": 7 }
    }
  }' \
  http://localhost:8090/api/metalakes/test/maintenance/profiles/apply
```

#### 5.2.3 Precedence (nearest attachment wins)

```text
effective_policy(table, maintenance_type) =
  nearest Active attachment of that type along:
    table → schema → catalog → metalake
```

Only **one** policy per maintenance type is evaluated for a table.

#### 5.2.4 Policy schedule (UI: Daily · 02:00, Sun · 03:00, Weekly, Paused)

Each maintenance policy instance stores a **schedule** in `policy_meta.content` (surfaced in the
maintenance policy UI). TMS computes **`next_due_at`** from the schedule — wall-clock aligned.

**Illustrative UI rows and content:**

| Policy name (UI)         | Applies to   | Schedule (UI) | Status | `content.schedule` (stored)                                            |
| ------------------------ | ------------ | ------------- | ------ | ---------------------------------------------------------------------- |
| `nightly_compaction`     | All Iceberg… | Daily · 02:00 | Active | `{ "type": "daily", "at": "02:00", "timezone": "UTC" }`                |
| `weekly_snapshot_expiry` | All Iceberg… | Sun · 03:00   | Active | `{ "type": "weekly", "day": "SUN", "at": "03:00", "timezone": "UTC" }` |
| `manifest_rewrite`       | `events.*`   | Daily · 04:00 | Active | `{ "type": "daily", "at": "04:00", "timezone": "UTC" }`                |
| `orphan_cleanup`         | All          | Weekly        | Paused | `{ "type": "weekly", "day": "SUN", "at": "04:00" }` + `enabled: false` |

**`next_due_at` computation:**

```text
when a table-level state row is created or the effective schedule changes:
  next_due_at = nextOccurrence(schedule, timezone)

on successful job finish:
  next_due_at = nextOccurrence(schedule, timezone, after = job_finished_at)
  // e.g. nightly 02:00 → tomorrow 02:00, not finished_at + 24h rolling
```

**Paused:** `policy_meta.enabled = false` → poller does not claim the row; commit path also skips
compaction for that policy.

**`minIntervalMs`:** minimum time between runs for a `(table, policy)` — checked via
`last_job_id` → `job_run_meta.job_finished_at` on **both** paths. Prevents commit and a 02:00 poller
run from submitting twice within one hour.

**Commit vs poller:** `schedule` and `next_due_at` apply to the **poller only**. The commit hook does
not read `next_due_at` and does not advance it after a run.

#### 5.2.5 State materialization (where `next_due_at` is written)

`next_due_at` lives only on **table-level** `table_maintenance_state` rows
(`metalake_id`, `table_identifier`, `policy_id`). A policy attached to a schema or catalog does
**not** store due time on that metadata object.

| Attachment grain                              | When state rows are written                             | Behavior                                                                                                                                                                               |
| --------------------------------------------- | ------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Table**                                     | **Immediately** on associate / schedule change / detach | O(1) UPSERT or DELETE one `(table, policy_id)` row; set `next_due_at = nextOccurrence(schedule)` on create/update                                                                      |
| **Above table** (schema / catalog / metalake) | **Timed discovery** (§5.3.5)                            | Bind records the Policy association only. Discovery lists tables in scope, resolves `effective_policy` (nearest-wins, §5.2.3), and INSERT/UPSERT missing state rows with `next_due_at` |

```text
effective_policy(table, type) → policy_id   // nearest Active along table → schema → catalog → metalake

UPSERT table_maintenance_state
  (metalake_id, table_identifier, policy_id, state='IDLE',
   next_due_at = nextOccurrence(schedule), …)
```

Only **one** state row per `(table, maintenance_type)` effective policy — ancestors covered by a
nearer attachment are not duplicated.

**Optional create-table hook (§5.3.5):** after IRC `createTable` succeeds, if an ancestor carries a
maintenance policy, UPSERT the new table's state row(s). Shortens the wait for the next discovery
cycle for tables created through IRC; discovery remains required for tables that appear only in the
Iceberg/HMS catalog.

---

### 5.3 Scheduled path: `MaintenancePoller` (poll + `takePendingDue`)

Timed maintenance uses the same coordination model as **`IcebergCleanupManager`**: every Gravitino
node runs worker loops that poll the entity DB for **due rows** and claim them with compare-and-swap
(§5.3).

#### 5.3.1 Poller lifecycle

On TMS plugin start (when `gravitino.maintenance.poller.enabled=true`, §8.1):

1. Start `workerThreads` worker loops (daemon threads, like `iceberg-cleanup-worker`).
2. Start one scheduler thread for **heartbeat renewal** on owned rows (`refreshClaimHeartbeats`).
   The renewer covers **both** poller workers and commit-path executor tasks that hold a claim
   (§5.4.1, §6.1) via a shared in-process claim registry.
3. Start one **discovery** loop / scheduled task (§5.3.5) on `discoveryIntervalSecs`.
4. Each worker iteration:
   - If cluster in-flight jobs ≥ `maxConcurrentJobs` (§5.3.3): `sleep(pollIntervalMs)` and continue.
   - `TableMaintenanceStateStore.takePendingDue(now, heartbeatTimeoutMs, candidateWindow)` —
     one **track** per call (compaction / Track A / Track B), see §5.3.2.
   - If a row is claimed (`rows_affected = 1`): register the claim with the heartbeat renewer;
     run `MaintenanceEvaluateSubmitPipeline` for that `(table, policy)`.
   - If nothing due: `sleep(pollIntervalMs)`.
5. On plugin shutdown: stop workers and discovery; in-flight claims expire via `heartbeatTimeoutMs`
   and are reclaimed by peers.

`takePendingDue` and discovery are **separate**:

| Loop           | Default interval                        | Work                                                       |
| -------------- | --------------------------------------- | ---------------------------------------------------------- |
| Claim due work | `pollIntervalSecs` = **30**             | Select existing state rows with `next_due_at <= now`       |
| Discovery      | `discoveryIntervalSecs` = **3600** (1h) | List tables in attachment scope; INSERT missing state rows |

#### 5.3.2 Due rows (`next_due_at`)

Each `table_maintenance_state` row for an **attached, enabled** policy carries:

| Field                                | Role                                                                                    |
| ------------------------------------ | --------------------------------------------------------------------------------------- |
| `next_due_at`                        | Epoch millis when this row becomes eligible for `takePendingDue` (from §5.2.4 schedule) |
| `state`                              | `IDLE` (claimable) or `RUNNING` (a node owns evaluate → submit)                         |
| `claim_lease_expires_at` / heartbeat | Reclaim stale `RUNNING` like `iceberg_cleanup_job.heartbeat_at`                         |

**When `next_due_at` is set:**

- When a state row is materialized (table attach or discovery, §5.2.5): `next_due_at = nextOccurrence(schedule)`.
- After successful maintenance: `next_due_at = nextOccurrence(schedule, after = job_finished_at)`.
- **All four policy types** are eligible for `takePendingDue` when
  `next_due_at <= now` and `enabled = true`.

**Candidate selection:** run **separate** `takePendingDue` queries per track so ranking does not mix
signals. Ranking columns such as `health_score` / `last_orphan_success_at` are derived or joined —
they are not required columns of `table_maintenance_state` (§6.2).

```sql
-- Compaction track
SELECT … FROM table_maintenance_state s
 JOIN policy_meta p ON … AND p.policy_type = 'system_iceberg_compaction'
 WHERE … /* due + IDLE or stale RUNNING */
 ORDER BY health_score DESC
 LIMIT :candidateWindow

-- Track A (manifest + expire): soft order — prefer manifest over expire for the same table
SELECT … FROM table_maintenance_state s
 JOIN policy_meta p ON … AND p.policy_type IN (
   'system_iceberg_rewrite_manifests', 'system_iceberg_snapshot_expiration')
 WHERE … /* due + IDLE or stale RUNNING */
   AND NOT (
     /* skip expire while this table still has a due or RUNNING manifest row */
     p.policy_type = 'system_iceberg_snapshot_expiration'
     AND EXISTS (
       SELECT 1 FROM table_maintenance_state m
        JOIN policy_meta pm ON pm.policy_id = m.policy_id
       WHERE m.metalake_id = s.metalake_id
         AND m.table_identifier = s.table_identifier
         AND pm.policy_type = 'system_iceberg_rewrite_manifests'
         AND (m.next_due_at <= :now OR m.state = 'RUNNING')
     )
   )
 ORDER BY
   CASE p.policy_type
     WHEN 'system_iceberg_rewrite_manifests' THEN 0 ELSE 1 END,
   health_score DESC
 LIMIT :candidateWindow

-- Track B (orphan)
SELECT … FROM table_maintenance_state s
 JOIN policy_meta p ON … AND p.policy_type = 'system_iceberg_orphan_file_removal'
 WHERE … /* due + IDLE or stale RUNNING */
 ORDER BY last_orphan_success_at ASC
 LIMIT :candidateWindow
```

Then for each candidate row, CAS claim (same predicate as the SELECT eligibility):

```sql
UPDATE table_maintenance_state
   SET state = 'RUNNING',
       claimed_by = :nodeId,
       claim_lease_expires_at = :now + :leaseMs,
       updated_at = :now
 WHERE metalake_id = ? AND table_identifier = ? AND policy_id = ?
   AND (state = 'IDLE'
        OR (state = 'RUNNING' AND claim_lease_expires_at < :now))
```

(`IcebergCleanupJobStore.takePendingJob` / `markRunning` pattern.)

**After claim — interval gate:** if `minIntervalMs` has not elapsed since `last_job_id` finished,
do **not** submit. Release the row to `IDLE` and set
`next_due_at = max(nextOccurrence(schedule, after = now), last_finished_at + minIntervalMs)`
so the poller does not immediately re-claim the same row. Applies to all policy types on the
poller path (including orphan, §5.6).

#### 5.3.3 Multi-node behavior

```text
Node A worker claims table1.compaction policy row (02:00 due)
Node B worker claims table2.manifest policy row   ← parallel, no leader
Node C worker claims table3.expire policy row
Node D worker claims table4.orphan policy row

Node A dies mid-run → heartbeat expires → Node B takePendingDue reclaims table1 row
```

`maxConcurrentJobs` (§8.1) caps **cluster-wide** in-flight maintenance Spark jobs. Before claiming
or submitting, each worker reads:

```sql
SELECT COUNT(*) FROM table_maintenance_state
 WHERE state = 'RUNNING' AND job_id IS NOT NULL
```

If `COUNT >= maxConcurrentJobs`, the worker does not claim new rows (and does not submit) until a
slot frees. This is approximate under races but bounds load without a separate leader or lock
service; overshoot of a few jobs is acceptable for maintenance.

#### 5.3.4 External clock APIs (out of scope)

Do **not** expose `POST …/maintenance/run-due` (or CronJob-driven scheduled-run) as an alternate
timed clock. The built-in `MaintenancePoller` is the only schedule driver. Ops APIs in §7
remain for manual / CLI-replacement runs, not for replacing the poller.

#### 5.3.5 Scope discovery (above-table attachments)

Discovery expands schema / catalog / metalake policy attachments into **table-level** state rows
(§5.2.5). It does **not** replace `takePendingDue`. **Why discovery (vs Glue copy / Amoro merge /
cron full-scope scan):** §4.7.

**Catalog source:** list tables from the **Iceberg catalog backend** used by IRC (for example the
same Hive Metastore). Do **not** limit discovery to Gravitino `table_meta` alone — tables that exist
only in HMS/Iceberg must still receive state rows for scheduled maintenance.

**One discovery round (illustrative):**

```text
1. Load enabled maintenance policies and their attachments (schema / catalog / metalake / table)
2. For each above-table attachment, listTables(scope) via Iceberg/HMS
3. For each table: policy_id = effective_policy(table, type)   // nearest-wins
4. If no table_maintenance_state row for (table, policy_id):
     INSERT … next_due_at = nextOccurrence(schedule)
5. Reconcile (required): for each table in scope, DELETE state rows whose policy_id is no longer
   the effective policy for that maintenance type (replaced or detached); DROP rows for tables
   that left the scope when the attachment was removed
6. Cap work with discoveryBatchSize per round
```

**Optional IRC create-table hook:** on successful `createTable` through IRC, UPSERT state rows for
any effective ancestor maintenance policies. Complements discovery for IRC-created tables; discovery
still covers tables that appear in Iceberg/HMS without an IRC create.

**Examples (scheduled compaction on a schema):**

| Situation                                                                      | Expected                                                                            |
| ------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------- |
| Table created outside IRC; later **commit via IRC**                            | Commit path: event INSERT → compaction accelerator (§5.4.1)                         |
| Table created outside IRC; **no** IRC commits; schema has scheduled compaction | Discovery sees the table in HMS → state row → poller runs compaction when due       |
| Table created outside IRC; commits also bypass IRC                             | No commit-path compaction; scheduled path still works if discovery listed the table |

---

### 5.4 Compaction: commit path + poller schedule (option B)

Compaction is the **only** policy type with two wake sources: the IRC commit path (§5.4.1) and the
poller schedule (§5.4.2).

#### 5.4.1 Commit path (compaction only)

```text
IRC commit succeeded (same JVM)
  └─ IRC post-commit hook (§5.1.1)
        │
        ├─ INSERT table_maintenance_event (§6.3)   ← one row per commit; never UPDATE
        └─ in-process callback / SPI
        │
        v
 IcebergCommitEventHandler
        │
              └─ enqueue on bounded executor (required):
                    resolve effective system_iceberg_compaction policy (§5.2.3)
                    if policy.enabled = false → return
                    UPSERT table_maintenance_state if missing
                      (next_due_at = nextOccurrence(schedule); so claim works before discovery)
                    minIntervalMs gate (event.created_at, last_job_id — §6.3)
                    claim compaction state row (§6.1): set claimed_by; register with
                      shared refreshClaimHeartbeats renewer for the hold duration
                    Recommender → submit builtin-iceberg-compaction
                    record job_run_meta (§6.5); clear claimed_by on release to IDLE
                    // do not advance next_due_at — poller owns schedule (§5.2.4)
```

1. IRC hook returns after the event **INSERT** and callback hand-off — **no** evaluate on the IRC
   thread.
2. **No `next_due_at` check** on this path; `minIntervalMs` (using `event.created_at`) and
   Recommender decide whether to submit.
3. If discovery has not yet created the state row (HMS table, first IRC commit), the executor
   **UPSERTs** it before claim so commit-path compaction does not wait for the discovery interval.
4. **Best effort** (§10): if the executor queue is full, the event row remains; the next commit or
   **poller schedule** can still drive compaction.

On multiple nodes, IRC replicas may each INSERT events for commits they serve; the executor uses the
shared row **claim** (§6.1) before submit so two replicas do not double-submit for the same table.

#### 5.4.2 Poller path (scheduled compaction)

When `nightly_compaction` is **Daily · 02:00**, the poller claims compaction rows with
`next_due_at <= now`, then runs the same evaluate → submit pipeline. After success,
`next_due_at = nextOccurrence(schedule)` (§5.2.4).

- **Inactive tables** with no recent commits still compact at 02:00.
- **Active tables** may have compacted via commit; `minIntervalMs` + Recommender may no-op.

Manifest / expire / orphan: **poller only** — the commit hook does not touch them.

#### 5.4.3 When commit and poller meet

If commit and poller try the same compaction row at once: **claim** (`state` CAS) plus
`minIntervalMs` and in-flight `job_id` prevent duplicate Spark jobs (§6.1). No extra commit-side
scheduling logic is required.

---

### 5.5 Hot pipeline (scheduled — Track A)

Track A covers **manifest rewrite** and **snapshot expiry**. Each type still has its own
`table_maintenance_state` row and is claimed **independently** as one `(table, policy)` — there is
**no** atomic multi-policy claim.

**Soft order (desired, not a single pipeline claim):**

1. Prefer claiming **manifest** before **expire** for the same table (§5.3.2 Track A query): skip an
   expire candidate while that table still has a due or `RUNNING` manifest row.
2. Operators should schedule manifest earlier than expire when both attach to the same scope
   (for example Daily · 04:00 manifests, later expire).

Compaction due rows are claimed on the compaction track (§5.4.2), not as Track A steps.

**Why prefer manifest before expire:**

- Rewrite manifests after the file set has stabilized (often after compaction on the write path or
  the compaction poller).
- Expire snapshots after manifest rewrite so metadata reflects the current file set.

Each type has its own **`minIntervalMs`** (§8.3). Within Track A, candidates rank **worst-first**
using refreshed statistics.

---

### 5.6 Orphan cleanup track (scheduled — Track B)

Orphan cleanup runs on a **separate track**, not as step 3 of the hot pipeline.

| Aspect           | Track A (hot pipeline)               | Track B (orphan)                                               |
| ---------------- | ------------------------------------ | -------------------------------------------------------------- |
| Operations       | manifest rewrite, snapshot expire    | `remove_orphan_files` only                                     |
| Candidate signal | metrics / time due (`minIntervalMs`) | per-table `minIntervalMs` since last **successful** orphan job |
| Queue order      | worst-first (health score)           | **oldest cleanup first**                                       |
| Shared limits    | `maxConcurrentJobs` (§8.1)           | same                                                           |

**Per-table eligibility:** a table becomes eligible again only after **`minIntervalMs`** since its
last successful orphan run (default seven days — §8.3). Listings spread across nights instead of one
global orphan night. If the poller claims an orphan row whose interval has not elapsed, it **releases**
the claim and pushes `next_due_at` forward (§5.3.2 interval gate) — it does not leave the row
immediately re-claimable.

**`olderThan` server-side enforcement:** TMS enforces a **minimum floor** on every evaluate and
policy write (§8.1).

---

### 5.7 Internal structure

| Part                                | Responsibility                                                                     |
| ----------------------------------- | ---------------------------------------------------------------------------------- |
| `TableMaintenanceRESTFeature`       | Jersey 2 `Feature`; commit callback, poller lifecycle, ops resources (§7).         |
| `IcebergCommitEventHandler`         | In-process callback; enqueues compaction pipeline on bounded executor (§5.4.1).    |
| `MaintenancePoller`                 | Worker loops + heartbeat + discovery; `takePendingDue` → evaluate → submit (§5.3). |
| `MaintenanceEvaluateSubmitPipeline` | Interval gate → Recommender → submit for one claimed `(table, policy)`.            |
| `TableMaintenanceEventStore`        | IRC hook INSERT for `table_maintenance_event`; rename / drop rewrite (§6.3–§6.4).  |
| `TableMaintenanceStateStore`        | `table_maintenance_state` upsert / `takePendingDue` / heartbeat / rename (§6).     |
| `IcebergTableLifecycleHook`         | IRC rename/drop: rewrite or purge state and event rows (§6.4).                     |
| Existing optimizer classes          | `Updater`, `Recommender`, providers, `JobSubmitter`.                               |

Pattern reference: `IcebergCleanupManager`, `IcebergCleanupJobStore.takePendingJob` (§5.3).

---

### 5.8 User process

1. Operator enables the TMS REST plugin (`extensionPackages`) and `iceberg-rest` **in the same JVM**,
   and turns on in-process commit callbacks (§5.1.1 / §8.2).
2. Operator applies a **`standard` profile** or creates four policies and attaches them at the
   intended grains via metalake Policy APIs.
3. Operator creates policies with schedules in the UI (§5.2.4), for example:
   `nightly_compaction` Daily · 02:00, `weekly_snapshot_expiry` Sun · 03:00,
   `manifest_rewrite` Daily · 04:00, `orphan_cleanup` Weekly (Paused = `enabled: false`).
4. Operator enables the **maintenance poller** (`gravitino.maintenance.poller.enabled=true`, §8.1).
5. Engines write through Gravitino Iceberg REST. On commit success, IRC **INSERTs**
   `table_maintenance_event` and invokes the in-process callback; TMS runs compaction evaluate →
   submit on the bounded executor (§5.4.1, §6.3).
6. On each poll cycle on every node, workers claim **due rows** (all four types when
   `next_due_at <= now`) and submit within `maxConcurrentJobs` (§5.3).
7. Operators observe runs in the Gravitino **Jobs** UI / APIs. Manual runs remain available through
   ops APIs (§7) and `runJob`.

---

## 6. Multi-node coordination (shared claim)

On **multiple** Gravitino / TMS nodes, commit compaction and poller workers may run on **any**
replica. Without coordination, two nodes could both evaluate and submit the same policy's job for the
same table.

**Approach:** shared table `table_maintenance_state` in the Gravitino entity DB. Table identity uses
a **normalized string `table_identifier`** (`catalog.schema.table`), **not** `table_meta.table_id`.

| Table                     | Role                                                                                    |
| ------------------------- | --------------------------------------------------------------------------------------- |
| `table_maintenance_event` | **Commit log** — one INSERT per successful Iceberg commit (§6.3)                        |
| `table_maintenance_state` | Multi-node **claim**, in-flight `job_id`, finished `last_job_id` per policy (§6.1–§6.2) |

Every node polls; **per-row CAS** picks the winner (§5.3). Primary key is
`(metalake_id, table_identifier, policy_id)` — **one row per effective maintenance policy** for a
table.

### 6.1 Claim flow (`takePendingDue` / commit path)

**Poller path** (same CAS as `markRunning` on `iceberg_cleanup_job`):

```text
Node A / Node B — both poll due rows
        │
        ├─ both SELECT candidate rows (next_due_at <= now, IDLE or stale RUNNING) per track
        ├─ both attempt per-row claim:
        │     UPDATE … SET state=RUNNING, claimed_by=:nodeId,
        │                 claim_lease_expires_at=now+leaseMs, updated_at=now
        │     WHERE metalake_id=? AND table_identifier=? AND policy_id=?
        │       AND (state=IDLE OR (state=RUNNING AND claim_lease_expires_at < now))
        │     ├─ Node A: rows_affected = 1 → register with refreshClaimHeartbeats
        │     │          → evaluate → submit → IDLE, clear claimed_by, set next_due_at
        │     │          (or interval gate fail → IDLE + push next_due_at, §5.3.2)
        │     └─ Node B: 0 rows → try next candidate
        v
refreshClaimHeartbeats on owned rows (poller + commit executor; peer cannot steal while lease fresh)
```

**Commit compaction path** (driven by `table_maintenance_event` + in-process callback) uses the same
`state` / `claimed_by` / `claim_lease_expires_at` columns and the same CAS as the poller before
submit. While the bounded executor holds the claim, it **must** register the row with the shared
`refreshClaimHeartbeats` renewer (same thread as §5.3.1) so peers do not reclaim mid-evaluate.
It does **not** read `next_due_at` and does **not** update `next_due_at` after a successful run —
only the poller advances `next_due_at` from the policy schedule (§5.2.4).

On multiple IRC replicas, each successful commit INSERTs an event row; claim + `minIntervalMs`
still bound duplicate submissions for the same `(table, compaction_policy)`.

Gate checks alone are insufficient (read race). **Claim is the write lock** for that policy row.

### 6.2 State table (shared store)

**Table name:** `table_maintenance_state`

| Column                       | Type                       | Notes                                                              |
| ---------------------------- | -------------------------- | ------------------------------------------------------------------ |
| `metalake_id`                | `BIGINT UNSIGNED NOT NULL` | Metalake that owns the maintenance policy                          |
| `table_identifier`           | `VARCHAR(512) NOT NULL`    | Normalized `catalog.schema.table`                                  |
| `policy_id`                  | `BIGINT UNSIGNED NOT NULL` | Real `policy_meta.policy_id`                                       |
| `state`                      | `VARCHAR(16) NOT NULL`     | `IDLE` / `RUNNING` (per policy row)                                |
| `next_due_at`                | `BIGINT NOT NULL`          | Epoch millis; poller selects rows with `next_due_at <= now` (§5.3) |
| `updated_at`                 | `BIGINT NOT NULL`          | Epoch millis; claim / reclaim / heartbeat                          |
| `job_id`                     | `BIGINT UNSIGNED NULL`     | In-flight job (`job_run_meta.job_run_id`)                          |
| `last_job_id`                | `BIGINT UNSIGNED NULL`     | Last finished job; `job_finished_at` drives min-interval           |
| `last_measured_snapshot_id`  | `BIGINT NULL`              | Snapshot id at last successful evaluate (§10.3)                    |
| `claimed_by`                 | `VARCHAR(128) NULL`        | Node / worker id holding `RUNNING`; cleared on release (§6.1)      |
| `claim_lease_expires_at`     | `BIGINT NULL`              | Epoch millis; reclaim `RUNNING` after expiry (§10.3)               |
| `submission_idempotency_key` | `VARCHAR(64) NULL`         | Written before job submit; dedupe boundary (§10.3)                 |

**Primary key:** (`metalake_id`, `table_identifier`, `policy_id`).

Illustrative MySQL DDL:

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_state` (
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `table_identifier` VARCHAR(512) NOT NULL COMMENT 'normalized catalog.schema.table',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id from policy_meta',
    `state` VARCHAR(16) NOT NULL COMMENT 'IDLE|RUNNING',
    `next_due_at` BIGINT(20) NOT NULL COMMENT 'poller eligibility time, epoch millis',
    `updated_at` BIGINT(20) NOT NULL COMMENT 'last state/heartbeat upsert, epoch millis',
    `job_id` BIGINT(20) UNSIGNED NULL COMMENT 'in-flight job_run_id',
    `last_job_id` BIGINT(20) UNSIGNED NULL COMMENT 'last finished job_run_id',
    `last_measured_snapshot_id` BIGINT(20) NULL COMMENT 'snapshot id at last evaluate',
    `claimed_by` VARCHAR(128) NULL COMMENT 'node/worker holding RUNNING claim',
    `claim_lease_expires_at` BIGINT(20) NULL COMMENT 'claim lease expiry, epoch millis',
    `submission_idempotency_key` VARCHAR(64) NULL COMMENT 'idempotency key before job submit',
    PRIMARY KEY (`metalake_id`, `table_identifier`, `policy_id`),
    KEY `idx_due_state` (`next_due_at`, `state`),
    KEY `idx_state_updated` (`state`, `updated_at`),
    KEY `idx_table_identifier` (`table_identifier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'TMS per-policy due time, claim, and job state';
```

### 6.3 Commit log (`table_maintenance_event`)

The **IRC post-commit hook** INSERTs one row for each successful Iceberg commit. TMS does **not**
write or UPDATE that row. Like `table_metrics`, rows key by **`table_identifier`** string — **not**
`table_meta.table_id` — so IRC tables without Gravitino table metadata still persist. The row records
that a commit happened for that table; it does not store `snapshot_id`.

```text
IRC post-commit hook
      │
      ├─ INSERT table_maintenance_event   ← one new row; never UPDATE
      └─ in-process callback → TMS bounded executor → pipeline (§5.4.1)
```

| Column             | Type                       | Notes                                    |
| ------------------ | -------------------------- | ---------------------------------------- |
| `event_id`         | `BIGINT UNSIGNED NOT NULL` | Surrogate PK (auto-increment)            |
| `metalake_id`      | `BIGINT UNSIGNED NOT NULL` | Metalake from config / policy resolution |
| `table_identifier` | `VARCHAR(512) NOT NULL`    | Normalized `catalog.schema.table`        |
| `created_at`       | `BIGINT NOT NULL`          | Epoch millis when the row was inserted   |

**Primary key:** (`event_id`). **Index:** (`metalake_id`, `table_identifier`, `created_at`).

Every commit INSERTs a new row. Claim, in-flight `job_id`, and `last_job_id` on
`table_maintenance_state` still prevent double-submit (§6.1–§6.2). Interval checks on the commit
path join the event row to state and `job_run_meta`:

```text
table_maintenance_event e
  JOIN table_maintenance_state s
    ON s.metalake_id = e.metalake_id
   AND s.table_identifier = e.table_identifier
   AND s.policy_id = :compaction_policy_id
  LEFT JOIN job_run_meta j
    ON j.job_run_id = s.last_job_id
```

`j.job_finished_at` is the end time of that policy's previous finished job (`NULL` when
`last_job_id` is null). Resolve `minIntervalMs` for the policy's task type (§8.3). When
`s.last_job_id` is **null** (LEFT JOIN yields no job row), the interval gate **passes**. When
`s.job_id` is null, `s.last_job_id` is set, and `e.created_at - j.job_finished_at > minIntervalMs`,
the policy has not completed another run after the interval elapsed. A non-null `s.job_id` means a
job is still in flight.

Illustrative MySQL DDL:

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_event` (
    `event_id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'commit event id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `table_identifier` VARCHAR(512) NOT NULL COMMENT 'normalized catalog.schema.table',
    `created_at` BIGINT(20) NOT NULL COMMENT 'insert time epoch millis',
    PRIMARY KEY (`event_id`),
    KEY `idx_table_created` (`metalake_id`, `table_identifier`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'TMS commit log; one INSERT per Iceberg commit';
```

**Retention:** event rows are append-only audit of commits. A background cleaner (or TTL config
follow-up) may DELETE rows older than a configurable window; compaction decisions use
`table_maintenance_state` + `job_run_meta`, not replay of the full event log.

### 6.4 Table rename / drop lifecycle (required with string keys)

Because TMS keys by **`table_identifier`** (not a stable `table_id`), a rename would otherwise orphan
claim / `job_id` / `last_job_id` rows and break cooldown / in-flight gates.

**Hook:** after a successful Iceberg table rename (or drop), IRC invokes an in-process
`IcebergTableLifecycleHook` registered by the TMS plugin (same pattern as the commit-event callback
— §5.1.1).

#### Rename

1. **`table_maintenance_state`:** `UPDATE … SET table_identifier = new WHERE … table_identifier = old`.
2. **`table_maintenance_event`:** same rewrite for the metalake so later joins still find those commits.

#### Drop

1. **`table_maintenance_state`:** `DELETE` all rows for `(metalake_id, table_identifier)`.
2. **`table_maintenance_event`:** `DELETE` rows for `(metalake_id, table_identifier)`.

### 6.5 Job run history (`job_run_meta`)

Every maintenance submission — Spark job or in-process execution — creates a `job_run_meta` row.
On job finish, TMS updates `last_job_id`, clears in-flight `job_id`, and records
`last_measured_snapshot_id` when evaluate completes (§10.3).

---

## 7. Optimizer CLI replacement APIs

The commit path and poller do **not** call these routes. They replace the
`gravitino-optimizer` CLI for operators and scripts on the main webserver (**8090**).

| CLI `--type`              | Method | Path                                           |
| ------------------------- | ------ | ---------------------------------------------- |
| `submit-strategy-jobs`    | `POST` | `/api/maintenance/table/ops/strategy-jobs`     |
| `submit-update-stats-job` | `POST` | `/api/maintenance/table/ops/update-stats-jobs` |
| `update-statistics`       | `POST` | `/api/maintenance/table/ops/statistics`        |
| `append-metrics`          | `POST` | `/api/maintenance/table/ops/metrics`           |
| `monitor-metrics`         | `POST` | `/api/maintenance/table/ops/metrics/monitor`   |
| `list-table-metrics`      | `GET`  | `/api/maintenance/table/ops/metrics/tables`    |
| `list-job-metrics`        | `GET`  | `/api/maintenance/table/ops/metrics/jobs`      |

---

## 8. Configuration

### 8.1 Enablement keys (`gravitino.conf`)

| Key                                                  | Default     | Description                                                                        |
| ---------------------------------------------------- | ----------- | ---------------------------------------------------------------------------------- |
| `gravitino.server.rest.extensionPackages`            | none        | TMS Feature package.                                                               |
| `gravitino.auxService.names`                         | none        | Must include `iceberg-rest` when using IRC.                                        |
| `gravitino.maintenance.claimLeaseMs`                 | `300000`    | Claim lease length; sets `claim_lease_expires_at` (§6.1, §10.3).                   |
| `gravitino.maintenance.executor.threads`             | `4`         | Bounded executor worker threads (§5.4.1).                                          |
| `gravitino.maintenance.executor.queueSize`           | `10000`     | Bounded executor queue depth.                                                      |
| `gravitino.maintenance.poller.enabled`               | `true`      | Enable `MaintenancePoller` worker loops (§5.3).                                    |
| `gravitino.maintenance.poller.workerThreads`         | `2`         | Poller worker threads per node (like `ASYNC_CLEANUP_WORKER_THREADS`).              |
| `gravitino.maintenance.poller.pollIntervalSecs`      | `30`        | Sleep when no due row claimed (like `ASYNC_CLEANUP_POLL_INTERVAL_SECS`).           |
| `gravitino.maintenance.poller.discoveryIntervalSecs` | `3600`      | How often discovery expands above-table attachments into state rows (§5.3.5).      |
| `gravitino.maintenance.poller.discoveryBatchSize`    | `500`       | Max new/reconciled tables per discovery round.                                     |
| `gravitino.maintenance.poller.heartbeatTimeoutSecs`  | `300`       | Reclaim stale `RUNNING` rows (like `ASYNC_CLEANUP_HEARTBEAT_TIMEOUT_SECS`).        |
| `gravitino.maintenance.poller.candidateWindow`       | `8`         | Max due rows considered per `takePendingDue` call.                                 |
| `gravitino.maintenance.poller.maxConcurrentJobs`     | `10`        | Max cluster in-flight jobs: count `state=RUNNING AND job_id IS NOT NULL` (§5.3.3). |
| `gravitino.maintenance.poller.maintenanceWindow`     | none        | Optional UTC window; poller skips submit outside window.                           |
| `gravitino.maintenance.orphan.olderThanMinMs`        | `259200000` | Server-side minimum `olderThan` (3 days) for orphan cleanup (§5.6).                |

Per-table cadence is **`minIntervalMs`** / schedule-driven **`next_due_at`** on each state row, not
the poller interval. `pollIntervalSecs` only controls how often nodes **claim due work**;
`discoveryIntervalSecs` controls how often above-table attachments are expanded into state rows.

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.auxService.names = iceberg-rest
gravitino.iceberg-rest.tableMaintenance.inProcess = true
gravitino.maintenance.claimLeaseMs = 300000
gravitino.maintenance.executor.threads = 4
gravitino.maintenance.poller.enabled = true
gravitino.maintenance.poller.workerThreads = 2
gravitino.maintenance.poller.pollIntervalSecs = 30
gravitino.maintenance.poller.discoveryIntervalSecs = 3600
gravitino.maintenance.poller.maxConcurrentJobs = 10
```

### 8.2 Iceberg REST → TMS in-process event keys

| Key (illustrative)                                  | Default | Description                                   |
| --------------------------------------------------- | ------- | --------------------------------------------- |
| `gravitino.iceberg-rest.tableMaintenance.inProcess` | `false` | IRC invokes compaction callback after commit. |

### 8.3 Task types and minimum interval (per policy type)

Each maintenance policy type has its own `minIntervalMs`, compared per `(table, policy_id)` via
`last_job_id` → `job_run_meta.job_finished_at`. Null `last_job_id` → gate passes.

| Task type          | Policy type                          | Code default `minIntervalMs` |
| ------------------ | ------------------------------------ | ---------------------------- |
| `compaction`       | `system_iceberg_compaction`          | `3600000` (1 hour)           |
| `snapshot-expiry`  | `system_iceberg_snapshot_expiration` | `86400000` (1 day)           |
| `manifest-rewrite` | `system_iceberg_rewrite_manifests`   | `86400000` (1 day)           |
| `orphan-cleanup`   | `system_iceberg_orphan_file_removal` | `604800000` (7 days)         |

**Resolution order:** table property override → global `gravitino.conf` key → code default.

---

## 9. Work Plan and Checklist

### 9.1 Suggested Work Plan

| Phase | Work item                        | Notes                                                                                                                |
| ----- | -------------------------------- | -------------------------------------------------------------------------------------------------------------------- |
| 1     | In-process plugin + commit event | Feature; IRC hook INSERT + callback; bounded executor (§5.4.1, §6.3).                                                |
| 2     | State + event tables + claim     | `table_maintenance_event` (§6.3); `table_maintenance_state` + materialization (§5.2.5, §6.2); nearest-wins (§5.2.3). |
| 3     | `MaintenancePoller` + discovery  | `takePendingDue`, discovery from Iceberg/HMS (§5.3.5); mirror `IcebergCleanupManager`.                               |
| 4     | Compaction on poller schedule    | Poller claims compaction due rows; commit path unchanged (§5.4).                                                     |
| 5     | Track A / B orchestration        | Hot pipeline + orphan ranking (§5.5–§5.6).                                                                           |
| 6     | Maintenance profile API          | `standard` one-step setup (§5.2.2).                                                                                  |
| 7     | Ops APIs                         | §7.                                                                                                                  |
| 8     | Hardening                        | Multi-node claim tests, metrics, fault tolerance (§10).                                                              |

#### Phase 1 checklist

- [ ] EntityStore migration for **`table_maintenance_event`** (§6.3).
- [ ] IRC post-commit hook **INSERTs** one event row per commit; TMS does not UPDATE it.
- [ ] `IcebergCommitEventHandler` + in-process callback (`tableMaintenance.inProcess`, §8.2).
- [ ] Bounded executor; IRC thread does not run Recommender / submit.

#### Phase 3 checklist

- [ ] `MaintenancePoller`: worker loops, `pollIntervalSecs`, `workerThreads` (§5.3.1).
- [ ] Discovery loop: `discoveryIntervalSecs`, list Iceberg/HMS (not only `table_meta`) (§5.3.5).
- [ ] Table-level attach: immediate state UPSERT; above-table: discovery materializes (§5.2.5).
- [ ] Optional IRC `createTable` hook UPSERT (§5.3.5).
- [ ] `TableMaintenanceStateStore.takePendingDue` — CAS claim like `IcebergCleanupJobStore.takePendingJob`.
- [ ] `refreshClaimHeartbeats` + stale `RUNNING` reclaim (§6.1).
- [ ] `next_due_at` from policy schedule on materialize / job finish (§5.2.4, §5.3.2).
- [ ] Poller claims compaction due rows alongside manifest / expire / orphan (§5.4.2).
- [ ] Hot pipeline soft order: prefer manifest before expire; per-policy claims (§5.5).
- [ ] Orphan track: oldest-cleanup-first; interval gate releases claim and pushes `next_due_at` (§5.3.2, §5.6).
- [ ] `maxConcurrentJobs` via COUNT of RUNNING+`job_id`; unclaimed due rows remain for next poll.
- [ ] Multi-node tests: two nodes poll; one row claimed once; heartbeat reclaim after node kill.
- [ ] Commit path registers claims with shared `refreshClaimHeartbeats` (§5.4.1, §6.1).
- [ ] Discovery reconcile deletes stale `(table, policy)` rows (§5.3.5).

#### Phase 4 checklist

- [ ] Poller claims compaction due rows at schedule; advances `next_due_at` (§5.4.2, §5.2.4).
- [ ] Commit path does not use or advance `next_due_at`; UPSERTs state if missing before claim (§5.4.1).
- [ ] Commit + poller: claim + `minIntervalMs` prevent double-submit (§5.4.3, §6.1).
- [ ] Unit tests: commit thread does not block on evaluate.
- [ ] Commit path ignores manifest / expire / orphan policies.

### 9.2 Review Checklist

| Area            | Checklist                                                                                                            |
| --------------- | -------------------------------------------------------------------------------------------------------------------- |
| Deployment      | `extensionPackages`; IRC colocated in same JVM. Ops on **8090** (§7).                                                |
| Policy          | Four built-in types; precedence nearest-wins (§5.2); table attach immediate; above-table via discovery (§5.2.5).     |
| Trigger         | Compaction: **commit + poller** (§5.4); others: **poller** + schedule / `next_due_at` (§5.2.4, §5.3).                |
| Discovery       | Iceberg/HMS list; separate from `takePendingDue` (§5.3.5).                                                           |
| Multi-node      | **No** maintenance leader; per-row `takePendingDue` like `IcebergCleanupManager` (§5.3, §4.8).                       |
| Executor        | Bounded; evaluation **off** commit thread (§5.4.1).                                                                  |
| Durability      | IRC hook INSERTs `table_maintenance_event` per commit (§6.3); `table_maintenance_state` for claim / schedule (§6.2). |
| Orchestration   | Track A soft order manifest before expire (§5.5); orphan separate track (§5.6).                                      |
| Industry        | §4.5–§4.6 scheduled vs commit; §4.7 discovery; §4.8 multi-node patterns (row CAS chosen).                            |
| Fault tolerance | Poller: at-least-once latest-state; commit: best effort (§10).                                                       |

---

## 10. Fault tolerance and delivery guarantees

### 10.1 Delivery models

| Model                      | TMS target                                                               |
| -------------------------- | ------------------------------------------------------------------------ |
| Best effort                | **Commit compaction path** (§5.4.1)                                      |
| At-least-once latest-state | **`MaintenancePoller` path** — recovery from table state, not event rows |
| Exactly-once job effect    | **Not required** — claims + idempotency key bound duplicates             |

**Commit path:** the event row is durable once INSERT succeeds. If the bounded executor drops the
callback task, the event remains; the next commit INSERT or **poller schedule** can still drive
compaction. Manifest, expire, and orphan run on the poller only.

**Poller path:** coalescing is acceptable — maintenance acts on the table's **current** state.
A due row stays due (`next_due_at` unchanged) until a worker successfully claims and completes it.

### 10.2 Recovery is driven by table state

`table_maintenance_event` records that a commit occurred; **recovery and interval gates** use:

- **Current snapshot id** vs `last_measured_snapshot_id`
- Per-policy **`last_job_id`** / in-flight **`job_id`**
- Per-type **`minIntervalMs`** (commit path joins `event.created_at` — §6.3)

### 10.3 Recovery at failure boundaries

| Boundary                                        | Behavior                                                                                       |
| ----------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| Event INSERT ok before executor runs compaction | Event row kept; next commit or poller schedule retries                                         |
| Node fails holding a claim                      | `claim_lease_expires_at` / heartbeat timeout reclaims `RUNNING`; peer `takePendingDue` retries |
| Interval gate fails after claim                 | Release to `IDLE`; push `next_due_at` forward (§5.3.2) — no tight re-claim loop                |
| All workers at `maxConcurrentJobs`              | Due rows remain; next poll on any node retries                                                 |
| Job accepted before `job_id` recorded           | `submission_idempotency_key` written before submit                                             |

---

## 11. References

1. [Gravitino Iceberg REST service](../docs/iceberg-rest-service.md)
2. [Manage policies in Gravitino](../docs/manage-policies-in-gravitino.md)
3. [Iceberg compaction policy](../docs/iceberg-compaction-policy.md)
4. [Iceberg expire snapshots maintenance job](./iceberg-expire-snapshots-maintenance-job.md)
5. [Iceberg rewrite manifests job](./iceberg-rewrite-manifests-job.md)
6. [Iceberg remove orphan files maintenance job](./iceberg-remove-orphan-files-maintenance-job.md)
7. [Table Maintenance optimizer overview](../docs/table-maintenance-service/optimizer.md)
8. [Amoro AIP-3 – Event-Triggered Optimization](https://cwiki.apache.org/confluence/display/AMORO/AIP-3%3A+Event-Triggered+Optimization+of+Iceberg+Tables+in+Amoro)
9. [Amoro configurations (self-optimizing, table-expire)](https://amoro.apache.org/docs/latest/configurations/)
10. [Floe policies (cron, triggerConditions)](https://github.com/nssalian/floe/blob/main/docs/policies.md)
11. [Floe maintenance trigger API](https://github.com/nssalian/floe)
12. [AWS Glue Iceberg table optimizers](https://docs.aws.amazon.com/glue/latest/dg/table-optimizers.html)
13. [Databricks Iceberg auto compaction](https://docs.databricks.com/aws/en/tables/tune-file-size)
14. [Databricks OPTIMIZE frequency guidance](https://docs.databricks.com/aws/en/tables/operations/optimize)
15. [OpenHouse architecture (Jobs Scheduler / CronJob)](https://github.com/linkedin/openhouse/blob/main/ARCHITECTURE.md)
16. [Apache Iceberg REST Catalog OpenAPI](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
17. Gravitino `IcebergCleanupManager` / `IcebergCleanupJobStore.takePendingJob`
    (`iceberg/iceberg-rest-server/src/main/java/org/apache/gravitino/iceberg/service/cleanup/`)
