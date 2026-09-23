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
4. When Spark maintenance work is needed, submitted work already returns a `jobId` owned by the
   Gravitino job framework. That job-status boundary should stay.

This design turns TMS into a **main-server REST plugin** on port **8090** (same pattern as IdP
via `gravitino.server.rest.extensionPackages`) with colocated IRC, reusing the existing optimizer
execution core. A **scheduler** drives maintenance (§5.3); commits only **mark tables dirty** as a
cheap compaction accelerator (§5.4).

---

## 2. Goals

1. **In-process plugin on the main server**: Load Table Maintenance through
   `gravitino.server.rest.extensionPackages` (Jersey 2 `Feature`, same pattern as IdP) so the IRC
   callback is registered in the main JVM. The commit path does **not** use HTTP. Operator calls that
   replace the optimizer CLI are the ops APIs in **§7**.
2. **Four independent maintenance policies**: One built-in policy **type** per activity
   (`compaction`, `rewrite-manifests`, `snapshot-expiry`, `orphan-removal`). Each attaches and
   schedules on its own grain (for example snapshot retention at catalog, compaction per table).
   No combined `system_iceberg_table_maintenance` storage model (§5.2).
3. **Maintenance profile (convenience)**: A profile such as `standard` creates and attaches all four
   policies with sensible defaults in one step. Profiles are **not** a fifth policy type (§5.2).
4. **Precedence per maintenance type**: For each maintenance type, the **nearest** attachment along
   `table → schema → catalog → metalake` wins. Policies are **not** additive for maintenance (§5.2).
5. **Scheduler-first trigger model**: A configurable scheduler refreshes statistics, evaluates
   effective policies, ranks tables **worst-first**, and submits work within concurrency limits and
   a maintenance window. Commits only set a **dirty** flag (§5.3–§5.4).
6. **Reuse existing optimizer execution core**: The scheduler invokes the same `Updater` /
   `Recommender` / job-submit paths in `maintenance/optimizer` as **in-process methods**.
7. **Job framework compatibility**: Spark maintenance work continues to use the Gravitino job
   framework. TMS returns or records submitted `jobId` values but does not own job status.
8. **Govern Policy reuse**: Maintenance policies stay on existing `policy_meta` and metalake Policy
   APIs (create / alter / enable / disable / associate). TMS does **not** introduce a parallel policy
   store or `/api/maintenance/table/policies` CRUD.
9. **Multi-node safe processing**: Shared DB claims so only one TMS replica runs evaluate → submit for
   a given `(table, policy)` at a time (§6).
10. **Bounded executor on commit path**: Evaluation **must not** run on the IRC commit thread. A
    bounded executor is a **design requirement**, not an implementation detail (§5.4).

---

## 3. Non-Goals

1. **Standalone maintenance daemon**: No separate process or
   `gravitino-iceberg-rest-server.sh`-style entrypoint.
2. **Dedicated auxiliary HTTP listener**: No `GravitinoAuxiliaryService`, no isolated
   `gravitino.maintenance.classpath`, and no dedicated TMS port (for example **9301**). TMS is not
   a dedicated listener like `iceberg-rest` / `lance-rest`.
3. **Provider SPI rewrite**: Does not replace `StatisticsUpdater`, `StatisticsCalculator`,
   `StatisticsProvider`, `StrategyProvider`, `TableMetadataProvider`, or `JobSubmitter` contracts
   used by the maintenance pipeline.
4. **Engine-side commit report path**: Engines that bypass Gravitino Iceberg REST are out of scope
   for the dirty-flag path.
5. **Commit-path HTTP or Kafka**: No `POST …/events/iceberg-commit`, no health resource, and no Kafka
   produce/consume path. Commit handling is **in-process only** (§5.1.1). APIs that replace the
   optimizer CLI are **§7**, and they are not a commit ingress. Remote IRC / cross-JVM delivery is
   out of scope (follow-up if needed).

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
sets a **dirty** flag and returns; the **scheduler** runs evaluate → submit off the commit thread.

**Pros:** No extra process or port; no remote event hop on the commit path; reuses Policy + Jobs on
the same server; matches plugin packaging.

**Decision:** **Chosen**.

### 4.3 Option C: Independent long-running Table Maintenance Service process

**Pros:** Full JVM isolation.

**Cons:** Extra deployable; duplicates server lifecycle patterns already covered by the main
webserver plugin.

**Decision:** Rejected. Prefer the in-process plugin on the main server.

### 4.4 Option E: Dedicated aux Jetty listener (:9301)

Implement `GravitinoAuxiliaryService` with `shortName() = "maintenance"`, expose a dedicated Jetty
listener (default **9301**), and keep TMS off the main 8090 JAX-RS app.

**Pros:** Classpath isolation similar to `iceberg-rest` / `lance-rest`.

**Cons:** Extra port and aux enablement; diverges from plugins that already extend
**8090** via `extensionPackages`.

**Decision:** Rejected. Prefer Option B.

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
        └─ IRC post-commit hook (cheap)
                │
                └─ UPSERT table_maintenance_state.dirty=true
                   (bounded executor — no evaluate on commit thread)

MaintenanceScheduler (configurable cadence — main path)
        │
        ├─ Track A — hot pipeline (§5.5):
        │     candidates: dirty tables + compaction/manifest/expire due
        │     refresh statistics (Updater)
        │     rank tables worst-first (health score)
        │     per table: compact → manifests → expire (Recommender → SQL)
        │
        ├─ Track B — orphan cleanup (§5.6):
        │     candidates: tables with orphan policy + per-table minIntervalMs elapsed
        │     rank oldest-successful-cleanup first (no cheap metadata score)
        │     remove orphan files SQL (olderThan enforced server-side)
        │
        ├─ shared: maintenance window + maxConcurrentJobs across both tracks
        │     (tables not reached carry over to next tick / next night)
        v
Gravitino Job framework + job_run_meta (every run, including in-process — §6.5)
```

#### 5.1.1 In-process commit callback

Commit signals are delivered **only in-process**. After a successful Iceberg commit, the **IRC
post-commit hook** upserts `dirty=true` on `table_maintenance_state` for that
`table_identifier`, then returns. It does **not** resolve policies, claim rows, refresh statistics,
or submit jobs on the commit thread.

| Requirement | Detail |
| ----------- | ------ |
| Deployment  | IRC (`iceberg-rest`) and the main Gravitino server share **one JVM**. |
| Transport   | In-process callback / SPI only — **no** HTTP `POST …/events/iceberg-commit`, **no** Kafka. |
| Payload     | Normalized `table_identifier` (`catalog.schema.table`). |
| Commit cost | One state upsert per table (dirty flag). **No** per-policy work. **No** `table_maintenance_event` insert. |

---

### 5.2 Policy model

#### 5.2.1 Four built-in policy types

Each activity is a **separate** built-in policy type with its own `content`, schedule /
`minIntervalMs`, and attachment grain:

| Maintenance type | Illustrative policy type | Built-in job template | Typical attachment |
| ---------------- | ------------------------ | --------------------- | ------------------ |
| Compaction | `system_iceberg_compaction` | `builtin-iceberg-compaction` | Table |
| Manifest rewrite | `system_iceberg_rewrite_manifests` | `builtin-iceberg-rewrite-manifests` | Table or schema |
| Snapshot expiry | `system_iceberg_snapshot_expiration` | `builtin-iceberg-expire-snapshots` | Catalog |
| Orphan cleanup | `system_iceberg_orphan_file_removal` | `builtin-iceberg-remove-orphan-files` | Catalog |

See [iceberg-compaction-policy](../docs/iceberg-compaction-policy.md),
[iceberg-expire-snapshots-maintenance-job](./iceberg-expire-snapshots-maintenance-job.md),
[iceberg-rewrite-manifests-job](./iceberg-rewrite-manifests-job.md), and
[iceberg-remove-orphan-files-maintenance-job](./iceberg-remove-orphan-files-maintenance-job.md)
for per-type content fields.

Activities attach at **different grains** in practice: snapshot retention is often one setting for
a whole catalog, while compaction thresholds are tuned per table. A combined policy would force
both onto the same attachment object.

#### 5.2.2 Maintenance profile (one-step setup)

A **profile** such as `standard` is a convenience API that creates four policy instances (or reuses
existing names) and attaches them to a catalog, schema, or table with sensible defaults. It is
**not** a fifth `policyType` and does not store a combined policy document.

Illustrative API (exact path may change in implementation):

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

Today, govern policies are **additive**: a table inherits every policy attached to it or its
ancestors, and the optimizer may evaluate multiple instances of the same type.

For **maintenance types**, TMS uses a different rule:

```text
effective_policy(table, maintenance_type) =
  nearest Active attachment of that type along:
    table → schema → catalog → metalake
```

Only **one** policy per maintenance type is evaluated for a table. A table-level compaction policy
**replaces** a catalog-level compaction policy for that table; both are **not** evaluated.

Policy attachment resolution still uses `policy_relation_meta` and `listPolicies()`; TMS applies
the precedence filter when building the effective maintenance configuration.

---

### 5.3 Trigger model (scheduler)

The scheduler is the **main** maintenance clock. Commits are a **compaction accelerator**, not the
only path.

**Why commit-only triggering is insufficient:**

| Issue | Scheduler address |
| ----- | ----------------- |
| Snapshot expiry, manifest rewrite, and orphan cleanup are time- and state-driven | Scheduler scans **all** tables with attached policies on each tick |
| Tables that stop committing are never expired or cleaned | Time-driven evaluation does not require commits |
| Orphan files come from failed writes (no commit event) | Orphan policy runs on schedule |
| Event path without statistics refresh uses stale metrics | Scheduler **refreshes statistics** before evaluate |
| Per-commit `(table, policy)` evaluate; busiest tables win | **Worst-first** ranking across tables |
| Commit bursts submit unbounded Spark jobs | **maxConcurrentJobs** + maintenance window; carry over |
| Missed callback or out-of-band catalog commit | Next scheduler tick recovers |

**Scheduler tick (illustrative):**

```text
every scheduler.intervalMs (within maintenance window):
  1. Acquire scheduler lock (multi-node — §6.1)
  2. Track A — hot pipeline:
       - candidates: dirty=true OR compaction/manifest/expire due (minIntervalMs)
       - refresh statistics (Updater)
       - resolve effective policy per type (precedence — §5.2.3)
       - Recommender evaluate; rank worst-first
       - per selected table: compact → manifests → expire (§5.5)
  3. Track B — orphan cleanup:
       - candidates: orphan policy attached AND per-table minIntervalMs elapsed since last success
       - rank oldest-successful-cleanup first (§5.6)
       - enforce olderThan floor server-side; submit remove_orphan_files
  4. Submit while slots < maxConcurrentJobs (shared across tracks)
  5. Write job_run_meta for every run, including in-process (§6.5)
  6. Clear dirty after successful Track A evaluate (or after submit — implementation choice)
  7. Tables not reached remain dirty / due for the next tick / next night
```

The **commit path** is **best effort** (§10): a lost dirty signal only delays compaction acceleration;
the scheduler evaluates the table on the next pass regardless.

---

### 5.4 Commit path (dirty flag + bounded executor)

```text
IRC commit succeeded (same JVM)
  └─ IRC post-commit hook (§5.1.1)
        │
        └─ enqueue on bounded executor (required):
              UPSERT table_maintenance_state
                SET dirty=true, dirty_at=now
                WHERE metalake_id=? AND table_identifier=?
              (no policy resolution; no claim; no submit)
```

**Design requirements:**

1. The IRC hook returns quickly. **No** `Recommender`, **no** claim, **no** job submit on the commit
   thread.
2. A **bounded executor** (fixed queue + worker threads) performs the dirty upsert. Queue saturation
   policy (drop vs block) is configurable; document the chosen behavior in implementation.
3. Dirty flag only on the commit path. The **scheduler** (§5.3) picks up dirty tables on the next tick.

---

### 5.5 Hot pipeline (scheduler orchestration — Track A)

When the scheduler selects a table for **Track A**, enabled operations run in this **fixed order**:

```text
1. compact           Recommender → rewrite data files SQL
2. manifests         Recommender → rewrite manifests SQL
3. expire            Recommender → expire snapshots SQL
```

```text
for each op in [compact, manifests, expire] if enabled by effective policy:
  1. Recommender / StrategyHandler
       └─ not triggered → skip this op's SQL; continue
  2. SQL (Iceberg Spark procedure / equivalent)
```

**Recommender vs SQL:** `Recommender` decides whether an op should run; SQL is the maintenance
work. If Recommender does not trigger, skip that op's SQL only.

**Why this order:**

- Compact first so readers stop paying for small files.
- Rewrite manifests after compact so the index matches the post-compaction file set.
- Expire next so snapshots created by compaction can be removed from metadata.

Orphan cleanup is **not** step 4 in this pass (§5.6). The `olderThan` safety window means files
left by a failed compaction hours ago are not eligible for deletion the same night anyway.

The job framework has no DAG orchestration. The scheduler may submit **one Spark job per op** or a
single built-in template that runs the ordered loop inside one job — implementation detail. The
**order** is fixed in either case.

Each hot-pipeline type has its own **`minIntervalMs`** (§8.3), compared per policy row via
`last_job_id` → `job_run_meta.job_finished_at`. When `last_job_id` is **null**, the interval gate
passes (first run after attach).

**Queue ordering:** tables in Track A are ranked **worst-first** using the health / debt score from
refreshed statistics.

---

### 5.6 Orphan cleanup track (scheduler — Track B)

Orphan cleanup runs on a **separate scheduler track**, not as step 4 of the hot pipeline.

| Aspect | Track A (hot pipeline) | Track B (orphan) |
| ------ | ---------------------- | ---------------- |
| Operations | compact, manifest rewrite, snapshot expire | `remove_orphan_files` only |
| Candidate signal | dirty + metrics / time due | per-table `minIntervalMs` since last **successful** orphan job |
| Queue order | worst-first (health score) | **oldest cleanup first** (time since last successful orphan run) |
| Evaluate | Recommender + metrics | no cheap metadata signal — eligibility is time-based |
| Shared limits | same maintenance window and `maxConcurrentJobs` (§8.1) | same |

**Per-table eligibility floor (not a global weekly cron):** a table becomes eligible again only
after **`minIntervalMs`** has elapsed since its own last **successful** orphan cleanup (default
seven days — §8.3). Prefix listings therefore spread across the week instead of the whole estate
listing on one night.

**`olderThan` server-side enforcement:** policy content may set `olderThan` (for example three to
seven days). TMS enforces a **server-side minimum floor** on every evaluate and submit path
(including REST policy writes and ops APIs), not only UI form validation.

**`job_run_meta`:** every orphan run — Spark or in-process — writes a `job_run_meta` row so
**Jobs → Runs** is the complete history and the maintenance UI reads from one place (§6.5).

---

### 5.7 Internal structure

| Part | Responsibility |
| ---- | -------------- |
| `TableMaintenanceRESTFeature` | Jersey 2 `Feature`; registers commit callback, scheduler, ops resources (§7). |
| `IcebergCommitEventHandler` | Enqueues dirty upsert on bounded executor (§5.4). |
| `MaintenanceScheduler` | Main path: candidates → stats refresh → evaluate → worst-first submit (§5.3). |
| `MaintenanceEvaluateSubmitPipeline` | Claim → interval gate → Recommender → submit for one `(table, policy)`. |
| `TableMaintenanceStateStore` | `table_maintenance_state` upsert / claim / dirty / rename (§6). |
| `IcebergTableLifecycleHook` | IRC rename/drop: rewrite or purge state rows (§6.4). |
| Existing optimizer classes | `Updater`, `Recommender`, providers, `JobSubmitter`. |

`TableMaintenanceEventStore` and `table_maintenance_event` are **not** in this design (§6.3).

---

### 5.8 User process

1. Operator enables the TMS REST plugin (`extensionPackages`) and `iceberg-rest` **in the same JVM**,
   and turns on in-process commit events (§5.1.1 / §8.2).
2. Operator applies a **`standard` profile** or creates four policies and attaches them at the
   intended grains (catalog for snapshot expiry, table for compaction, etc.) via metalake Policy APIs.
3. Operator enables the **scheduler** (`gravitino.maintenance.scheduler.enabled=true`, §8.1).
4. Engines write through Gravitino Iceberg REST. On commit success, IRC sets **`dirty=true`** (§5.4).
5. On each scheduler tick, TMS refreshes statistics, evaluates effective policies, ranks tables,
   and submits maintenance jobs worst-first within limits (§5.3).
6. Operators observe runs in the Gravitino **Jobs** UI / APIs. Manual runs remain available through
   ops APIs (§7) and `runJob`.

**Example — create compaction policy and attach to a table:**

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "orders_compaction",
    "comment": "Per-table compaction",
    "policyType": "system_iceberg_compaction",
    "enabled": true,
    "content": {}
  }' \
  http://localhost:8090/api/metalakes/test/policies

curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"policiesToAdd": ["orders_compaction"]}' \
  http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.orders/policies
```

---

## 6. Multi-node coordination (shared claim)

On **multiple** Gravitino / TMS nodes, dirty upserts and scheduler ticks may run on **any** replica
that hosts colocated IRC. Without coordination, two nodes could both evaluate and submit the same
policy's job for the same table.

**Approach:** shared table `table_maintenance_state` in the Gravitino entity DB. Table identity uses
a **normalized string `table_identifier`** (`catalog.schema.table`), **not** `table_meta.table_id`.

Iceberg REST / optimizer tables often have **no** row in `table_meta` (same reason
`table_metrics` stores `table_identifier`, and `iceberg_cleanup_job` keys by
`catalog_id` + `namespace` + `table_name`). TMS must not require Gravitino table metadata to exist.

| Table | Role |
| ----- | ---- |
| `table_maintenance_state` | Dirty flag, multi-node **claim**, in-flight `job_id`, finished `last_job_id` per policy (§6.1–§6.2) |

`table_maintenance_state` primary key is `(metalake_id, table_identifier, policy_id)` — **one row
per effective maintenance policy instance** that has state for a table. Claim is **per policy row**.

Policy attachment remains in `policy_relation_meta`; precedence (§5.2.3) runs at evaluate time.

### 6.1 Claim flow

```text
Node A / Node B — both evaluate the same (table, policy)
        │
        ├─ both attempt per-policy claim:
        │     UPDATE … SET state=RUNNING
        │     WHERE metalake_id=? AND table_identifier=? AND policy_id=? AND state=IDLE
        │     ├─ Node A: rows_affected = 1 → runs evaluate → submit → release to IDLE
        │     └─ Node B: 0 rows → skip (another node holds claim)
        v
Scheduler tick acquires a cluster-wide scheduler lock before selecting candidates
```

Gate checks alone are insufficient (read race). **Claim is the write lock** for that policy row;
gates run only after claim succeeds.

### 6.2 State table (shared store)

**Table name:** `table_maintenance_state`

| Column | Type | Notes |
| ------ | ---- | ----- |
| `metalake_id` | `BIGINT UNSIGNED NOT NULL` | Metalake that owns the maintenance policy |
| `table_identifier` | `VARCHAR(512) NOT NULL` | Normalized `catalog.schema.table` |
| `policy_id` | `BIGINT UNSIGNED NOT NULL` | Real `policy_meta.policy_id` |
| `dirty` | `BOOLEAN NOT NULL` | `true` after commit; cleared after evaluate (default `false`) |
| `dirty_at` | `BIGINT NULL` | Epoch millis when dirty was last set |
| `state` | `VARCHAR(16) NOT NULL` | `IDLE` / `RUNNING` (per policy row) |
| `updated_at` | `BIGINT NOT NULL` | Epoch millis; claim / reclaim |
| `job_id` | `BIGINT UNSIGNED NULL` | In-flight job (`job_run_meta.job_run_id`) |
| `last_job_id` | `BIGINT UNSIGNED NULL` | Last finished job; `job_finished_at` drives min-interval |
| `last_measured_snapshot_id` | `BIGINT NULL` | Snapshot id at last successful evaluate (§10.3) |
| `claim_lease_expires_at` | `BIGINT NULL` | Epoch millis; reclaim `RUNNING` after expiry (§10.3) |
| `submission_idempotency_key` | `VARCHAR(64) NULL` | Written before job submit; dedupe boundary (§10.3) |

**Primary key:** (`metalake_id`, `table_identifier`, `policy_id`).

**Lifecycle:**

1. On dirty upsert: ensure a row exists per attached policy for the table (or upsert table-level
   dirty on all policy rows for that `table_identifier`). On duplicate key for policy rows, set
   `dirty=true` without changing a live `RUNNING` claim.
2. Claim: conditional `UPDATE … SET state=RUNNING, claim_lease_expires_at=now+leaseMs WHERE … AND
   state=IDLE` (reclaim stale `RUNNING` when `claim_lease_expires_at` has passed, or after
   `claimTimeoutMs`).
3. If `job_id` is set and that job has finished: `last_job_id = job_id`, clear `job_id`.
4. Before submit: if `job_id` still in flight → release claim; skip.
5. Apply per-type `minIntervalMs` via `last_job_id` → `job_finished_at` (§8.3). Null `last_job_id` →
   gate passes.
6. On submit: set `job_id`; do not change `last_job_id` until the job finishes.
7. Done: `state=IDLE`; clear `dirty` when the table's evaluate pass completes.

Illustrative MySQL DDL:

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_state` (
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `table_identifier` VARCHAR(512) NOT NULL COMMENT 'normalized catalog.schema.table',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id from policy_meta',
    `dirty` TINYINT(1) NOT NULL DEFAULT 0 COMMENT 'commit accelerator flag',
    `dirty_at` BIGINT(20) NULL COMMENT 'when dirty was last set, epoch millis',
    `state` VARCHAR(16) NOT NULL COMMENT 'IDLE|RUNNING',
    `updated_at` BIGINT(20) NOT NULL COMMENT 'last state upsert time in epoch millis',
    `job_id` BIGINT(20) UNSIGNED NULL COMMENT 'in-flight job_run_id',
    `last_job_id` BIGINT(20) UNSIGNED NULL COMMENT 'last finished job_run_id',
    `last_measured_snapshot_id` BIGINT(20) NULL COMMENT 'snapshot id at last evaluate',
    `claim_lease_expires_at` BIGINT(20) NULL COMMENT 'claim lease expiry, epoch millis',
    `submission_idempotency_key` VARCHAR(64) NULL COMMENT 'idempotency key before job submit',
    PRIMARY KEY (`metalake_id`, `table_identifier`, `policy_id`),
    KEY `idx_dirty` (`metalake_id`, `dirty`, `dirty_at`),
    KEY `idx_state_updated` (`state`, `updated_at`),
    KEY `idx_table_identifier` (`table_identifier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'TMS dirty flag, claim, in-flight and last finished job per policy';
```

### 6.3 `table_maintenance_event` — not in this design

Earlier drafts INSERTed one `table_maintenance_event` row per commit. That table is **not** part of
this design.

Under the scheduler model, **`dirty` + `last_job_id` + `minIntervalMs`** are sufficient:

| Need | Mechanism |
| ---- | --------- |
| Table may need attention after commit | `dirty=true` |
| Last maintenance finished when | `last_job_id` → `job_run_meta.job_finished_at` |
| Minimum time between runs | Per-type `minIntervalMs` (§8.3) |
| Time-driven maintenance without commits | Scheduler tick (§5.3) |

A per-commit event log caused unbounded growth (one row per commit, deleted only on table drop) and
high entity-store write volume (~10k writes/hour at one commit / 5s with four policies) for mostly
"not yet" decisions. TMS does **not** ship `table_maintenance_event`.

### 6.4 Table rename / drop lifecycle (required with string keys)

Because TMS keys by **`table_identifier`** (not a stable `table_id`), a rename would otherwise orphan
claim / `job_id` / `last_job_id` rows. Historical **`table_metrics`** rows can tolerate orphan names;
**`table_maintenance_state` cannot**.

**Hook:** after a successful Iceberg table rename (or drop), IRC invokes an in-process
`IcebergTableLifecycleHook` registered by the TMS plugin (§5.1.1).

#### Rename (`old_identifier` → `new_identifier`)

1. **`table_maintenance_state`:** `UPDATE … SET table_identifier = new WHERE metalake_id = ? AND
   table_identifier = old`. Preserve `dirty`, `job_id`, `last_job_id`, `state`.
2. Policy attachments on Gravitino metadata objects survive rename when `table_meta` exists.

#### Drop

1. **`table_maintenance_state`:** `DELETE` all rows for `(metalake_id, table_identifier)`.
2. In-flight Spark jobs are **not** cancelled by this hook; operators cancel via Jobs APIs if needed.

Catalog rename is a **follow-up** bulk rewrite.

### 6.5 Job run history (`job_run_meta`)

Every maintenance submission — Spark job or **in-process** execution — creates a `job_run_meta`
row (and linked `job_id` on the policy state row). This keeps **Jobs → Runs** as the single audit
trail and lets the maintenance UI read history from one place.

On job finish, TMS updates `last_job_id`, clears in-flight `job_id`, and records
`last_measured_snapshot_id` when evaluate completes (§10.3).

---

## 7. Optimizer CLI replacement APIs

The commit path stays in-process and does **not** call these APIs. They replace the
`gravitino-optimizer` CLI (`--type ...`) for operators and scripts. The same plugin serves them on
the main webserver (**8090**).

- Prefix: `/api/maintenance/table/ops/...`.
- Caller must have **WRITE** on each target table. Missing privilege → **403**.
- `dryRun=true` returns the recommendation or job config and does not submit.

| CLI `--type` | Method | Path |
| ------------ | ------ | ---- |
| `submit-strategy-jobs` | `POST` | `/api/maintenance/table/ops/strategy-jobs` |
| `submit-update-stats-job` | `POST` | `/api/maintenance/table/ops/update-stats-jobs` |
| `update-statistics` | `POST` | `/api/maintenance/table/ops/statistics` |
| `append-metrics` | `POST` | `/api/maintenance/table/ops/metrics` |
| `monitor-metrics` | `POST` | `/api/maintenance/table/ops/metrics/monitor` |
| `list-table-metrics` | `GET` | `/api/maintenance/table/ops/metrics/tables` |
| `list-job-metrics` | `GET` | `/api/maintenance/table/ops/metrics/jobs` |

Each route calls the existing optimizer command implementation.

---

## 8. Configuration

### 8.1 Enablement keys (`gravitino.conf`)

| Key | Default | Description |
| --- | ------- | ----------- |
| `gravitino.server.rest.extensionPackages` | none | TMS Feature package. |
| `gravitino.auxService.names` | none | Must include `iceberg-rest` when using IRC. |
| `gravitino.maintenance.claimTimeoutMs` | `300000` | Reclaim stale `RUNNING` claim. |
| `gravitino.maintenance.executor.threads` | `4` | Bounded executor worker threads (§5.4). |
| `gravitino.maintenance.executor.queueSize` | `10000` | Bounded executor queue depth. |
| `gravitino.maintenance.scheduler.enabled` | `true` | Enable the maintenance scheduler (§5.3). |
| `gravitino.maintenance.scheduler.intervalMs` | `300000` | Scheduler tick interval (5 min). |
| `gravitino.maintenance.scheduler.maxConcurrentJobs` | `10` | Max concurrent maintenance Spark jobs. |
| `gravitino.maintenance.scheduler.maintenanceWindow` | none | Optional UTC window for submissions. |
| `gravitino.maintenance.claimLeaseMs` | `300000` | Claim lease; reclaim `RUNNING` after expiry (§10.3). |
| `gravitino.maintenance.orphan.olderThanMinMs` | `259200000` | Server-side minimum `olderThan` (3 days) for orphan cleanup (§5.6). |

### 8.2 Iceberg REST → TMS in-process event keys

| Key (illustrative) | Default | Description |
| ------------------ | ------- | ----------- |
| `gravitino.iceberg-rest.tableMaintenance.inProcess` | `false` | IRC invokes dirty-flag callback after commit. |

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.auxService.names = iceberg-rest
gravitino.iceberg-rest.tableMaintenance.inProcess = true
gravitino.maintenance.claimTimeoutMs = 300000
gravitino.maintenance.executor.threads = 4
gravitino.maintenance.scheduler.enabled = true
gravitino.maintenance.scheduler.intervalMs = 300000
gravitino.maintenance.scheduler.maxConcurrentJobs = 10
```

### 8.3 Task types and minimum interval (per policy type)

Each maintenance policy type has its own `minIntervalMs`, compared per `(table, policy_id)` via
`last_job_id` → `job_run_meta.job_finished_at`. Null `last_job_id` → gate passes.

| Task type | Policy type | Code default `minIntervalMs` |
| --------- | ----------- | ---------------------------- |
| `compaction` | `system_iceberg_compaction` | `3600000` (1 hour) |
| `snapshot-expiry` | `system_iceberg_snapshot_expiration` | `86400000` (1 day) |
| `manifest-rewrite` | `system_iceberg_rewrite_manifests` | `86400000` (1 day) |
| `orphan-cleanup` | `system_iceberg_orphan_file_removal` | `604800000` (7 days) |

**Resolution order** for `minIntervalMs`:

```text
1. Table property override (if set)
2. Global gravitino.conf key (if set)
3. Code default in the table above
```

**Global keys** (`gravitino.maintenance.task.*.minIntervalMs`) and **table properties**
(`maintenance.<task>.minIntervalMs`) mirror the task types above.

---

## 9. Work Plan and Checklist

### 9.1 Suggested Work Plan

| Phase | Work item | Notes |
| ----- | --------- | ----- |
| 1 | In-process plugin + bounded executor | Feature, dirty callback, executor (§5.4). |
| 2 | State table + claim + precedence | `table_maintenance_state` (§6); nearest-wins (§5.2.3). |
| 3 | Maintenance scheduler | Tick, cluster lock, worst-first, window (§5.3). |
| 4 | Per-table orchestration | Ordered ops + Recommender → SQL (§5.5). |
| 5 | Maintenance profile API | `standard` one-step setup (§5.2.2). |
| 6 | Ops APIs | §7. |
| 7 | Hardening | Metrics, shutdown, fault-tolerance tests (§10), docs. |

#### Phase 1 checklist

- [ ] `TableMaintenanceRESTFeature` registers commit callback and bounded executor.
- [ ] IRC hook sets `dirty=true` only; no evaluate on commit thread.
- [ ] Unit tests: commit thread does not block on evaluate.

#### Phase 2 checklist

- [ ] EntityStore migration for `table_maintenance_state` (§6.2) — **no** `table_maintenance_event`.
- [ ] Per-policy claim (§6.1).
- [ ] Precedence resolver: one effective policy per maintenance type (§5.2.3).
- [ ] Rename/drop lifecycle hook (§6.4).

#### Phase 3 checklist

- [ ] `MaintenanceScheduler` tick + cluster-wide lock (§6.1).
- [ ] Candidate set: dirty tables + time-driven policy scan (§5.3).
- [ ] Statistics refresh before evaluate (Updater).
- [ ] Worst-first ranking; `maxConcurrentJobs`; maintenance window.
- [ ] Tables not reached carry over to next tick.

#### Phase 4 checklist

- [ ] Hot pipeline op order: `compact → manifests → expire` (§5.5).
- [ ] Orphan track separate: per-table `minIntervalMs`, oldest-cleanup-first queue (§5.6).
- [ ] Server-side `olderThan` floor for orphan policies (§5.6, §8.1).
- [ ] Per-type `minIntervalMs` via `last_job_id` (§8.3).
- [ ] `job_run_meta` for every run including in-process (§6.5).
- [ ] Tests: Recommender no-trigger skips SQL; hot-pipeline order preserved; disabled ops skipped.

#### Phase 5 checklist

- [ ] `POST …/maintenance/profiles/apply` for `standard` profile (§5.2.2).
- [ ] Creates and attaches four policies with defaults; not a fifth policy type.

#### Phase 6 checklist

- [ ] Seven ops resources in §7; WRITE required; `dryRun` support.
- [ ] Commit path and scheduler do not call ops routes.

### 9.2 Review Checklist

| Area | Checklist |
| ---- | --------- |
| Deployment | `extensionPackages`; IRC colocated in same JVM. Ops APIs on **8090** (§7). |
| Policy | Four built-in types; precedence nearest-wins (§5.2); profile is convenience only. |
| Trigger | Scheduler main path (§5.3); commit → dirty only (§5.4). |
| Executor | Bounded; evaluation **off** commit thread (§5.4). |
| Durability | `table_maintenance_state` only; **no** per-commit event log (§6.3). |
| Multi-node | Per-policy claim + scheduler cluster lock (§6.1). |
| Orchestration | Hot pipeline `compact → manifests → expire` (§5.5); orphan separate track (§5.6). |
| Job boundary | Spark / in-process; all runs in `job_run_meta` (§6.5). |
| Fault tolerance | Scheduler: at-least-once latest-state; commit: best effort (§10). |

---

## 10. Fault tolerance and delivery guarantees

Following [PR #13386 discussion](https://github.com/apache/gravitino/pull/13386) (Mark Hoerth,
Rory Qi), TMS documents explicit delivery models and recovery behavior.

### 10.1 Delivery models

| Model | Description | TMS target |
| ----- | ----------- | ---------- |
| Best effort | Successful commit may permanently lose its maintenance signal | **Commit path only** (§5.4) |
| At-least-once latest-state | After recovery, the table's **latest** state is eventually evaluated; commits may coalesce | **Chosen** — provided by the **scheduler** |
| At-least-once per-commit | Every commit eventually represented in evaluation | **Not required** |
| Exactly-once job effect | Maintenance jobs are neither missed nor submitted twice | **Not achievable / not needed** |

**Chosen guarantee:** **at-least-once latest-state evaluation** on the scheduler path.
Coalescing is acceptable for **all four activities**: maintenance acts on the table's **current**
state, not on the history that produced it.

**Commit path:** **best effort by design**. A lost dirty signal delays compaction acceleration,
never maintenance, because the next scheduler pass evaluates the table regardless.

**Job submission:** at most **one in-flight job per table per activity** (per policy row claim +
`job_id`). Repeated jobs are **harmless** (a repeated compaction wastes work; orphans from failures
are collected by orphan cleanup).

### 10.2 Recovery is driven by table state, not event rows

TMS does **not** ship `table_maintenance_event` (§6.3). Recovery uses the **table's own state**:

- **Current snapshot id** vs `last_measured_snapshot_id` at last evaluate
- Per-policy **`last_job_id`** / in-flight **`job_id`**
- **`dirty`** for commit acceleration

An evaluation is **owed** whenever these disagree with the table's current metadata. Nothing is
**replayed** from a per-commit event log.

### 10.3 Recovery at failure boundaries

| Boundary | Behavior |
| -------- | -------- |
| Commit durable before dirty, or dirty before claim | Signal lost → evaluated on the **next scheduler pass** |
| Node fails holding a claim | **`claim_lease_expires_at`** (or `claimTimeoutMs`) reclaims `RUNNING`; next pass takes the table |
| Job accepted before `job_id` recorded | **Duplicate-submission boundary** — write **`submission_idempotency_key`** before submit; reconcile on it |
| Deferred for in-flight job or `minIntervalMs` | **Deferral, not loss** — snapshot ids still disagree; next pass retries |

**Failure tests:** kill the node after commit and before dirty upsert; mid-evaluate while holding a
claim; between job submission and `job_id` persistence. In each case the table is evaluated on the
next pass and no second concurrent job runs for the same `(table, activity)`.

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
9. [OpenHouse architecture (Jobs Scheduler)](https://github.com/linkedin/openhouse/blob/main/ARCHITECTURE.md)
10. [Apache Iceberg REST Catalog OpenAPI](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
