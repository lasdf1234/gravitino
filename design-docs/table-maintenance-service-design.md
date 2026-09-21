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

# Design of Table Maintenance Service in Gravitino Enterprise

## 1. Background

The Table Maintenance Service (TMS) in Gravitino Enterprise is currently an alpha feature.
The `maintenance/optimizer` package already contains the execution core for statistics collection,
rule evaluation, strategy recommendation, metrics query, and job submission (`Updater`,
`Recommender`, providers, `JobSubmitter`).

Today that core is not hosted as a long-running Gravitino service. Without a server-side component:

1. There is no stable **server-side commit event** after Iceberg commits. Engines would otherwise each need a
   TMS-specific listener, or operators must schedule maintenance externally.
2. Configuration, audit, and service-level metrics are hard to centralize when execution is
   ad hoc and process-local.
3. Each run creates its own runtime and provider instances instead of a shared service lifecycle.
4. When Spark maintenance work is needed, submitted work already returns a `jobId` owned by the
   Gravitino job framework. That job-status boundary should stay.

This design turns TMS into a **main-server REST plugin** on port **8090** (same pattern as IdP
via `gravitino.server.rest.extensionPackages`) so colocated IRC can drive the
evaluate → submit pipeline **in-process** after commits, while reusing the existing optimizer
execution core.

---

## 2. Goals

1. **Main REST plugin on 8090**: Enable Table Maintenance through
   `gravitino.server.rest.extensionPackages` (Jersey 2 `Feature`, same pattern as IdP). Lifecycle
   is owned by the main Gravitino webserver; APIs share port **8090**.
2. **IRC in-process commit event**: After successful Iceberg commits via IRC, TMS receives a commit
   event through a **main-server-registered in-process callback / SPI** (IRC aux and main server
   share one JVM; see **§5.1.1**). The event handler runs gates, optional statistics Collect, policy
   trigger evaluation, and job submission in-process (see **§5.4**).
3. **Reuse existing optimizer execution core**: Event handling invokes the same `Updater` /
   `Recommender` / job-submit paths already present in `maintenance/optimizer`, as **in-process
   methods**, not as a second copy of the logic.
4. **Job framework compatibility**: Spark maintenance work continues to use the Gravitino job
   framework. TMS returns or records submitted `jobId` values but does not own job status.
5. **Govern Policy reuse**: Maintenance policies stay on existing `policy_meta` and metalake Policy
   APIs (create / alter / enable / disable / associate). TMS does **not** introduce a parallel policy
   store or `/api/maintenance/table/policies` CRUD.
6. **Multi-node safe event processing**: Use a shared DB claim on `table_maintenance_state` so only
   one TMS replica runs the evaluate → submit pipeline for a table at a time (§6).
7. **Durable event log**: **Each event is written into the database** (`table_maintenance_event`)
   **before** claim / evaluate, so a crash or restart cannot silently drop an event (§6.3).

---

## 3. Non-Goals

1. **Standalone maintenance daemon**: No separate process or
   `gravitino-iceberg-rest-server.sh`-style entrypoint.
2. **Dedicated auxiliary HTTP listener**: No `GravitinoAuxiliaryService`, no isolated
   `gravitino.maintenance.classpath`, and no dedicated TMS port (for example **9301**). TMS is not
   an aux sibling of `iceberg-rest` / `lance-rest`.
3. **Provider SPI rewrite**: Does not replace `StatisticsUpdater`, `StatisticsCalculator`,
   `StatisticsProvider`, `StrategyProvider`, `TableMetadataProvider`, or `JobSubmitter` contracts
   used by the event pipeline.
4. **Engine-side commit report path**: Engines that bypass Gravitino Iceberg REST are out of scope
   for event-driven path.
5. **HTTP or Kafka commit-event ingress**: No `POST …/events/iceberg-commit` for IRC, and no Kafka
   produce/consume path. Commit events are **in-process only** (§5.1.1). Remote IRC / cross-JVM
   delivery is out of scope (follow-up if needed).

---

## 4. Solution Investigations

### 4.1 Option A: Keep process-local execution only

Continue running all optimizer work in ad hoc local processes, with no TMS service endpoint.

**Pros:** No new listener. Minimal implementation work.

**Cons:** No IRC event target; no centralized service for event-driven evaluate → submit.

**Decision:** Rejected.

### 4.2 Option B: Main REST plugin on port 8090 (Chosen)

Register Table Maintenance as a Jersey 2 `Feature` through
`gravitino.server.rest.extensionPackages` (same pattern as IdP). Expose **health**
(and optional later ops) under `/api/maintenance/table/...` on the main Gravitino webserver
(**8090**). After each Iceberg commit, **colocated** IRC invokes a main-server-registered
**in-process** callback that upserts state, takes an **atomic table claim**, runs gates, optional
statistics Collect, `Recommender` trigger, and job submit.

**Pros:** One HTTP port for ops/health; reuses main-server auth filters; no remote event hop on the
commit path; reuses Policy + Jobs on the same server; matches Enterprise plugin packaging.

**Decision:** **Chosen** for MVP.

### 4.3 Option C: Independent long-running Table Maintenance Service process

**Pros:** Full JVM isolation.

**Cons:** Extra deployable; duplicates server lifecycle patterns already covered by the main
webserver plugin.

**Decision:** Rejected for Enterprise. Prefer the 8090 REST plugin.

### 4.4 Option D: Expose a full optimizer-ops REST surface as the primary product API

Map every optimizer capability (statistics update, recommendation submit, metrics list/monitor,
update-stats job submit, etc.) to a synchronous HTTP API as the primary product surface.

**Pros:** Convenient for scripts that want 1:1 HTTP coverage of optimizer capabilities.

**Cons:** Enterprise UI and IRC-driven flow do not need those surfaces as public APIs. Duplicates
Job APIs and inflates authz and support surface.

**Decision:** Rejected as the **primary** product / UI API shape. Prefer in-process commit events
plus a minimal public health API (Option B). Optimizer ops may still be exposed later as a
**separate non-UI ops group** (§7.2), not as a console Compact-policy surface.

### 4.5 Option E: Dedicated aux Jetty listener (:9301)

Implement `GravitinoAuxiliaryService` with `shortName() = "maintenance"`, expose a dedicated Jetty
listener (default **9301**), and keep TMS off the main 8090 JAX-RS app.

**Pros:** Classpath isolation similar to `iceberg-rest` / `lance-rest`.

**Cons:** Extra port and aux enablement; diverges from Enterprise plugins that already extend
**8090** via `extensionPackages`.

**Decision:** Rejected. Prefer Option B.

---

## 5. Proposal

### 5.1 Architecture

```text
Spark / Flink / Trino
        │  Iceberg REST commit
        v
Gravitino Iceberg REST (IRC aux, typically :9001)
        │  commit succeeded (same JVM as main server)
        │
        └─ in-process event callback / SPI  →  TMS handler on main-server classpath
                │
                v
         IcebergCommitEventHandler
                │
                ├─ INSERT table_maintenance_event (PENDING)  ← durable; no silent loss (§6.3)
                ├─ upsert state rows per Active policy
                ├─ atomic DB claim on all rows for the table (multi-node — §6)
                ├─ claim lost → leave event PENDING/DEFERRED; return quickly (deferred)
                ├─ pipeline done → UPDATE event terminal status
                v
         MaintenanceEvaluateSubmitPipeline
                │
                +--> Settings gates (max concurrency)
                +--> [on-demand] builtin-iceberg-update-stats → statistic_meta
                +--> Recommender.submitForStrategyName(...) → compaction when trigger passes
                │
                v
         Gravitino Job framework (rewrite / cleanup / …)
```

#### 5.1.1 In-process commit event (MVP only)

MVP delivers commit events **only in-process**. After a successful Iceberg commit, IRC invokes a
**main-server-registered callback / SPI** (for example on `GravitinoEnv`). That callback enters
`IcebergCommitEventHandler` → durable `table_maintenance_event` insert →
`MaintenanceEvaluateSubmitPipeline` + `table_maintenance_state` claim.

| Requirement | Detail |
| ----------- | ------ |
| Deployment  | IRC aux (`iceberg-rest`) and the main Gravitino server share **one JVM**. |
| Transport   | In-process callback / SPI only — **no** HTTP `POST …/events/iceberg-commit`, **no** Kafka. |
| Payload     | Normalized `table_identifier` (`catalog.schema.table`). Policy selection uses Active policies + triggers, not commit metadata. |
| Classloader | IRC uses an **isolated** aux classloader. Do **not** cast into TMS types. Register a callback on the main server that IRC can invoke across the boundary. |

Notes:

- Multi-node safety still uses the shared DB claim (§6): each replica that hosts colocated IRC may
  receive commits for tables routed to that node.
- **Durability** still uses `table_maintenance_event` (§6.3) — in-process delivery does not remove
  the need to persist the event before evaluate.
- HTTP and Kafka commit-event ingress are **out of scope** for MVP (Non-Goal #5).

Deployment:

1. Package the TMS plugin jars with the main Gravitino server and set
   `gravitino.server.rest.extensionPackages` to include the TMS Feature package (see §8.1).
2. Enable `iceberg-rest` in `gravitino.auxService.names` (same process as the main server).
3. Enable in-process commit events (`tableMaintenance.inProcess` — §8.3).
4. Attach Govern maintenance policies (e.g. `system_iceberg_compaction`) to catalogs/schemas/tables
   via existing Policy APIs on the main server (**8090**).

REST prefix for TMS **health** (and optional later ops) on **8090**:

```text
/api/maintenance/table
```

### 5.2 Internal structure

| Part                                | Responsibility                                                                                         |
| ----------------------------------- | ------------------------------------------------------------------------------------------------------ |
| `TableMaintenanceRESTFeature`       | Jersey 2 `Feature` registered via `extensionPackages`; wires health (MVP); registers in-process callback. |
| `IcebergCommitEventHandler`          | In-process event entry; persists event; upserts state; claims; runs pipeline.                          |
| `MaintenanceEvaluateSubmitPipeline` | Gates → on-demand Collect → `Recommender` → `JobSubmitter`.                                            |
| `TableMaintenanceStateStore`        | Shared DB access for `table_maintenance_state` upsert / claim / release / rename (§6.1–§6.2, §6.4). |
| `TableMaintenanceEventStore`        | Shared DB access for durable `table_maintenance_event` insert / terminal update / reclaim / rename (§6.3–§6.4). |
| `IcebergTableLifecycleHook`         | In-process IRC rename/drop hook: rewrite or purge TMS rows keyed by `table_identifier` (§6.4).     |
| Existing optimizer classes          | `Updater`, `Recommender`, providers, `JobSubmitter` — unchanged contracts for event path.                |

No REST handlers are required for the **UI Compact-policy** surface beyond **health** in MVP.
Optimizer ops APIs are a **separate non-UI group** (§7.2), not shown in the console.

### 5.3 User process (event-driven)

1. Operator enables the TMS REST plugin (`extensionPackages`) and `iceberg-rest` aux **in the same
   JVM**, and turns on in-process commit events (§5.1.1 / §8.3).
2. Operator creates / enables a maintenance policy and associates it to tables (or parents) via
   metalake Policy APIs, for example:

   ```bash
   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "iceberg_compaction_default",
       "comment": "Built-in Iceberg compaction policy",
       "policyType": "system_iceberg_compaction",
       "enabled": true,
       "content": {}
     }' \
     http://localhost:8090/api/metalakes/test/policies

   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     -d '{"policiesToAdd": ["iceberg_compaction_default"]}' \
     http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/policies
   ```

3. Engines write through Gravitino Iceberg REST. On commit success, IRC delivers a commit event to
   TMS **in-process** (§5.1.1).
4. TMS **writes a `table_maintenance_event` row first** (keyed by `table_identifier`), then resolves
   Active policies, upserts one state row per `(table_identifier, policy)`, claims **all** rows for
   that table, runs gates → optional Collect → trigger → submits when thresholds are met, and marks
   the event terminal.
5. Operators observe runs in the Gravitino **Jobs** UI / APIs.

### 5.4 Implementation process (event path)

```text
IRC commit succeeded (same JVM)
  └─ in-process callback / SPI (§5.1.1)
        │
        v
 IcebergCommitEventHandler
        │
        ├─ INSERT table_maintenance_event status=PENDING (§6.3)  ← durable first
        ├─ resolve Active policies (StrategyProvider / listPolicies)
        ├─ upsert one state row per policy (§6.2)
        ├─ atomic claim: all rows for this table PENDING → RUNNING (§6.1)
        │     └─ claim failed → mark event DEFERRED; return deferred (another node / reclaim)
        v
 MaintenanceEvaluateSubmitPipeline
        ├─ apply Settings gates:
        │     max concurrency
        │     └─ hard gate fails → release claim; mark event DEFERRED; return deferred
        ├─ Collect only if stats missing/stale
        │     JobManager.runJob(builtin-iceberg-update-stats)  // stats mode only
        │       └── Spark → StatisticsUpdater → Gravitino statistic_meta (main DB)
        ├─ for each Active policy:
        │     if job_id still QUEUED/STARTED → skip
        │     else Recommender.submitForStrategyName(...)
        │       └── JobSubmitter → rewrite / … when trigger passes
        │       └── write job_id on that policy row
        ├─ release claim (state → IDLE on all rows for the table; do not DELETE)
        └─ UPDATE table_maintenance_event → PROCESSED | SKIPPED | FAILED (§6.3)
```

The in-process handler runs the full gate + submit path on the calling thread (or a bounded
executor owned by the plugin — implementation detail). Collect and compaction Spark jobs are
**submitted asynchronously** via the job framework; the event path does not block on Spark
completion. **Every accepted event has a DB row before evaluate runs.** Table claim serializes
concurrent events; per-policy `job_id` prevents a later event from submitting again while the previous
job for that policy is still running (and supplies `job_finished_at` for cooldown when configured).
A reclaim loop (startup + periodic) finishes `PENDING` / `DEFERRED` / retryable `FAILED` events so
crashes cannot silently drop events (§6.3).

**Gate order (MVP):**

1. Global concurrency limits (Collect and/or maintenance submits).
2. Per-policy in-flight / min-interval via `job_id` → `job_run_meta` and resolved `minIntervalMs`
   (table prop → global conf → code default; §8.4).
3. Policy trigger (`Recommender`) for each remaining Active policy.
4. Submit maintenance job when trigger passes; persist `job_id`.

---

## 6. Multi-node coordination (shared claim)

On **multiple** Gravitino / TMS nodes, an in-process event may run on **any** replica that hosts
colocated IRC and receives the commit. Without coordination, two nodes could both pass gates and
double-submit Collect or compaction jobs.

**MVP approach:** two shared tables in the Gravitino entity DB. Table identity uses a **normalized
string `table_identifier`** (`catalog.schema.table`), **not** `table_meta.table_id`.

Iceberg REST / optimizer tables often have **no** row in `table_meta` (same reason
`table_metrics` stores `table_identifier`, and `iceberg_cleanup_job` keys by
`catalog_id` + `namespace` + `table_name`). TMS must not require Gravitino table metadata to exist.

| Table                      | Role                                                                 |
| -------------------------- | -------------------------------------------------------------------- |
| `table_maintenance_event`  | **Durable event log** — insert before evaluate so no event is lost (§6.3) |
| `table_maintenance_state`  | Multi-node **claim** + last `job_id` per policy (§6.1–§6.2)          |

`table_maintenance_state` primary key is `(metalake_id, table_identifier, policy_id)` — **one row
per attached maintenance policy**. Table-level claim flips **all** rows for that
`(metalake_id, table_identifier)` together.

Policy attachment remains in `policy_relation_meta` (resolved via `listPolicies()` / object
identifier APIs); the state table stores multi-node claim state and the last `job_id` per policy.
The event table stores every event attempt independently of claim success. `metalake_id` and
`policy_id` still come from Gravitino Policy / metalake metadata; only **table** identity avoids
`table_meta`.

### 6.1 Claim flow

```text
Node A / Node B — both receive an event for same table
        │
        ├─ both INSERT table_maintenance_event (PENDING)   ← durable first (§6.3)
        ├─ both resolve Active policies; upsert one row per policy
        ├─ both attempt table claim:
        │     UPDATE … SET state=RUNNING
        │     WHERE metalake_id=? AND table_identifier=? AND state=PENDING
        │     ├─ Node A: rows_affected > 0 → runs pipeline → event terminal
        │     └─ Node B: 0 rows → mark event DEFERRED; return deferred
        v
Node A runs §5.4 pipeline → set state=IDLE on all rows for the table (do not DELETE)
```

Gate checks alone are insufficient (read race). **Claim is the write lock**; gates run only after
claim succeeds. Claiming every policy row for the table keeps evaluate → submit single-flight across
nodes for that table. **Event insert is not the lock** — it is the durability / reclaim source so a
deferred or crashed event is not forgotten.

### 6.2 State table (shared store)

One relational table holds multi-node claim and the last submitted job per policy. Style follows
work-queue tables such as `iceberg_cleanup_job` (no soft-delete / version / audit boilerplate).
Table keying follows optimizer **`table_metrics.table_identifier`** (string identity), not
`table_meta.table_id`.

**Table name:** `table_maintenance_state`

| Column             | Type                       | Notes                                                              |
| ------------------ | -------------------------- | ------------------------------------------------------------------ |
| `metalake_id`      | `BIGINT UNSIGNED NOT NULL` | Metalake that owns the maintenance policy                          |
| `table_identifier` | `VARCHAR(512) NOT NULL`    | Normalized `catalog.schema.table` (same form as optimizer / event payload) |
| `policy_id`        | `BIGINT UNSIGNED NOT NULL` | Real `policy_meta.policy_id`                                       |
| `state`            | `VARCHAR(16) NOT NULL`     | IDLE / PENDING / RUNNING (mirrored; claim updates all rows)        |
| `updated_at`       | `BIGINT NOT NULL`          | Epoch millis; claim / reclaim                                      |
| `job_id`           | `BIGINT UNSIGNED NULL`     | Last submitted job for **this policy** (`job_run_meta.job_run_id`) |

**Primary key:** (`metalake_id`, `table_identifier`, `policy_id`).

**Lifecycle:**

1. On each event: resolve Active policies → `INSERT … ON DUPLICATE KEY UPDATE` each
   `(table_identifier, policy)` row: set `state=PENDING`, `updated_at=now`.
2. Claim: conditional `UPDATE … SET state=RUNNING WHERE metalake_id=? AND table_identifier=? AND
   state=PENDING` (and reclaim stale `RUNNING` after `claimTimeoutMs`).
3. Before submit: if `job_id` is set and that job is still active → skip that policy.
4. On submit: write the new `job_id` on that policy's row.
5. Done: set `state=IDLE` on all rows for the table. **Do not DELETE** — keep `job_id` for later
   events.

Illustrative MySQL DDL:

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_state` (
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `table_identifier` VARCHAR(512) NOT NULL COMMENT 'normalized catalog.schema.table',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id from policy_meta',
    `state` VARCHAR(16) NOT NULL COMMENT 'IDLE|PENDING|RUNNING',
    `updated_at` BIGINT(20) NOT NULL COMMENT 'last state upsert time in epoch millis',
    `job_id` BIGINT(20) UNSIGNED NULL COMMENT 'last job_run_id for this policy',
    PRIMARY KEY (`metalake_id`, `table_identifier`, `policy_id`),
    KEY `idx_state_updated` (`state`, `updated_at`),
    KEY `idx_table_identifier` (`table_identifier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'TMS multi-node event claim + last policy job';
```

### 6.3 Durable event log (`table_maintenance_event`)

**Requirement:** each in-process commit event is written into this table **before** claim / evaluate,
so TMS process death or a claim race cannot silently drop the event. Like `table_metrics`, rows key
by **`table_identifier`** string — **not** `table_meta.table_id` — so IRC tables without Gravitino
table metadata still persist.

```text
in-process callback
      │
      ▼
INSERT table_maintenance_event   ← durable; status=PENDING
      │
      ▼
upsert + claim (table_maintenance_state) → pipeline (§5.4)
      │
      ▼
UPDATE table_maintenance_event
  status = PROCESSED | SKIPPED | DEFERRED | FAILED
```

| Column             | Type                       | Notes                                                                 |
| ------------------ | -------------------------- | --------------------------------------------------------------------- |
| `event_id`         | `BIGINT UNSIGNED NOT NULL` | Surrogate PK (auto-increment)                                         |
| `metalake_id`      | `BIGINT UNSIGNED NOT NULL` | Metalake from config / policy resolution                              |
| `table_identifier` | `VARCHAR(512) NOT NULL`    | Normalized `catalog.schema.table` (event payload)                     |
| `ingress`          | `VARCHAR(16) NOT NULL`     | MVP: always `IN_PROCESS`                                              |
| `status`           | `VARCHAR(16) NOT NULL`     | `PENDING` / `PROCESSING` / `PROCESSED` / `SKIPPED` / `DEFERRED` / `FAILED` |
| `result`           | `VARCHAR(32) NULL`         | Internal outcome: `accepted` / `submitted` / `deferred` / …           |
| `job_id`           | `BIGINT UNSIGNED NULL`     | Set when a maintenance job was submitted                              |
| `attempt_count`    | `INT NOT NULL`             | Incremented on reclaim / retry                                        |
| `last_error`       | `VARCHAR(1024) NULL`       | Last failure message (no raw stacks)                                  |
| `created_at`       | `BIGINT NOT NULL`          | Epoch millis at insert                                                |
| `updated_at`       | `BIGINT NOT NULL`          | Epoch millis at last status change                                    |

**Primary key:** (`event_id`). **Index:** (`status`, `updated_at`), (`metalake_id`,
`table_identifier`, `created_at`).

MVP event payload is only `table_identifier` (§5.1.1), so there is **no** unique key on snapshot id.
Duplicate callbacks (if any) may insert multiple rows for the same table; claim + per-policy
`job_id` still prevent double-submit. Optional follow-up: persist Iceberg `snapshotId` (when IRC
provides it) and add a unique `(table_identifier, snapshot_id)` for stronger idempotency.

**Lifecycle:**

1. Event handler: `INSERT` with `status=PENDING`, then mark `PROCESSING` when claim succeeds.
2. Pipeline terminal: `PROCESSED` (evaluated / submitted), `SKIPPED` (no Active policy / noop),
   `FAILED` (retryable evaluate error), or `DEFERRED` (claim lost or hard gate — another node or
   reclaim will finish).
3. **Reclaim** (plugin start + periodic interval): select `status IN ('PENDING','DEFERRED','FAILED')`
   with `attempt_count` under max; skip tables that currently have a `RUNNING` claim; re-enter
   claim → pipeline; update terminal status. This meets **no silent event loss** after process
   crashes mid-handler.
4. Retention: purge terminal rows older than a configured TTL (follow-up config; default e.g. 7–30
   days).

Illustrative MySQL DDL:

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_event` (
    `event_id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT COMMENT 'commit event id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `table_identifier` VARCHAR(512) NOT NULL COMMENT 'normalized catalog.schema.table',
    `ingress` VARCHAR(16) NOT NULL COMMENT 'IN_PROCESS (MVP)',
    `status` VARCHAR(16) NOT NULL COMMENT 'PENDING|PROCESSING|PROCESSED|SKIPPED|DEFERRED|FAILED',
    `result` VARCHAR(32) NULL COMMENT 'accepted|submitted|deferred|… when known',
    `job_id` BIGINT(20) UNSIGNED NULL COMMENT 'submitted job_run_id when any',
    `attempt_count` INT NOT NULL DEFAULT 0 COMMENT 'reclaim / retry count',
    `last_error` VARCHAR(1024) NULL COMMENT 'last failure message',
    `created_at` BIGINT(20) NOT NULL COMMENT 'insert time epoch millis',
    `updated_at` BIGINT(20) NOT NULL COMMENT 'last update epoch millis',
    PRIMARY KEY (`event_id`),
    KEY `idx_status_updated` (`status`, `updated_at`),
    KEY `idx_table_created` (`metalake_id`, `table_identifier`, `created_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'TMS durable commit-event log (no silent event loss)';
```

### 6.4 Table rename / drop lifecycle (required with string keys)

Because TMS keys by **`table_identifier`** (not a stable `table_id`), a rename would otherwise orphan
claim / `job_id` rows and break cooldown / in-flight gates. Historical **`table_metrics`** rows can
tolerate orphan names; **`table_maintenance_state` cannot**.

**Hook:** after a successful Iceberg table rename (or drop), IRC invokes an in-process
`IcebergTableLifecycleHook` registered by the TMS plugin (same classloader-boundary pattern as the
commit-event callback — §5.1.1). Prefer wiring next to existing IRC rename/drop paths (e.g.
`IcebergTableHookDispatcher.renameTable` / `IcebergRenameTableEvent`).

#### Rename (`old_identifier` → `new_identifier`)

1. **`table_maintenance_state`:** rewrite every row for the metalake:
   `UPDATE … SET table_identifier = new WHERE metalake_id = ? AND table_identifier = old`.
   Preserve `state`, `job_id`, `updated_at` (except bump `updated_at` for audit). If a conflicting
   destination key already exists (rare), fail the rewrite loudly or merge per implementation policy
   — do not silently drop `job_id`.
2. **`table_maintenance_event`:** rewrite **non-terminal** rows
   (`PENDING` / `PROCESSING` / `DEFERRED` / retryable `FAILED`) to `new_identifier` so reclaim
   continues under the new name. **Terminal** rows may keep the old identifier as audit history
   (optional: also rewrite for simpler queries).
3. Policy attachments on Gravitino metadata objects (when present via `metadata_object_id`) are
   outside this table rewrite; object-id bindings survive rename when `table_meta` exists. String-
   based policy attachments, if any, must be updated by the Policy / IRC reconcile path separately.

#### Drop

1. **`table_maintenance_state`:** `DELETE` (or soft-clear) all rows for
   `(metalake_id, table_identifier)`.
2. **`table_maintenance_event`:** mark incomplete rows `SKIPPED` / `FAILED` with reason `table_dropped`,
   or delete them; do not reclaim after drop.
3. In-flight Spark jobs are **not** cancelled by this hook (job framework owns lifecycle); operators
   cancel via Jobs APIs if needed.

Catalog rename (changes the `catalog.` prefix of many identifiers) is a **follow-up** bulk rewrite;
MVP covers table rename/drop within a catalog.

---

## 7. Public REST API

TMS HTTP APIs are split into **groups**. MVP ships **health only** for the public surface; the
commit-event path is **in-process** (no REST). Group B covers optional optimizer ops (statistics /
metrics / recommend / submit); it is **not** exposed in the UI.

| Group                    | Audience                    | In UI? | MVP                              |
| ------------------------ | --------------------------- | ------ | -------------------------------- |
| A — Health               | Ops / LB                    | No     | **Yes**                          |
| B — Optimizer ops        | Scripts / advanced ops only | **No** | Follow-up; authz required (§7.2) |

### 7.0 Group A — Health (MVP)

| Method | Path                            | Caller   | Required? |
| ------ | ------------------------------- | -------- | --------- |
| `GET`  | `/api/maintenance/table/health` | Ops / LB | Optional  |

There is **no** `POST /api/maintenance/table/events/iceberg-commit` in MVP (Non-Goal #5).

### 7.1 GET /api/maintenance/table/health

Liveness probe for the TMS plugin on the main webserver (**8090**). **Not** a deep dependency check.
**No** `code` envelope.

**Response:** `{ "status": "UP" }` (`200 OK`). May return `{ "status": "DOWN" }` with **503** while
the plugin is shutting down.

```json
{ "status": "UP" }
```

### 7.2 Group B — Optimizer ops (non-UI)

These APIs expose optimizer capabilities as a **separate HTTP group** for scripts and advanced
operators:

- **Not** shown in the Enterprise UI Compact-policy console.
- **Authorization:** caller must have **WRITE** privilege on the target table (Gravitino table write
  privilege / equivalent). Missing privilege → **403**. List/query ops that target a table also
  require WRITE on that table for this group (ops-only surface, not a general read API).
- Prefer path prefix such as `/api/maintenance/table/ops/…` (illustrative) so UI clients never
  discover them as product navigation.

| Capability               | Responsibility                                           |
| ------------------------ | -------------------------------------------------------- |
| Update statistics        | Calculate and persist table or partition statistics      |
| Append metrics           | Calculate and append table, partition, or job metrics    |
| Submit strategy jobs     | Evaluate policies and optionally submit maintenance jobs |
| Monitor metrics          | Evaluate before/after metrics around an action time      |
| List table metrics       | Query stored table or partition metrics                  |
| List job metrics         | Query stored job metrics                                 |
| Submit update-stats job  | Submit built-in Iceberg update-stats Spark jobs          |

MVP **event path does not call** this group (Collect / submit stay in-process inside the event
pipeline). Shipping Group B REST is a **follow-up**.

---

## 8. Configuration

### 8.1 Enablement keys (`gravitino.conf`)

| Key                                       | Default | Description                                                                                          |
| ----------------------------------------- | ------- | ---------------------------------------------------------------------------------------------------- |
| `gravitino.server.rest.extensionPackages` | none    | Must include the TMS Feature package (illustrative: `org.apache.gravitino.maintenance.web.rest.feature`). |
| `gravitino.auxService.names`              | none    | Must include `iceberg-rest` when using IRC. TMS itself is **not** an aux service.                    |

### 8.2 Table Maintenance plugin keys (`gravitino.conf`)

Pipeline / provider keys stay under `gravitino.maintenance.*` (read by the plugin from the main
server config). There is **no** dedicated TMS HTTP `host` / `httpPort` — health (and optional later
ops) bind on the main webserver (**8090**).

| Key                      | Default  | Description                                              |
| ------------------------ | -------- | -------------------------------------------------------- |
| `claimTimeoutMs`         | `300000` | Reclaim stale `RUNNING` claim rows after worker failure. |
| `eventReclaimIntervalMs` | `60000`  | Scan incomplete `table_maintenance_event` rows (§6.3).   |
| `eventMaxAttempts`       | `5`      | Max reclaim attempts before leaving `FAILED`.            |

Existing provider keys continue under `gravitino.maintenance.*`, for example:

- `gravitino.maintenance.gravitinoUri` / `gravitinoMetalake` / `gravitinoDefaultCatalog`
- `gravitino.maintenance.recommender.*`, `updater.*`

Runtime Settings used by event gates (max concurrency) are configuration keys in MVP.

Per-task **minimum interval** defaults are under §8.4 (global → table override → code default).

Example:

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.auxService.names = iceberg-rest,lance-rest

gravitino.maintenance.claimTimeoutMs = 300000
gravitino.maintenance.eventReclaimIntervalMs = 60000
gravitino.maintenance.eventMaxAttempts = 5
gravitino.maintenance.gravitinoUri = http://127.0.0.1:8090
gravitino.maintenance.gravitinoMetalake = test

# Optional global min-interval overrides (omit → code defaults in §8.4)
gravitino.maintenance.task.compaction.minIntervalMs = 3600000
gravitino.maintenance.task.snapshot-expiry.minIntervalMs = 86400000
```

### 8.3 Iceberg REST → TMS in-process event keys

Illustrative keys (exact names may be finalized in implementation).

| Key (illustrative)                                  | Default | Description                                                                 |
| --------------------------------------------------- | ------- | --------------------------------------------------------------------------- |
| `gravitino.iceberg-rest.tableMaintenance.inProcess` | `false` | When `true`, IRC invokes the **main-server-registered event callback / SPI** after commit. |

Because IRC uses an isolated aux classloader, the callback must be registered by the TMS plugin (for
example on `GravitinoEnv`), not a direct cast to TMS implementation classes. IRC aux and the main
server must share **one JVM**.

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.auxService.names = iceberg-rest
gravitino.iceberg-rest.tableMaintenance.inProcess = true
```

HTTP `tableMaintenance.uri` / Kafka produce-consume keys are **not** part of MVP (Non-Goal #5).

### 8.4 Task types and minimum interval (global default + table override)

TMS recognizes four maintenance **task types** (aligned with product Compact policy surface):

| Task type          | Typical job / policy                             | Code default `minIntervalMs` |
| ------------------ | ------------------------------------------------ | ---------------------------- |
| `compaction`       | rewrite data files / `system_iceberg_compaction` | `3600000` (1 hour)           |
| `snapshot-expiry`  | expire snapshots                                 | `86400000` (1 day)           |
| `manifest-rewrite` | rewrite manifests                                | `86400000` (1 day)           |
| `orphan-cleanup`   | orphan file cleanup                              | `604800000` (7 days)         |

**Resolution order** (first hit wins), same idea as Amoro table props + AMS defaults:

```text
1. Table property override (if set)
2. Global gravitino.conf key (if set)
3. Code default in the table above
```

**Global keys** (`gravitino.conf`, prefix `gravitino.maintenance.`):

| Key                                   | Description                               |
| ------------------------------------- | ----------------------------------------- |
| `task.compaction.minIntervalMs`       | Default min interval for compaction jobs  |
| `task.snapshot-expiry.minIntervalMs`  | Default min interval for snapshot expiry  |
| `task.manifest-rewrite.minIntervalMs` | Default min interval for manifest rewrite |
| `task.orphan-cleanup.minIntervalMs`   | Default min interval for orphan cleanup   |

**Table-level overrides** (Iceberg / Gravitino table properties):

| Property                                     | Overrides                                    |
| -------------------------------------------- | -------------------------------------------- |
| `maintenance.compaction.minIntervalMs`       | Compaction min interval for this table       |
| `maintenance.snapshot-expiry.minIntervalMs`  | Snapshot expiry min interval for this table  |
| `maintenance.manifest-rewrite.minIntervalMs` | Manifest rewrite min interval for this table |
| `maintenance.orphan-cleanup.minIntervalMs`   | Orphan cleanup min interval for this table   |

The event path uses per-policy `job_id` → `job_run_meta.job_finished_at` and the resolved `minIntervalMs` for
that task type (§5.4 / §6.2). Policy content still owns **trigger thresholds** (e.g. MSE); interval
only caps how often a successful submit may repeat.

Example table override:

```sql
ALTER TABLE rest_catalog.db.orders SET TBLPROPERTIES (
  'maintenance.compaction.minIntervalMs' = '7200000'
);
```

---

## 9. Work Plan and Checklist

### 9.1 Suggested Work Plan

MVP delivers the 8090 REST plugin (health), in-process IRC commit events, durable
`table_maintenance_event` log, shared `table_maintenance_state` + claim, and inline evaluate →
submit pipeline.

| Phase | Work item                              | Notes                                                                                      |
| ----- | -------------------------------------- | ------------------------------------------------------------------------------------------ |
| 1     | REST plugin + health                   | `TableMaintenanceRESTFeature`, `GET …/health`, errors, enablement docs.                    |
| 2     | Internal evaluate → submit pipeline    | `MaintenanceEvaluateSubmitPipeline` + Settings gates; unit tests.                          |
| 3     | In-process IRC hook + event log        | Callback / SPI (§5.1.1); **durable event insert** + claim (§6).                            |
| 4     | Collect path wiring                    | On-demand stats Collect; `update-mode=stats`; gates for Collect storm.                     |
| 5     | Hardening                              | Service metrics, graceful shutdown, user docs.                                             |

#### Phase 1 checklist

- [ ] Add `TableMaintenanceRESTFeature` (Jersey 2 `Feature`) and health JAX-RS resource.
- [ ] Register via `gravitino.server.rest.extensionPackages` (illustrative package
      `org.apache.gravitino.maintenance.web.rest.feature`).
- [ ] Package plugin jars with the main Gravitino server distribution (no aux classpath).
- [ ] Add `GET /api/maintenance/table/health` on **8090** (no `code` envelope).
- [ ] Add sanitized error handling.
- [ ] Document `extensionPackages` enablement.
- [ ] Add unit tests for health and sanitized errors.

#### Phase 2 checklist

- [ ] Implement `MaintenanceEvaluateSubmitPipeline` calling `Recommender.submitForStrategyName`.
- [ ] Enforce Settings gates: max concurrency.
- [ ] Resolve Active attached policies via existing Policy / `StrategyProvider` (no new policy store).
- [ ] Add unit tests for skip / noop / submit / deferred outcomes.

#### Phase 3 checklist

- [ ] Add `IcebergCommitEventHandler` and main-server-registered in-process callback / SPI (§5.1.1 /
      §8.3).
- [ ] Add EntityStore migration for **`table_maintenance_event`** (§6.3) and
      **`table_maintenance_state`** (§6.2).
- [ ] Persist every event **before** claim / evaluate; terminal status update; reclaim loop
      (`eventReclaimIntervalMs` / `eventMaxAttempts`).
- [ ] Upsert + **table claim** on `table_maintenance_state` (§6.1).
- [ ] Wire IRC post-commit hook to the in-process callback (`tableMaintenance.inProcess`).
- [ ] Wire IRC **rename/drop** in-process hook to rewrite / purge `table_maintenance_state` and
      incomplete `table_maintenance_event` rows (§6.4).
- [ ] Integration tests: in-process event → event row → claim → pipeline once; crash after insert →
      reclaim; no double-submit; rename updates state `table_identifier`; drop clears state.
- [ ] Do **not** ship HTTP `…/events/iceberg-commit` or Kafka ingress in MVP.

#### Phase 4 checklist

- [ ] Wire on-demand `builtin-iceberg-update-stats` with **stats-only** mode when stats missing/stale.
- [ ] Global Collect concurrency limits (if configured).
- [ ] Tests: missing stats → Collect; no metrics append on the event path.

#### Phase 5 checklist

- [ ] Service metrics: event counts, deferred/submit ratios, claim conflicts, failures.
- [ ] Graceful shutdown tests.
- [ ] Update user-facing TMS / optimizer docs for in-process event mode.
- [ ] Add OpenAPI for health if published; validate with `./gradlew :docs:build`.

### 9.2 Review Checklist

| Area         | Checklist                                                                                         |
| ------------ | ------------------------------------------------------------------------------------------------- |
| Deployment   | Enabled via `gravitino.server.rest.extensionPackages`; health on main server **8090**; IRC colocated in same JVM. |
| Classpath    | TMS plugin on main server classpath; **not** an aux isolated listener.                            |
| Event        | **In-process only** (§5.1.1); shared handler + durable event + DB claim; no HTTP/Kafka ingress.   |
| Public API   | Group A health (§7); Group B ops non-UI + table WRITE (§7.2); no commit-event REST in MVP.        |
| Pipeline     | Inline on event: gates → stats Collect → `Recommender` → Jobs; no metrics/monitor on event path.    |
| Collect      | On-demand update-stats (stats mode); gated; never blind 1:1 per commit.                           |
| Durability   | Every event inserted into `table_maintenance_event` before evaluate; reclaim incomplete rows (§6.3). |
| Rename/drop  | In-process lifecycle hook rewrites / purges string-keyed TMS rows (§6.4).                             |
| Multi-node   | Shared `table_maintenance_state` + DB **claim** (§6.1–§6.2); no CronJob in MVP.                   |
| Policy       | Reuses metalake Policy APIs + `policy_meta`; no TMS policy CRUD.                                  |
| Job boundary | Spark work stays in Gravitino job framework; stats land in `statistic_meta` (main DB).            |
| Security     | No public commit-event REST; Group B requires table WRITE; sanitized errors.                      |

---

## 10. References

1. [Gravitino Iceberg REST service](../docs/iceberg-rest-service.md)
2. [Gravitino Lance REST service](../docs/lance-rest-service.md)
3. [Manage policies in Gravitino](../docs/manage-policies-in-gravitino.md)
4. [Iceberg compaction policy](../docs/iceberg-compaction-policy.md)
5. [Table Maintenance optimizer overview](../docs/table-maintenance-service/optimizer.md)
6. [Design of SCIM 2.0 User and Group Provisioning in Gravitino](./gravitino-scim-provisioning.md)
7. [Amoro AIP-3 – Event-Triggered Optimization of Iceberg Tables](https://cwiki.apache.org/confluence/display/AMORO/AIP-3%3A+Event-Triggered+Optimization+of+Iceberg+Tables+in+Amoro)
8. [OpenHouse architecture (Jobs Scheduler / CronJob data services)](https://github.com/linkedin/openhouse/blob/main/ARCHITECTURE.md)
9. [Apache Iceberg REST Catalog OpenAPI](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
