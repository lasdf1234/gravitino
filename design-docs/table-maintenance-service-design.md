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

The Table Maintenance Service (TMS) is an alpha feature. The `maintenance/optimizer` package already
has the execution core: statistics, rules, strategy, metrics, and job submit (`Updater`,
`Recommender`, providers, `JobSubmitter`).

That core is not yet a long-running Gravitino service. Without a server-side host:

1. No stable **server-side signal** after Iceberg commits (engines would need TMS listeners, or
   operators must schedule outside Gravitino).
2. Config, audit, and metrics stay ad hoc and process-local.
3. Each run builds its own runtime instead of a shared service lifecycle.

This design makes TMS a **main-server REST plugin** on port **8090** (same pattern as IdP via
`gravitino.server.rest.extensionPackages`) with colocated IRC, reusing the optimizer core. Spark
jobs stay on the Gravitino job framework (`jobId` boundary unchanged).

TMS uses a **dual trigger model** (§5.4–§5.6):

- **Commit path** — after each successful Iceberg commit, the IRC post-commit hook **INSERTs** one
  `table_maintenance_event` row (§6.3), then invokes an in-process TMS callback for **compaction
  only** (§5.4.1).
- **Scheduled path** — each node runs a **`MaintenanceScheduler`** (`selectDueWork` per-row claim).
  When `next_due_at` is due, any node may claim and run that policy. **All four policy types** use
  this path (§10: at-least-once latest-state).

---

## 2. Goals

1. **In-process plugin on the main server**: Load TMS via
   `gravitino.server.rest.extensionPackages` (Jersey 2 `Feature`, same as IdP) so the IRC callback is
   in the main JVM. Commit path does **not** use HTTP. Ops APIs that replace the optimizer CLI are
   in **§7**.
2. **Maintenance profile**: A profile such as `standard` creates and attaches all four policies with
   defaults in one step. Profiles are **not** a fifth policy type (§5.2).
3. **Precedence per type**: For each type, the **nearest** attachment along
   `table → schema → catalog → metalake` wins. Policies are **not** additive (§5.2).
4. **Dual trigger (option B)**: **Compaction** on **commit** (§5.4.1) **and** on the **scheduler** at
   wall-clock schedule (§5.4.2). Manifest rewrite, snapshot expiry, and orphan cleanup use the
   scheduler only (§5.5–§5.6). Policy **schedule** drives `next_due_at` for the **scheduler only**
   (§5.2.4).
5. **Wall-clock schedules in Gravitino**: Schedules live in Gravitino and are read by the scheduler,
   not by the commit hook.
6. **Reuse optimizer core**: Both paths call the same `Updater` / `Recommender` / job-submit paths
   as **in-process methods**.
7. **Job framework compatibility**: Spark work stays on the job framework. TMS records `jobId` but
   does not own job status.
8. **Govern Policy reuse**: Policies stay on `policy_meta` and metalake Policy APIs. No parallel
   policy store or `/api/maintenance/table/policies` CRUD.
9. **Multi-node safe**: Shared DB **per-policy claims** so only one replica runs evaluate → submit
   for a `(table, policy)` (§6). Replicas stay **peers**; no maintenance **leader** (§5.3).
10. **Commit log**: IRC post-commit hook **INSERTs** one `table_maintenance_event` per successful
    commit (`table_identifier`, `created_at`) (§6.3).

---

## 3. Non-Goals

1. **Standalone daemon**: No separate process or `gravitino-iceberg-rest-server.sh`-style entry.
2. **Dedicated aux HTTP listener**: No `GravitinoAuxiliaryService`, no
   `gravitino.maintenance.classpath`, no TMS-only port (e.g. **9301**).
3. **No maintenance leader**: No single node that scans all tables each tick. Timed work uses
   **per-node schedulers** and **per-row claims** (§5.3).
4. **Provider SPI rewrite**: Does not replace `StatisticsUpdater`, `StatisticsCalculator`,
   `StatisticsProvider`, `StrategyProvider`, `TableMetadataProvider`, or `JobSubmitter`.
5. **Engine-side commit report**: Engines that bypass Gravitino Iceberg REST are out of scope for
   commit-path compaction.
6. **Commit-path HTTP or Kafka**: No `POST …/events/iceberg-commit`, no health resource, no Kafka.
   Commit handling is **in-process only** (§5.1.1).
7. **External clock APIs**: No `POST …/maintenance/run-due` (or CronJob) as an alternate timed clock.
   The built-in `MaintenanceScheduler` is the only schedule driver; §7 is for manual / CLI runs only.

---

## 4. Solution Investigations

### 4.1 Deployment options

|                     | A: Process-local only                           | **B: In-process plugin (Chosen)**       | C: Separate TMS process                                  | D: Aux Jetty listener (:9301)                          |
| ------------------- | ----------------------------------------------- | --------------------------------------- | -------------------------------------------------------- | ------------------------------------------------------ |
| Pros                | Simple; no new listener                         | Same JVM plugin; no extra port          | Full JVM isolation                                       | Classpath isolation like IRC                           |
| Cons / why rejected | No IRC target; no central automated maintenance | Slightly couples TMS to the main server | Extra deployable; duplicates main-server plugin patterns | Extra port; diverges from **8090** `extensionPackages` |
| Decision            | Rejected                                        | **Chosen**                              | Rejected                                                 | Rejected                                               |

### 4.2 Maintenance trigger options

**Compaction** is most often tied to **writes/commits**. Manifest rewrite, expire, and orphan usually
are **not** run on every commit.

|                        | [AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-api-table-optimizers.html) | [Amoro](https://cwiki.apache.org/confluence/display/AMORO/AIP-3%3A+Event-Triggered+Optimization+of+Iceberg+Tables+in+Amoro) | [Databricks](https://docs.databricks.com/aws/en/tables/tune-file-size) | [Floe](https://github.com/nssalian/floe/blob/main/docs/policies.md) |
| ---------------------- | ----------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------- |
| Write / commit trigger | —                                                                                         | compaction                                                                                                                  | compaction                                                             | compaction                                                          |
| What stays scheduled   | compaction; snapshot expire; orphan clean                                                 | snapshot expire; orphan clean                                                                                               | compaction; manifest rewrite; snapshot expire; orphan clean            | compaction; manifest rewrite; snapshot expire; orphan clean         |

**Why TMS limits the commit path to compaction:**

1. Manifest rewrite, snapshot expire, and orphan clean are too heavy for the commit path (listing /
   scans hurt latency).
2. Inactive tables still need scheduled compaction when commits stop; expire / orphan must not depend
   on successful commits (orphans can appear without one).
3. Expire / orphan need fresh table-wide metadata; industry products keep them on a separate
   schedule, with only compaction on the write path.

**TMS decision:** IRC commit path runs **`system_iceberg_compaction` only** (§5.4.1). Scheduler runs
**all four** types on schedule (§5.4.2, §5.2.4). Manifest / expire / orphan are **scheduler-only**.
Nightly compaction covers tables that stop receiving commits.

### 4.3 Multi-node schedule options

Peer nodes typically use one of three patterns: **1** = policy grain; **2** = row grain;
**3** = external Cron + queue.

|                  | 1. Policy-level compete                                                                                     | 2. Row-level CAS                                                                                                                                                   | 3. External cron enqueue                                                                                                                                        |
| ---------------- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Typical products | ShedLock; Spring + Redis/DB lock; Quartz JDBC Cluster                                                       | Temporal lease; Hangfire; db-scheduler; SQS visibility timeout (analogy)                                                                                           | OpenHouse CronJob; Floe                                                                                                                                         |
| Pros             | Simple or mature; one winner per policy fire; prevents double runs of the same policy                       | Peers claim different `(table, policy)` rows — no whole-policy lock; same claim shared with the commit path (no double-submit)                                     | Decouples trigger from execution; consumers scale on the **external** queue                                                                                     |
| Cons             | Winner then lists the whole policy scope — hard to parallelize **per table**; lock/trigger is policy-scoped | —                                                                                                                                                                  | Requires **extra components** (external Cron and/or message queue); duplicate-enqueue and consumer **idempotency** still needed                                 |
| Chosen? Reason   | **Rejected.** Coarse policy grain; does not give per-table claim shared with the commit path.               | **Chosen** (§5.3). Matches in-tree cleanup; scheduler and commit path share the same `(table, policy)` claim; scales with due rows, not with a single policy lock. | **Rejected.** TMS must not introduce other runtime components beyond Gravitino and its entity DB. Pattern **2** keeps coordination in-process + existing store. |

---
### 4.4 Table discovery options

Products close the gap from catalog/scope defaults to runnable table work differently:

|      | [AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/catalog-level-optimizers.html) | [Apache Amoro](https://amoro.apache.org/docs/latest/configurations/) | [Floe](https://github.com/nssalian/floe/blob/main/docs/policies.md) |
| ---- | ------------------------------------------------------------------------------------ | -------------------------------------------------------------------- | ------------------------------------------------------------------- |
| How  | Copy catalog default to table on Create/Update                                       | Runtime-merge catalog settings into managed tables                   | Each cron tick lists tables in scope and runs                       |
| Cons | Catalog changes do not re-arm tables that already have table-level optimizers        | Tables not yet in AMS / unseen by the scheduler do not run           | Work waits for the next tick; each tick re-lists the scope          |

**TMS decision — discovery (§5.2.5, §5.3.4):**

1. **Create/Update/Drop hooks:** on IRC `createTable` / `updateTable` / `dropTable`, refresh or purge
   that table's maintenance state (nearest-wins → `table_maintenance_state` / `next_due_at`; drop
   deletes state rows — §6.4).
2. **Periodic discovery:** call IRC/Iceberg catalog APIs on an interval to reconcile scope —
   INSERT missing state rows, UPDATE wrong ones, DELETE stale — for tables that never went through
   IRC APIs.
3. **Discovery stays separate from the schedule loop:** discovery reconciles state rows (insert /
   update / delete); the scheduler (`selectDueWork`) only claims already-due rows and runs work.


## 5. Proposal

### 5.1 Architecture

```text
Spark / Flink / Trino → Iceberg REST commit
        v
Gravitino IRC (:9001, same JVM as main server)
        └─ post-commit hook (§5.1.1, §5.4.1)
                ├─ INSERT table_maintenance_event (§6.3)
                └─ async IcebergCommitEventHandler → compaction only

MaintenanceScheduler (every node — §5.3)
        selectDueWork → claim → evaluate → submit
        ├─ Compaction due (§5.4.2)
        ├─ Track A (§5.5): manifest | expire (soft: manifest before expire; worst-first)
        ├─ Track B (§5.6): orphan (oldest-cleanup-first; olderThan floor)
        └─ shared: optional maintenance window + maxConcurrentJobs
        v
Gravitino Job framework + job_run_meta (§6.5)
```

#### 5.1.1 In-process commit callback

After a successful Iceberg commit, the IRC hook **INSERTs** one `table_maintenance_event` (§6.3)
and asynchronously invokes `IcebergCommitEventHandler` (§5.4.1). No non-compaction policy resolve,
Recommender, or submit on the IRC thread.

|        | Deployment                             | Transport                                    | Payload                                           | Commit scope                                   | IRC thread cost                                              |
| ------ | -------------------------------------- | -------------------------------------------- | ------------------------------------------------- | ---------------------------------------------- | ------------------------------------------------------------ |
| Detail | IRC and main server share **one JVM**. | In-process only — **no** HTTP, **no** Kafka. | Normalized `table_identifier`; event row in §6.3. | **`system_iceberg_compaction` only** (§5.4.1). | One event `INSERT` + async hand-off to `IcebergCommitEventHandler`. |

---

### 5.2 Policy model

#### 5.2.1 Four built-in policy types

Each activity is a **separate** built-in policy type with its own `content` and `minIntervalMs`:

|                          | Compaction                                   | Manifest rewrite                    | Snapshot expiry                      | Orphan cleanup                        |
| ------------------------ | -------------------------------------------- | ----------------------------------- | ------------------------------------ | ------------------------------------- |
| Illustrative policy type | `system_iceberg_compaction`                  | `system_iceberg_rewrite_manifests`  | `system_iceberg_snapshot_expiration` | `system_iceberg_orphan_file_removal`  |
| Built-in job template    | `builtin-iceberg-compaction`                 | `builtin-iceberg-rewrite-manifests` | `builtin-iceberg-expire-snapshots`   | `builtin-iceberg-remove-orphan-files` |
| Trigger path             | **Commit** (§5.4.1) **+ Scheduler** (§5.4.2) | **Scheduler** (§5.3, §5.5)          | **Scheduler** (§5.3, §5.5)           | **Scheduler** (§5.3, §5.6)            |

#### 5.2.2 Maintenance profile (one-step setup)

A **profile** such as `standard` creates four policies with defaults — not a fifth `policyType`.

```bash
curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
  -H "Content-Type: application/json" \
  -d '{"profile":"standard","target":"catalog.rest_catalog",
       "overrides":{"compaction":{"enabled":true},"snapshot-expiry":{"olderThanDays":7}}}' \
  http://localhost:8090/api/metalakes/test/maintenance/profiles/apply
```

#### 5.2.3 Precedence (nearest attachment wins)

```text
effective_policy(table, maintenance_type) =
  nearest Active attachment of that type along:
    table → schema → catalog → metalake
```

Only **one** policy per maintenance type is evaluated for a table.

#### 5.2.4 Policy schedule

Each policy stores a **schedule** in `policy_meta.content`; TMS sets wall-clock **`next_due_at`** from it.

**Illustrative `content.schedule` (crontab):**

|                             | `nightly_compaction` | `weekly_snapshot_expiry` | `manifest_rewrite` | `orphan_cleanup` |
| --------------------------- | -------------------- | ------------------------ | ------------------ | ---------------- |
| `content.schedule` (stored) | `0 2 * * *`          | `0 3 * * 0`              | `0 4 * * *`        | `0 4 * * 0` + `enabled: false` |

**Commit vs scheduler:** `schedule` / `next_due_at` are **scheduler only**; commit neither reads nor advances them.

#### 5.2.5 Writing `table_maintenance_state`

`next_due_at` lives only on table-level `table_maintenance_state` rows.

|                             | Table                                                            | **Above table** (schema / catalog / metalake)                                                      |
| --------------------------- | ---------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| When state rows are written | **Immediately** on associate / schedule change / detach          | **Timed discovery** (§5.3.4)                                                                       |
| Behavior                    | O(1) UPSERT/DELETE; set `next_due_at = nextOccurrence(schedule)` | Bind association only; discovery lists scope, nearest-wins, INSERT / UPDATE / DELETE state rows |

```text
effective_policy(table, type) → policy_id   // nearest Active along table → schema → catalog → metalake

UPSERT table_maintenance_state
  (metalake_id, table_identifier, policy_id, state='IDLE',
   next_due_at = nextOccurrence(schedule), …)
```

Only **one** state row per `(table, maintenance_type)` effective policy.

**IRC table lifecycle hooks (§5.3.4, §6.4):** `createTable` and `updateTable` UPSERT / refresh state
for effective (ancestor) policies; `dropTable` purges state (and events). Tables that never call IRC
APIs rely on discovery.

---

### 5.3 Scheduled path: `MaintenanceScheduler`

`selectDueWork` vs discovery:

|                  | Claim due work                                                                      | Discovery                                                                                          |
| ---------------- | ----------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| Config           | `gravitino.maintenance.scheduler.pollIntervalSecs` (default **300**)                | `gravitino.maintenance.scheduler.discoveryIntervalSecs` (default **3600**)                         |
| Work             | Select existing state rows with `next_due_at <= now`                                | Reconcile scope: **INSERT** missing, **UPDATE** wrong (`policy_id` / `next_due_at`), **DELETE** stale |

#### 5.3.1 Due rows (`next_due_at`)

Each `table_maintenance_state` row for an attached, enabled policy carries:

|      | `next_due_at`                                                                           | `state`                                                         | `claim_lease_expires_at` / heartbeat                            |
| ---- | --------------------------------------------------------------------------------------- | --------------------------------------------------------------- | --------------------------------------------------------------- |
| Role | Epoch millis when this row becomes eligible for `selectDueWork` (from §5.2.4 schedule) | `IDLE` (claimable) or `RUNNING` (a node owns evaluate → submit) | Reclaim stale `RUNNING` like `iceberg_cleanup_job.heartbeat_at` |

**When `next_due_at` is set:** on materialize → `nextOccurrence(schedule)`; after success →
`nextOccurrence(schedule, after = job_finished_at)`. All four types when `next_due_at <= now` and
enabled.

**Candidate selection:** separate `selectDueWork` per track. `health_score` /
`last_orphan_success_at` are derived/joined, not required state columns (§6.2).

```sql
-- Compaction
SELECT … FROM table_maintenance_state s
 JOIN policy_meta p ON … AND p.policy_type = 'system_iceberg_compaction'
 WHERE … /* due + IDLE or stale RUNNING */
 ORDER BY health_score DESC LIMIT :candidateWindow

-- Track A: prefer manifest over expire for the same table
SELECT … FROM table_maintenance_state s
 JOIN policy_meta p ON … AND p.policy_type IN (
   'system_iceberg_rewrite_manifests', 'system_iceberg_snapshot_expiration')
 WHERE … /* due + IDLE or stale RUNNING */
   AND NOT (
     p.policy_type = 'system_iceberg_snapshot_expiration'
     AND EXISTS (
       SELECT 1 FROM table_maintenance_state m
        JOIN policy_meta pm ON pm.policy_id = m.policy_id
       WHERE m.metalake_id = s.metalake_id AND m.table_identifier = s.table_identifier
         AND pm.policy_type = 'system_iceberg_rewrite_manifests'
         AND (m.next_due_at <= :now OR m.state = 'RUNNING')
     )
   )
 ORDER BY CASE p.policy_type WHEN 'system_iceberg_rewrite_manifests' THEN 0 ELSE 1 END,
          health_score DESC
 LIMIT :candidateWindow

-- Track B
SELECT … FROM table_maintenance_state s
 JOIN policy_meta p ON … AND p.policy_type = 'system_iceberg_orphan_file_removal'
 WHERE … /* due + IDLE or stale RUNNING */
 ORDER BY last_orphan_success_at ASC LIMIT :candidateWindow
```

CAS claim (same eligibility predicate):

```sql
UPDATE table_maintenance_state
   SET state = 'RUNNING', claimed_by = :nodeId,
       claim_lease_expires_at = :now + :leaseMs, updated_at = :now
 WHERE metalake_id = ? AND table_identifier = ? AND policy_id = ?
   AND (state = 'IDLE' OR (state = 'RUNNING' AND claim_lease_expires_at < :now))
```

(`IcebergCleanupJobStore.takePendingJob` / `markRunning`.)

**After claim — interval gate:** if `minIntervalMs` not elapsed since `last_job_id` finished, release
to `IDLE` and set
`next_due_at = max(nextOccurrence(schedule, after = now), last_finished_at + minIntervalMs)`.

#### 5.3.2 Multi-node behavior

```text
Node A/B/C/D claim different (table, policy) rows in parallel — no leader
Node A dies → heartbeat expires → peer reclaim via selectDueWork
```

`maxConcurrentJobs` (§8.1) caps cluster in-flight Spark jobs:

```sql
SELECT COUNT(*) FROM table_maintenance_state
 WHERE state = 'RUNNING' AND job_id IS NOT NULL
```

If `COUNT >= maxConcurrentJobs`, do not claim (small race overshoot OK).

#### 5.3.3 External clock APIs (out of scope)

Do **not** expose `POST …/maintenance/run-due` as an alternate timed clock. `MaintenanceScheduler` is
the only schedule driver; §7 is for manual / CLI runs only.

#### 5.3.4 Scope discovery (above-table attachments)

Discovery expands schema / catalog / metalake attachments into **table-level** state rows (§5.2.5).
It does **not** replace `selectDueWork`. **Why discovery:** §4.4.

**Catalog source:** list tables from the **Iceberg/HMS** backend used by IRC — not only Gravitino
`table_meta`.

**One discovery round (illustrative):**

```text
1. Load enabled policies + attachments
2. For each above-table attachment: listTables(scope) via Iceberg/HMS
3. policy_id = effective_policy(table, type)  // nearest-wins
4. INSERT missing state rows with next_due_at = nextOccurrence(schedule)
5. UPDATE wrong rows (e.g. policy_id or next_due_at no longer matches effective policy / schedule)
6. DELETE stale (table, policy) rows; drop tables that left scope
7. Cap with discoveryBatchSize
```

**IRC table lifecycle hooks:** `createTable` / `updateTable` UPSERT or refresh ancestor state;
`dropTable` purges state and events (§6.4). Tables that never call IRC APIs rely on discovery.

**Examples (scheduled compaction on a schema):**

|          | Table created outside IRC; later **commit via IRC**         | Table created outside IRC; **no** IRC commits; schema has scheduled compaction   | Table created outside IRC; commits also bypass IRC                                  |
| -------- | ----------------------------------------------------------- | -------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------- |
| Expected | Commit path: event INSERT → compaction accelerator (§5.4.1) | Discovery sees the table in HMS → state row → scheduler runs compaction when due | No commit-path compaction; scheduled path still works if discovery listed the table |

---

### 5.4 Compaction: commit path + scheduler schedule (option B)

Compaction is the **only** type with two wake sources: IRC commit (§5.4.1) and scheduler schedule
(§5.4.2).

#### 5.4.1 Commit path (compaction only)

```text
IRC commit succeeded → post-commit hook (§5.1.1)
  ├─ INSERT table_maintenance_event (§6.3)   ← never UPDATE
  └─ async IcebergCommitEventHandler:
        resolve effective compaction policy (§5.2.3); skip if disabled
        UPSERT state if missing (next_due_at = nextOccurrence(schedule))
        minIntervalMs gate (event.created_at, last_job_id — §6.3)
        claim row (§6.1) + register refreshClaimHeartbeats
        Recommender → submit; record job_run_meta (§6.5); release to IDLE
        // do not advance next_due_at — scheduler owns schedule (§5.2.4)
```

IRC thread: event INSERT + async hand-off only. No `next_due_at` check; `minIntervalMs` + Recommender
decide submit. Missing state → UPSERT before claim. Async failure → best effort (§10); next commit
or scheduler can still drive. Multi-node: claim (§6.1) prevents double-submit.

#### 5.4.2 Scheduler path (scheduled compaction)

Scheduler claims compaction rows with `next_due_at <= now`, then evaluate → submit; after success
`next_due_at = nextOccurrence(schedule)` (§5.2.4). Inactive tables still compact on schedule;
active ones may no-op via `minIntervalMs` + Recommender. Manifest / expire / orphan: **scheduler only**.

#### 5.4.3 When commit and scheduler meet

Same row: **claim** + `minIntervalMs` + in-flight `job_id` prevent duplicate Spark jobs (§6.1).

---

### 5.5 Hot pipeline (scheduled — Track A)

Track A: **manifest** and **expire**, each with its own state row / claim — **no** multi-policy claim.
Soft order (§5.3.1): prefer manifest before expire; skip expire while that table has a due/RUNNING
manifest. Schedule manifest earlier when both attach. Compaction uses the compaction track (§5.4.2).
Prefer rewrite after the file set stabilizes, then expire. Per-type `minIntervalMs` (§8.3); rank
worst-first.

---

### 5.6 Orphan cleanup track (scheduled — Track B)

Orphan is a **separate track**, not step 3 of Track A.

| Aspect           | Track A (hot pipeline)               | Track B (orphan)                                               |
| ---------------- | ------------------------------------ | -------------------------------------------------------------- |
| Operations       | manifest rewrite, snapshot expire    | `remove_orphan_files` only                                     |
| Candidate signal | metrics / time due (`minIntervalMs`) | per-table `minIntervalMs` since last **successful** orphan job |
| Queue order      | worst-first (health score)           | **oldest cleanup first**                                       |
| Shared limits    | `maxConcurrentJobs` (§8.1)           | same                                                           |

Eligible again only after `minIntervalMs` since last success (default 7d — §8.3); if claimed early,
release and push `next_due_at` (§5.3.1). Enforce `olderThan` floor on evaluate and policy write (§8.1).

---

### 5.7 Internal structure

|                | `TableMaintenanceRESTFeature`                                                 | `IcebergCommitEventHandler`                                                     | `MaintenanceScheduler`                                                             | `MaintenanceEvaluateSubmitPipeline`                                     | `TableMaintenanceEventStore`                                                      | `TableMaintenanceStateStore`                                                   | `IcebergTableLifecycleHook`                                    | Existing optimizer classes                           |
| -------------- | ----------------------------------------------------------------------------- | ------------------------------------------------------------------------------- | ---------------------------------------------------------------------------------- | ----------------------------------------------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ | -------------------------------------------------------------- | ---------------------------------------------------- |
| Responsibility | Jersey 2 `Feature`; commit callback, scheduler lifecycle, ops resources (§7). | Async in-process callback; runs compaction pipeline (§5.4.1). | Worker loops + heartbeat + discovery; `selectDueWork` → evaluate → submit (§5.3). | Interval gate → Recommender → submit for one claimed `(table, policy)`. | IRC hook INSERT for `table_maintenance_event`; rename / drop rewrite (§6.3–§6.4). | `table_maintenance_state` upsert / `selectDueWork` / heartbeat / rename (§6). | IRC rename/drop: rewrite or purge state and event rows (§6.4). | `Updater`, `Recommender`, providers, `JobSubmitter`. |

Pattern: `IcebergCleanupManager` / `takePendingJob` (§5.3).

---

### 5.8 User process

1. Enable TMS plugin + `iceberg-rest` in the same JVM; turn on in-process callbacks (§5.1.1 / §8.2).
2. Apply `standard` profile or create/attach four policies; set schedules (§5.2.4).
3. Enable scheduler (`gravitino.maintenance.scheduler.enabled=true`, §8.1).
4. IRC commits INSERT events + compaction callback (§5.4.1); scheduler claims due rows (§5.3).
5. Observe via Jobs APIs; manual runs via §7.

---

## 6. Multi-node coordination (shared claim)

Shared `table_maintenance_state` in the entity DB; identity is `table_identifier`
(`catalog.schema.table`), not `table_meta.table_id`.

|      | `table_maintenance_event`                                        | `table_maintenance_state`                                                               |
| ---- | ---------------------------------------------------------------- | --------------------------------------------------------------------------------------- |
| Role | **Commit log** — one INSERT per successful Iceberg commit (§6.3) | Multi-node **claim**, in-flight `job_id`, finished `last_job_id` per policy (§6.1–§6.2) |

Every node polls; **per-row CAS** picks the winner. PK: `(metalake_id, table_identifier, policy_id)`.

### 6.1 Claim flow (`selectDueWork` / commit path)

**Scheduler** (same CAS as `iceberg_cleanup_job.markRunning`):

```text
Both nodes SELECT due candidates → both CAS claim (IDLE or stale RUNNING):
  UPDATE … SET state=RUNNING, claimed_by=:nodeId, claim_lease_expires_at=…
  WHERE … AND (state=IDLE OR (state=RUNNING AND claim_lease_expires_at < now))
  winner (rows_affected=1) → heartbeat → evaluate → submit → IDLE + next_due_at
  (or interval gate fail → IDLE + push next_due_at, §5.3.1)
  loser → next candidate
refreshClaimHeartbeats covers scheduler + commit-path claims
```

**Commit path** uses the same CAS and **must** register heartbeats; it does **not** read/advance
`next_due_at`. Claim + `minIntervalMs` bound duplicates; **claim is the write lock** (gates alone
race).

### 6.2 State table (shared store)

**Table name:** `table_maintenance_state`

|       | `metalake_id`              | `table_identifier`                | `policy_id`                | `state`                | `next_due_at`                              | `updated_at`                | `job_id`               | `last_job_id`                      | `last_measured_snapshot_id`       | `claimed_by`                  | `claim_lease_expires_at`            | `submission_idempotency_key`  |
| ----- | -------------------------- | --------------------------------- | -------------------------- | ---------------------- | ------------------------------------------ | --------------------------- | ---------------------- | ---------------------------------- | --------------------------------- | ----------------------------- | ----------------------------------- | ----------------------------- |
| Type  | `BIGINT UNSIGNED NOT NULL` | `VARCHAR(512) NOT NULL`           | `BIGINT UNSIGNED NOT NULL` | `VARCHAR(16) NOT NULL` | `BIGINT NOT NULL`                          | `BIGINT NOT NULL`           | `BIGINT UNSIGNED NULL` | `BIGINT UNSIGNED NULL`             | `BIGINT NULL`                     | `VARCHAR(128) NULL`           | `BIGINT NULL`                       | `VARCHAR(64) NULL`            |
| Notes | Metalake owning the policy | Normalized `catalog.schema.table` | `policy_meta.policy_id`    | `IDLE` / `RUNNING`     | Scheduler eligibility, epoch millis (§5.3) | Claim / reclaim / heartbeat | In-flight `job_run_id` | Last finished; drives min-interval | Snapshot at last evaluate (§10.3) | Node holding `RUNNING` (§6.1) | Lease expiry; reclaim after (§10.3) | Written before submit (§10.3) |

**Primary key:** (`metalake_id`, `table_identifier`, `policy_id`).

Illustrative MySQL DDL:

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_state` (
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `table_identifier` VARCHAR(512) NOT NULL COMMENT 'normalized catalog.schema.table',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id from policy_meta',
    `state` VARCHAR(16) NOT NULL COMMENT 'IDLE|RUNNING',
    `next_due_at` BIGINT(20) NOT NULL COMMENT 'scheduler eligibility time, epoch millis',
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

IRC INSERTs one row per successful commit (TMS never UPDATEs). Keyed by `table_identifier` string,
not `table_meta.table_id`. Records that a commit happened; no `snapshot_id`.

```text
IRC post-commit hook → INSERT event → async IcebergCommitEventHandler (§5.4.1)
```

|       | `event_id`                    | `metalake_id`                     | `table_identifier`                | `created_at`              |
| ----- | ----------------------------- | --------------------------------- | --------------------------------- | ------------------------- |
| Type  | `BIGINT UNSIGNED NOT NULL`    | `BIGINT UNSIGNED NOT NULL`        | `VARCHAR(512) NOT NULL`           | `BIGINT NOT NULL`         |
| Notes | Surrogate PK (auto-increment) | Metalake from config / resolution | Normalized `catalog.schema.table` | Insert time, epoch millis |

**Primary key:** (`event_id`). **Index:** (`metalake_id`, `table_identifier`, `created_at`).

Claim / `job_id` / `last_job_id` prevent double-submit. Interval join:

```text
event e JOIN state s ON … AND s.policy_id = :compaction_policy_id
  LEFT JOIN job_run_meta j ON j.job_run_id = s.last_job_id
```

Null `last_job_id` → gate passes; non-null `job_id` → in flight. Compare
`e.created_at - j.job_finished_at` to `minIntervalMs` (§8.3).

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

**Retention:** append-only; cleaner may DELETE old rows. Decisions use state + `job_run_meta`.

### 6.4 Table rename / drop lifecycle (required with string keys)

String keys need rewrite/purge via `IcebergTableLifecycleHook` (§5.1.1).

#### Rename

1. **`table_maintenance_state`:** `UPDATE … SET table_identifier = new WHERE … = old`.
2. **`table_maintenance_event`:** same rewrite for the metalake.

#### Drop

1. **`table_maintenance_state`:** `DELETE` all rows for `(metalake_id, table_identifier)`.
2. **`table_maintenance_event`:** `DELETE` rows for `(metalake_id, table_identifier)`.

### 6.5 Job run history (`job_run_meta`)

Every submission creates a `job_run_meta` row. On finish: update `last_job_id`, clear `job_id`, set
`last_measured_snapshot_id` when evaluate completes (§10.3).

---

## 7. Optimizer CLI replacement APIs

Commit path and scheduler do **not** call these routes. They replace the `gravitino-optimizer` CLI on
**8090**.

|        | `submit-strategy-jobs`                     | `submit-update-stats-job`                      | `update-statistics`                     | `append-metrics`                     | `monitor-metrics`                            | `list-table-metrics`                        | `list-job-metrics`                        |
| ------ | ------------------------------------------ | ---------------------------------------------- | --------------------------------------- | ------------------------------------ | -------------------------------------------- | ------------------------------------------- | ----------------------------------------- |
| Method | `POST`                                     | `POST`                                         | `POST`                                  | `POST`                               | `POST`                                       | `GET`                                       | `GET`                                     |
| Path   | `/api/maintenance/table/ops/strategy-jobs` | `/api/maintenance/table/ops/update-stats-jobs` | `/api/maintenance/table/ops/statistics` | `/api/maintenance/table/ops/metrics` | `/api/maintenance/table/ops/metrics/monitor` | `/api/maintenance/table/ops/metrics/tables` | `/api/maintenance/table/ops/metrics/jobs` |

---

## 8. Configuration

### 8.1 Enablement keys (`gravitino.conf`)

|             | `gravitino.server.rest.extensionPackages` | `gravitino.auxService.names`           | `gravitino.maintenance.claimLeaseMs`           | `gravitino.maintenance.scheduler.enabled` | `gravitino.maintenance.scheduler.workerThreads` | `gravitino.maintenance.scheduler.pollIntervalSecs` | `gravitino.maintenance.scheduler.discoveryIntervalSecs`  | `gravitino.maintenance.scheduler.discoveryBatchSize` | `gravitino.maintenance.scheduler.heartbeatTimeoutSecs` | `gravitino.maintenance.scheduler.candidateWindow` | `gravitino.maintenance.scheduler.maxConcurrentJobs` | `gravitino.maintenance.scheduler.maintenanceWindow` | `gravitino.maintenance.orphan.olderThanMinMs` |
| ----------- | ----------------------------------------- | -------------------------------------- | ---------------------------------------------- | ----------------------------------------- | ----------------------------------------------- | -------------------------------------------------- | -------------------------------------------------------- | ---------------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------- | --------------------------------------------------- | --------------------------------------------------- | --------------------------------------------- |
| Default     | none                                      | none                                   | `300000`                                       | `true`                                    | `2`                                             | `300`                                              | `3600`                                                   | `500`                                                | `300`                                                  | `8`                                               | `10`                                                | none                                                | `259200000`                                   |
| Description | TMS Feature package.                      | Include `iceberg-rest` when using IRC. | Claim lease → `claim_lease_expires_at` (§6.1). | Enable `MaintenanceScheduler` (§5.3).     | Scheduler workers per node.                     | Sleep when no due row claimed.                     | Discovery interval for above-table attachments (§5.3.4). | Max tables per discovery round.                      | Reclaim stale `RUNNING`.                               | Max candidates per `selectDueWork`.              | Cap: COUNT `RUNNING` + `job_id` (§5.3.2).           | Optional UTC window; skip submit outside.           | Min `olderThan` (3 days) for orphan (§5.6).   |

Per-table cadence is **`minIntervalMs`** / **`next_due_at`**. `pollIntervalSecs` = claim frequency;
`discoveryIntervalSecs` = expansion frequency.

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.auxService.names = iceberg-rest
gravitino.iceberg-rest.tableMaintenance.inProcess = true
gravitino.maintenance.scheduler.enabled = true
gravitino.maintenance.scheduler.maxConcurrentJobs = 10
```

### 8.2 Iceberg REST → TMS in-process event keys

|             | `gravitino.iceberg-rest.tableMaintenance.inProcess` |
| ----------- | --------------------------------------------------- |
| Default     | `false`                                             |
| Description | IRC invokes compaction callback after commit.       |

### 8.3 Task types and minimum interval (per policy type)

Each type has its own `minIntervalMs`, compared per `(table, policy_id)` via
`last_job_id` → `job_run_meta.job_finished_at`. Null `last_job_id` → gate passes.

|                              | `compaction`                | `snapshot-expiry`                    | `manifest-rewrite`                 | `orphan-cleanup`                     |
| ---------------------------- | --------------------------- | ------------------------------------ | ---------------------------------- | ------------------------------------ |
| Policy type                  | `system_iceberg_compaction` | `system_iceberg_snapshot_expiration` | `system_iceberg_rewrite_manifests` | `system_iceberg_orphan_file_removal` |
| Code default `minIntervalMs` | `3600000` (1 hour)          | `86400000` (1 day)                   | `86400000` (1 day)                 | `604800000` (7 days)                 |

**Resolution order:** table property override → global `gravitino.conf` key → code default.

---

## 9. Work Plan and Checklist

### 9.1 Suggested Work Plan

|           | 1                                                         | 2                                                      | 3                                               | 4                                                  | 5                                 | 6–8                           |
| --------- | --------------------------------------------------------- | ------------------------------------------------------ | ----------------------------------------------- | -------------------------------------------------- | --------------------------------- | ----------------------------- |
| Work item | In-process plugin + commit event                          | State + event tables + claim                           | `MaintenanceScheduler` + discovery              | Compaction on scheduler schedule                   | Track A / B                       | Profile API, ops APIs, harden |
| Notes     | Feature; IRC INSERT + async `IcebergCommitEventHandler` (§5.4.1) | Event + state materialization; nearest-wins (§5.2, §6) | `selectDueWork` + Iceberg/HMS discovery (§5.3) | Scheduler compaction; commit path unchanged (§5.4) | Hot pipeline + orphan (§5.5–§5.6) | §5.2.2, §7, §10               |

#### Phase 1–4 checklist

- [ ] Phase 1: event migration; IRC INSERT + async `IcebergCommitEventHandler` (§5.4.1, §6.3, §8.2).
- [ ] Phase 3: scheduler workers; discovery from Iceberg/HMS; immediate table attach; CAS
      `selectDueWork`; heartbeats; `next_due_at`; all four types; Track A soft order; orphan
      oldest-first + interval gate; `maxConcurrentJobs`; multi-node claim tests; commit-path
      heartbeat registration; discovery reconcile (§5.2–§5.6, §6.1).
- [ ] Phase 4: scheduler advances compaction `next_due_at`; commit path does not; claim +
      `minIntervalMs` vs double-submit; commit ignores non-compaction (§5.4).

### 9.2 Review Checklist

|           | Deployment                                               | Policy                                                                          | Trigger                                                                           | Discovery                                                        | Multi-node                                                        | Commit callback                                            | Durability                                                       | Orchestration                                    | Industry                             | Fault tolerance                                                 |
| --------- | -------------------------------------------------------- | ------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- | ---------------------------------------------------------------- | ----------------------------------------------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------------- | ------------------------------------------------ | ------------------------------------ | --------------------------------------------------------------- |
| Checklist | `extensionPackages`; IRC same JVM; ops on **8090** (§7). | Four types; nearest-wins; table attach immediate; above-table discovery (§5.2). | Compaction: commit + scheduler; others: scheduler + `next_due_at` (§5.4, §5.2.4). | Iceberg/HMS list; separate from `selectDueWork` (§5.3.4, §4.4). | No leader; per-row CAS like `IcebergCleanupManager` (§5.3, §4.3). | Async `IcebergCommitEventHandler` (§5.4.1).                | Event INSERT per commit (§6.3); state for claim/schedule (§6.2). | Track A soft order; orphan separate (§5.5–§5.6). | §4.2 / §4.3 / §4.4 (row CAS chosen). | Scheduler at-least-once latest-state; commit best effort (§10). |

---

## 10. Fault tolerance and delivery guarantees

### 10.1 Delivery models

|            | Best effort                         | At-least-once latest-state                                                  | Exactly-once job effect                                      |
| ---------- | ----------------------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------ |
| TMS target | **Commit compaction path** (§5.4.1) | **`MaintenanceScheduler` path** — recovery from table state, not event rows | **Not required** — claims + idempotency key bound duplicates |

**Commit path:** durable after event INSERT; async handler failure → next commit or scheduler. Manifest /
expire / orphan are scheduler-only.

**Scheduler path:** coalescing OK — act on **current** state; due row stays due until claim + complete.

### 10.2 Recovery is driven by table state

Recovery / interval gates use current snapshot vs `last_measured_snapshot_id`, per-policy
`last_job_id` / `job_id`, and `minIntervalMs` (commit path joins `event.created_at` — §6.3).

### 10.3 Recovery at failure boundaries

|          | Event INSERT ok before async handler runs compaction      | Node fails holding a claim                                                                     | Interval gate fails after claim                                                 | All workers at `maxConcurrentJobs`                    | Job accepted before `job_id` recorded              |
| -------- | --------------------------------------------------------- | ---------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------- | ----------------------------------------------------- | -------------------------------------------------- |
| Behavior | Event row kept; next commit or scheduler schedule retries | `claim_lease_expires_at` / heartbeat timeout reclaims `RUNNING`; peer `selectDueWork` retries | Release to `IDLE`; push `next_due_at` forward (§5.3.1) — no tight re-claim loop | Due rows remain; next claim cycle on any node retries | `submission_idempotency_key` written before submit |

---

## 11. References

1. [Gravitino Iceberg REST](../docs/iceberg-rest-service.md); [policies](../docs/manage-policies-in-gravitino.md); [compaction policy](../docs/iceberg-compaction-policy.md)
2. [Expire](./iceberg-expire-snapshots-maintenance-job.md) / [rewrite-manifests](./iceberg-rewrite-manifests-job.md) / [remove-orphan](./iceberg-remove-orphan-files-maintenance-job.md) design docs
3. [Optimizer overview](../docs/table-maintenance-service/optimizer.md)
4. [Amoro AIP-3](https://cwiki.apache.org/confluence/display/AMORO/AIP-3%3A+Event-Triggered+Optimization+of+Iceberg+Tables+in+Amoro); [Amoro configs](https://amoro.apache.org/docs/latest/configurations/)
5. [Floe policies](https://github.com/nssalian/floe/blob/main/docs/policies.md); [AWS Glue optimizers](https://docs.aws.amazon.com/glue/latest/dg/table-optimizers.html)
6. [Databricks auto compaction](https://docs.databricks.com/aws/en/tables/tune-file-size); [OpenHouse](https://github.com/linkedin/openhouse/blob/main/ARCHITECTURE.md)
7. Gravitino `IcebergCleanupManager` / `IcebergCleanupJobStore.takePendingJob`
