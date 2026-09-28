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

TMS uses a **scheduled two-step model** (§5.3–§5.5):

- **Step 1:** the node holding the planner lease finds policies whose `policy_meta.content` crontab is
  due and expands each by attachment grain (catalog / schema / table) into `table_maintenance_work`
  (§5.3.1).
- **Step 2:** workers on every node claim those rows and submit (§5.3.2). **All four policy
  types** use this path.

---

## 2. Goals

1. **In-process plugin on the main server**: Load TMS via
   `gravitino.server.rest.extensionPackages` (Jersey 2 `Feature`, same as IdP) on the main server.
2. **Maintenance profile**: A profile such as `standard` creates and attaches all four policies with
   defaults in one step. Profiles are **not** a fifth policy type (§5.2).
3. **Precedence per type**: For each type, the **nearest** attachment along
   `table → schema → catalog` wins. Policies are **not** additive (§5.2).
4. **Scheduler-driven maintenance**: All four policy types run on the **scheduler** path
   (§5.3–§5.5). Policy **crontab** in `policy_meta.content` drives Step 1 expansion (§5.2.3,
   §5.3.1).
5. **Wall-clock schedules in Gravitino**: Crontabs live on policies and are read by the **planner**.
6. **Reuse optimizer core**: Compaction submits one job that runs update-stats, decision, and
   rewrite in-process to the Spark job; other types reuse `Updater` / `Recommender` / submit as
   needed.
7. **Job framework compatibility**: Spark work stays on the job framework. TMS records `jobId` but
   does not own job status.
8. **Govern Policy reuse**: Policies stay on `policy_meta` and metalake Policy APIs. No parallel
   policy store or `/api/maintenance/table/policies` CRUD.
9. **Multi-node safe**: Shared DB **per `(table, policy)` claims** so only one replica runs
   claim → submit for the same policy on the same table (§6). **Different policies on the same
   table may run concurrently.** Replicas stay **peers** for execution; a **singleton planner**
   lease only serializes work enqueue (§5.3).

---

## 3. Non-Goals

1. **Standalone daemon**: No separate process or `gravitino-iceberg-rest-server.sh`-style entry.
2. **Dedicated aux HTTP listener**: No `GravitinoAuxiliaryService`, no
   `gravitino.maintenance.classpath`, no TMS-only port (e.g. **9301**).
3. **No execution leader**: No single node that **executes** all maintenance. Timed work uses a
   **singleton planner** to enqueue due `(table, policy)` rows, then **per-node workers** with
   **per-row claims** (§5.3).
4. **Provider SPI rewrite**: Does not replace `StatisticsUpdater`, `StatisticsCalculator`,
   `StatisticsProvider`, `StrategyProvider`, `TableMetadataProvider`, or `JobSubmitter`.
5. **External clock APIs**: No `POST …/maintenance/run-due` (or CronJob) as an alternate timed clock.
   The built-in `MaintenanceScheduler` is the only schedule driver.

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
| Write / commit trigger | —                                                                                         | compaction                                                                                                                  | compaction                                                             | —                                                                   |
| What stays scheduled   | compaction; snapshot expire; orphan clean                                                 | compaction; snapshot expire; orphan clean                                                                                   | compaction; manifest rewrite; snapshot expire; orphan clean            | compaction; manifest rewrite; snapshot expire; orphan clean         |

**Why industry products often limit the write-path trigger to compaction:**

1. Inactive tables still need scheduled compaction when commits stop; expire / orphan must not depend
   on successful commits (orphans can appear without one).
2. Expire / orphan need fresh table-wide metadata; industry products keep them on a separate
   schedule, with only compaction on the write path.
3. Manifest rewrite is typically run about once a day; a scheduler already covers it.

**TMS decision:** This design covers the **scheduler path only** — all four types on schedule
(§5.2.3, §5.3).

### 4.3 Multi-node schedule options

Peer nodes typically use one of three patterns: **1** = policy grain; **2** = row grain;
**3** = external Cron + queue.

|                  | 1. Policy-level compete                                                                                     | 2. Row-level CAS                                                                                                                          | 3. External cron enqueue                                                                                                                                        |
| ---------------- | ----------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Typical products | ShedLock; Spring + Redis/DB lock; Quartz JDBC Cluster                                                       | Temporal lease; Hangfire; db-scheduler; SQS visibility timeout (analogy)                                                                  | OpenHouse CronJob; Floe                                                                                                                                         |
| Pros             | Simple or mature; one winner per policy fire; prevents double runs of the same policy                       | Peers claim different `(table, policy)` rows — no whole-policy lock; per-row CAS with heartbeat reclaim                                   | Decouples trigger from execution; consumers scale on the **external** queue                                                                                     |
| Cons             | Winner then lists the whole policy scope — hard to parallelize **per table**; lock/trigger is policy-scoped | —                                                                                                                                         | Requires **extra components** (external Cron and/or message queue); duplicate-enqueue and consumer **idempotency** still needed                                 |
| Chosen? Reason   | **Rejected.** Coarse policy grain; does not give per-table parallel claim.                                | **Chosen** (§5.3). Scales with due work rows, not with a single policy lock.                                                              | **Rejected.** TMS must not introduce other runtime components beyond Gravitino and its entity DB. Pattern **2** keeps coordination in-process + existing store. |

---
### 4.4 Table discovery policy options

Products close the gap from catalog/scope defaults to runnable table work differently:

|      | [AWS Glue](https://docs.aws.amazon.com/glue/latest/dg/catalog-level-optimizers.html) | [Apache Amoro](https://amoro.apache.org/docs/latest/configurations/) | [Floe](https://github.com/nssalian/floe/blob/main/docs/policies.md) |
| ---- | ------------------------------------------------------------------------------------ | -------------------------------------------------------------------- | ------------------------------------------------------------------- |
| How  | Copy catalog default to table on Create/Update                                       | Runtime-merge catalog settings into managed tables                   | Each cron tick lists tables in scope and runs                       |
| Cons | Catalog changes do not re-arm tables that already have table-level optimizers        | Tables not yet in AMS / unseen by the scheduler do not run           | Work waits for the next tick; each tick re-lists the scope          |

**TMS decision — plan then claim (§5.2.4, §5.3):**

1. **Step 1 (planner):** policies whose crontab is due → expand by **catalog / schema / table**
   attachment (§5.3.1.1–§5.3.1.3), applying nearest-wins, then enqueue into
   `table_maintenance_work` (capped).
2. **Step 2 (workers):** every node claims work rows and submits (§5.3.2).

## 5. Proposal

### 5.1 Architecture

```text
MaintenanceScheduler (§5.3)
        Step 1 Planner (singleton): due policy crontab → expand catalog|schema|table → enqueue
        Step 2 Workers (every node): claim table_maintenance_work → submit
        ├─ Compaction (§5.4)  // job: update-stats → decision → compaction
        ├─ Track A (§5.5): manifest | expire
        ├─ Track B (§5.6): orphan
        v
Gravitino Job framework + job_run_meta (§6.3)
```

### 5.2 Policy model

#### 5.2.1 Four built-in policy types

Each activity is a **separate** built-in policy type with its own `content` (including
`schedule` and `minIntervalMs`):

|                          | Compaction                                   | Manifest rewrite                    | Snapshot expiry                      | Orphan cleanup                        |
| ------------------------ | -------------------------------------------- | ----------------------------------- | ------------------------------------ | ------------------------------------- |
| Illustrative policy type | `system_iceberg_compaction`                  | `system_iceberg_rewrite_manifests`  | `system_iceberg_snapshot_expiration` | `system_iceberg_orphan_file_removal`  |
| Built-in job template    | `builtin-iceberg-compaction`                 | `builtin-iceberg-rewrite-manifests` | `builtin-iceberg-expire-snapshots`   | `builtin-iceberg-remove-orphan-files` |
| Trigger path             | **Scheduler** (§5.3, §5.4)                   | **Scheduler** (§5.3, §5.5)          | **Scheduler** (§5.3, §5.5)           | **Scheduler** (§5.3, §5.6)            |

#### 5.2.2 Precedence (nearest attachment wins)

```text
effective_policy(table, maintenance_type) =
  nearest Active attachment of that type along:
    table → schema → catalog
```

Only **one** policy per maintenance type is evaluated for a table.

#### 5.2.3 Policy schedule and `minIntervalMs`

Each policy stores **`schedule`** (crontab) and **`minIntervalMs`** in `policy_meta.content`.
**Step 1** of the scheduled path is driven by that crontab: the node that holds the planner lease
finds policies whose `schedule` is due, then **expands each due policy by its attachment grain**
(catalog / schema / table) into work rows (§5.3.1). `minIntervalMs` gates enqueue per
`(table, policy)` (§7.2).

**Illustrative `content` fields:**

|                         | `nightly_compaction` | `nightly_snapshot_expiry` | `nightly_manifest_rewrite` | `weekly_orphan_cleanup` |
| ----------------------- | -------------------- | ------------------------- | -------------------------- | ----------------------- |
| `content.schedule`      | `0 2 * * *`          | `0 3 * * *`               | `0 4 * * *`                | `0 5 * * 0`             |
| `content.minIntervalMs` | `3600000`            | `3600000`                 | `3600000`                  | `3600000`               |

**Scheduler only:** crontab expansion and enqueue are **planner only**. Workers apply `minIntervalMs`
before claim (§6.1).

#### 5.2.4 Work model: two steps

| Step | Who | What |
| ---- | --- | ---- |
| **1. Plan** | Singleton planner lease | Read due policies from `policy_meta` + crontab → expand by attachment grain (§5.3.1) → INSERT `table_maintenance_work` |
| **2. Execute** | Workers on every node | CAS-claim work rows → submit jobs (§5.3.2) |

| Concept | Meaning |
| ------- | ------- |
| Work row grain | One row = one `(catalog_id, table_identifier, policy_id)` |
| Mutual exclusion | Same `(table, policy)` only. **Different policies on the same table may run concurrently** |
| Enqueue cap | At most `scheduler.planner.max-enqueue-per-round` (default **128**) inserts per planner round; remainder carries to the next round |

**Run-state:** `table_maintenance_policy_state` holds `last_job_id` / in-flight markers for
`minIntervalMs` and same-policy exclusion (§6.2.2). It is not the crontab clock.

---

### 5.3 Scheduled path: two steps

|        | Step 1 — Planner (`table_maintenance_planner_state`) | Step 2 — Workers (`table_maintenance_work`) |
| ------ | ---------------------------------------------------- | --------------------------------------------- |
| Config | `planner.poll-interval-secs` (**60**); `planner.interval-secs` (**300**); `planner.max-enqueue-per-round` (**128**) | `work.poll-interval-secs` (**60**); `work.worker-threads` (**8**); `work.candidate-window` (**32**) |
| Work   | Hold lease → find due policies by crontab → expand catalog/schema/table attachments → enqueue | Claim `PENDING` work → submit |

#### 5.3.1 Step 1 — Plan: crontab → expand policy → enqueue

Only **one** node holds the planner lease per round (CAS on `table_maintenance_planner_state`,
§6.2.3). Winner:

```text
1. CAS-claim planner_state when next_due_at <= now
2. Load Active policies from policy_meta (+ attachments)
3. Select policies whose content.schedule is due now
4. For each due policy: expand by attachment grain (§5.3.1.1–§5.3.1.3)
     → candidate (table, policy_id) rows
5. Apply gates per candidate:
     minIntervalMs; skip if PENDING/RUNNING work already exists for (table, policy)
6. INSERT into table_maintenance_work until max-enqueue-per-round
7. Release planner_state → IDLE; next_due_at = now + planner.interval-secs
```

Expansion must respect **nearest-wins** (§5.2.2): when a catalog- or schema-attached policy fires,
a table is enqueued **only if that policy is still the effective policy** for the table and type
(no nearer table/schema attachment of the same maintenance type overrides it).

##### 5.3.1.1 Catalog-attached policy (crontab fired)

When the due policy is attached to a **catalog**:

```text
1. Resolve catalog_id from the attachment
2. List schemas (namespaces / databases) under that catalog via Iceberg/HMS
3. For each schema S:
     listTables(S) via Iceberg/HMS
     for each table T in S:
       effective = effective_policy(T, policy.maintenance_type)  // table → schema → catalog
       if effective.policy_id != this due policy_id → skip  // nearer table/schema attachment wins
       else → candidate (catalog_id, S.T, policy_id)
4. Pass candidates through minIntervalMs + same-policy in-flight gates → enqueue (capped)
```

Example: catalog `lake` has `system_iceberg_compaction` with `0 2 * * *`. At 02:00 the planner
**lists schemas under `lake`**, then tables under each schema, skips tables that already have a
table- or schema-level compaction policy, and enqueues the rest under this catalog policy.

##### 5.3.1.2 Schema-attached policy (crontab fired)

When the due policy is attached to a **schema**:

```text
1. Resolve (catalog_id, schema) from the attachment
2. List all tables in that schema (Iceberg/HMS listTables(schema))
3. For each table T in the schema:
     effective = effective_policy(T, policy.maintenance_type)
     if effective.policy_id != this due policy_id → skip   // table-level attachment wins
     else → candidate (catalog_id, schema.T, policy_id)
4. Gates → enqueue (capped)
```

A schema-level fire never walks sibling schemas. Tables under the schema that attach a **nearer**
policy of the same type are skipped.

##### 5.3.1.3 Table-attached policy (crontab fired)

When the due policy is attached to a **table**:

```text
1. Resolve (catalog_id, schema.table) from the attachment
2. effective = effective_policy(table, policy.maintenance_type)
3. if effective.policy_id != this due policy_id → skip
     // e.g. policy disabled / detached / superseded — should be rare for table grain
4. else → single candidate (catalog_id, schema.table, policy_id)
5. Gates → enqueue (at most one row for this fire, still subject to the round cap)
```

Table grain is O(1) per due policy: no listing of siblings.

#### 5.3.2 Step 2 — Execute: worker claim

Each node runs a **fixed worker pool** (`work.worker-threads`, default **8**). Workers **only**
claim and submit; they do **not** parse crontabs or expand attachments.

```text
worker loop (× work.worker-threads):
  SELECT up to work.candidate-window PENDING (or stale RUNNING) candidates
  if SELECT returns 0 rows → sleep work.poll-interval-secs; continue
  try CAS claim one row (§6.1)            // same (table, policy) only
  if claim wins → heartbeat → submit Spark → complete work row; update policy_state
  if CAS loses → try next candidate in the batch (do not sleep)
```

| Concept                         | Meaning                                                                                           |
| ------------------------------- | ------------------------------------------------------------------------------------------------- |
| `work.worker-threads`           | Max **concurrent** claims / in-flight submits on this node (default **8**).                       |
| `work.candidate-window`         | Max rows per **SELECT** for CAS retries (default **32**).                                         |
| `work.poll-interval-secs`       | Sleep only when **SELECT returns 0 rows** (default **60**).                                       |
| `planner.max-enqueue-per-round` | Cap on INSERT count per planner round (default **128**).                                          |

**Claim at most free capacity:** busy workers must not claim more rows. Extra `PENDING` rows stay in
`table_maintenance_work`.

**Same-policy mutual exclusion:** at most one in-flight execution per `(catalog_id, table_identifier,
policy_id)`. Different policies on the same table may run concurrently. See §6.1.

---

### 5.4 Scheduled compaction

Planner enqueues due compaction work like any other type (§5.3.1). Workers claim and submit one job:
**update-stats → decision → compaction**. Manifest / expire / orphan use the same planner + worker
path (§5.5–§5.6).

---

### 5.5 Hot pipeline (scheduled — Track A)

Track A: **manifest** and **expire** as **separate** scheduled policies (own work rows / claims).
Same-table different policies may run concurrently (§6.1). Compaction uses §5.4. Per-policy
`minIntervalMs` in `content` (§7.2).

---

### 5.6 Orphan cleanup track (scheduled — Track B)

Orphan is a **separate track**, not step 3 of Track A.

---

### 5.7 User process

1. Enable TMS plugin + `iceberg-rest` in the same JVM (§7.1).
2. Apply `standard` profile or create/attach four policies; set schedules (§5.2.3).
3. Planner enqueues due work; workers claim and submit (§5.3).
4. Observe via Jobs APIs.

---

## 6. Multi-node coordination (shared claim)

Shared store in the entity DB:

| Table                               | Grain                        | Purpose                                                   |
| ----------------------------------- | ---------------------------- | --------------------------------------------------------- |
| `table_maintenance_work`            | `(table, policy)` work item  | Planner enqueue + worker claim / in-flight (§6.2.1)       |
| `table_maintenance_policy_state`    | `(table, policy)`            | Last / in-flight job ids for `minIntervalMs` (§6.2.2)     |
| `table_maintenance_planner_state`   | Cluster singleton (`id = 1`) | Who runs the planner round (§6.2.3)                       |

Identity is `catalog_id` + `table_identifier` (`schema.table`), not `table_meta.table_id`. Workers
**CAS**-claim work rows (same pattern as `iceberg_cleanup_job.markRunning`). Planner uses the same
CAS pattern on the singleton planner row.

### 6.1 Claim flow (workers)

**Workers** (same CAS as `iceberg_cleanup_job.markRunning`). Mutual exclusion is **same `(table,
policy)` only** — no cross-policy table lock:

```text
Both nodes SELECT up to work.candidate-window PENDING (or stale RUNNING) candidates
  → each free worker CAS-claims one:
  UPDATE table_maintenance_work
    SET state=RUNNING, heartbeat_at=:now
  WHERE … AND (state=PENDING OR (state=RUNNING AND heartbeat_at < :heartbeatExpiry))
    // Unique / application check: at most one PENDING|RUNNING row per
    // (catalog_id, table_identifier, policy_id)
  winner (rows_affected=1) → heartbeat → submit
    → on success: complete/delete work row; update policy_state.last_job_id
  CAS loser → try next candidate in the batch (do not sleep)
  SELECT returned 0 rows → sleep work.poll-interval-secs
Never claim more rows than free workers
```

**Same-policy rule:** two replicas must not run the same `(table, policy)` concurrently. **Different
policies on the same table may run concurrently** (e.g. compaction and snapshot expiry together).

Workers gate with `minIntervalMs` **before** claim. Submitted compaction job runs **update-stats →
decision → compaction** in one Spark job (§5.4).

### 6.2 State tables (shared store)

#### 6.2.1 `table_maintenance_work`

|       | `id`                       | `catalog_id`               | `table_identifier`        | `policy_id`                | `state`                    | `job_id`               | `heartbeat_at`                                     | `enqueued_at`        |
| ----- | -------------------------- | -------------------------- | ------------------------- | -------------------------- | -------------------------- | ---------------------- | -------------------------------------------------- | -------------------- |
| Type  | `BIGINT UNSIGNED NOT NULL` | `BIGINT UNSIGNED NOT NULL` | `VARCHAR(512) NOT NULL`   | `BIGINT UNSIGNED NOT NULL` | `VARCHAR(16) NOT NULL`     | `BIGINT UNSIGNED NULL` | `BIGINT NOT NULL`                                  | `BIGINT NOT NULL`    |
| Notes | Surrogate key              | Owning catalog id          | Normalized `schema.table` | `policy_meta.policy_id`    | `PENDING` / `RUNNING`      | In-flight `job_run_id` | Last worker heartbeat; stale `RUNNING` reclaimable | Planner enqueue time |

**Uniqueness:** at most one `PENDING` or `RUNNING` row per (`catalog_id`, `table_identifier`,
`policy_id`) (partial unique index or equivalent application upsert).

Illustrative MySQL DDL:

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_work` (
    `id` BIGINT(20) UNSIGNED NOT NULL AUTO_INCREMENT,
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `table_identifier` VARCHAR(512) NOT NULL COMMENT 'normalized schema.table',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id from policy_meta',
    `state` VARCHAR(16) NOT NULL COMMENT 'PENDING|RUNNING',
    `job_id` BIGINT(20) UNSIGNED NULL COMMENT 'in-flight job_run_id',
    `heartbeat_at` BIGINT(20) NOT NULL COMMENT 'last heartbeat from worker, 0 when pending',
    `enqueued_at` BIGINT(20) NOT NULL COMMENT 'planner enqueue time, epoch millis',
    PRIMARY KEY (`id`),
    KEY `idx_state_enqueued` (`state`, `enqueued_at`),
    KEY `idx_table_policy` (`catalog_id`, `table_identifier`, `policy_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'TMS planned maintenance work items';
```

#### 6.2.2 `table_maintenance_policy_state`

Run-history / gate state per `(table, policy)` — **not** the due clock:

|       | `catalog_id`               | `table_identifier`        | `policy_id`                | `job_id`               | `last_job_id`                      | `heartbeat_at` |
| ----- | -------------------------- | ------------------------- | -------------------------- | ---------------------- | ---------------------------------- | -------------- |
| Type  | `BIGINT UNSIGNED NOT NULL` | `VARCHAR(512) NOT NULL`   | `BIGINT UNSIGNED NOT NULL` | `BIGINT UNSIGNED NULL` | `BIGINT UNSIGNED NULL`             | `BIGINT NOT NULL` |
| Notes | Owning catalog id          | Normalized `schema.table` | `policy_meta.policy_id`    | In-flight `job_run_id` | Last finished; drives min-interval | Optional mirror of in-flight heartbeat |

**Primary key:** (`catalog_id`, `table_identifier`, `policy_id`).

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_policy_state` (
    `catalog_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'catalog id',
    `table_identifier` VARCHAR(512) NOT NULL COMMENT 'normalized schema.table',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id from policy_meta',
    `job_id` BIGINT(20) UNSIGNED NULL COMMENT 'in-flight job_run_id',
    `last_job_id` BIGINT(20) UNSIGNED NULL COMMENT 'last finished job_run_id',
    `heartbeat_at` BIGINT(20) NOT NULL COMMENT 'last heartbeat, 0 when idle',
    PRIMARY KEY (`catalog_id`, `table_identifier`, `policy_id`),
    KEY `idx_table_identifier` (`table_identifier`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'TMS per-policy run state for minIntervalMs and in-flight tracking';
```

#### 6.2.3 `table_maintenance_planner_state`

One seeded row (`id = 1`) for cluster-wide planner lease (replaces the old discovery-only lease).

|       | `id`                       | `state`                | `next_due_at`                                                   | `heartbeat_at`                                     |
| ----- | -------------------------- | ---------------------- | --------------------------------------------------------------- | -------------------------------------------------- |
| Type  | `BIGINT UNSIGNED NOT NULL` | `VARCHAR(16) NOT NULL` | `BIGINT NOT NULL`                                               | `BIGINT NOT NULL`                                  |
| Notes | Singleton row; seed `1`    | `IDLE` / `RUNNING`     | Next planner eligibility; advanced by `planner.interval-secs`   | Last planner-worker heartbeat; stale reclaimable   |

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_planner_state` (
    `id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'singleton id; seed row id=1',
    `state` VARCHAR(16) NOT NULL COMMENT 'IDLE|RUNNING',
    `next_due_at` BIGINT(20) NOT NULL COMMENT 'next planner eligibility, epoch millis',
    `heartbeat_at` BIGINT(20) NOT NULL COMMENT 'last heartbeat from planner, 0 when idle',
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'TMS cluster-wide planner claim (one row)';
```

**Planner loop (every node):** every `planner.poll-interval-secs`, try CAS when `next_due_at <= now`;
on win run §5.3.1 enqueue with heartbeats (stale reclaim via `planner.heartbeat-timeout-secs`); on
lose or not due, wait until the next poll.

### 6.3 Job run history (`job_run_meta`)

Every submission creates a `job_run_meta` row. On finish: update `last_job_id`, clear `job_id` on
`table_maintenance_policy_state`, and complete the work row.
---

## 7. Configuration

### 7.1 Enablement keys (`gravitino.conf`)

| Key                                                                | Default | Description                                                                |
| ------------------------------------------------------------------ | ------- | -------------------------------------------------------------------------- |
| `gravitino.server.rest.extensionPackages`                          | none    | TMS Feature package.                                                       |
| `gravitino.auxService.names`                                       | none    | Include `iceberg-rest` when using IRC.                                     |
| `gravitino.maintenance.scheduler.planner.poll-interval-secs`       | `60`    | How often each node polls `table_maintenance_planner_state` (§5.3.1).      |
| `gravitino.maintenance.scheduler.planner.interval-secs`            | `300`   | Advance planner `next_due_at` after a successful round (§5.3.1).           |
| `gravitino.maintenance.scheduler.planner.max-enqueue-per-round`    | `128`   | Max work rows inserted per planner round (§5.2.4, §5.3.1).                 |
| `gravitino.maintenance.scheduler.planner.heartbeat-timeout-secs`   | `300`   | Stale planner-state `heartbeat_at` → reclaim `RUNNING` (§6.2.3).           |
| `gravitino.maintenance.scheduler.work.poll-interval-secs`          | `60`    | Sleep when SELECT returns 0 work rows (§5.3.2).                            |
| `gravitino.maintenance.scheduler.work.worker-threads`              | `8`     | Concurrent claim/submit workers per node (§5.3.2).                         |
| `gravitino.maintenance.scheduler.work.candidate-window`            | `32`    | Max SELECT candidates per claim attempt (§5.3.2).                          |
| `gravitino.maintenance.scheduler.work.heartbeat-timeout-secs`      | `300`   | Stale work-row `heartbeat_at` → reclaim `RUNNING` (§6.1).                  |

`scheduler.planner.*` is enqueue planning; `scheduler.work.*` is work-row claim. Naming follows
`gravitino.iceberg-rest.async-cleanup.*` kebab-case. Each side has its own `heartbeat-timeout-secs`.

**Schedules** are evaluated at plan time from policy `content.schedule`. `minIntervalMs` is the
min-gap gate before enqueue/claim: policy `content` overrides per-type `gravitino.conf` defaults
(§7.2).

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.auxService.names = iceberg-rest
```

### 7.2 Minimum interval (`minIntervalMs`)

`minIntervalMs` is the **minimum gap between consecutive runs** of the same `(table, policy_id)`
(compared via `last_job_id` → `job_run_meta.job_finished_at`). It is **not** the planner interval or
work-poll cadence. Null `last_job_id` → gate passes.

Each **policy** may set its **own** `minIntervalMs` in **`policy_meta.content`** (§5.2.3). Server
keys below supply the **default** when `content` omits the field (per policy type).

| Key                                                      | Default            | Description                                               |
| -------------------------------------------------------- | ------------------ | --------------------------------------------------------- |
| `gravitino.maintenance.compaction.min-interval-ms`       | `3600000` (1 hour) | Default min gap for `system_iceberg_compaction`.          |
| `gravitino.maintenance.snapshot-expiry.min-interval-ms`  | `3600000` (1 hour) | Default min gap for `system_iceberg_snapshot_expiration`. |
| `gravitino.maintenance.manifest-rewrite.min-interval-ms` | `3600000` (1 hour) | Default min gap for `system_iceberg_rewrite_manifests`.   |
| `gravitino.maintenance.orphan-cleanup.min-interval-ms`   | `3600000` (1 hour) | Default min gap for `system_iceberg_orphan_file_removal`. |

**Resolution:** effective policy `content.minIntervalMs` (§5.2.2) → matching
`gravitino.maintenance.<type>.min-interval-ms` → code default `3600000`.

---

## 8. Work Plan and Checklist

### 8.1 Suggested Work Plan

|           | 1                                                       | 2                                                      | 3                                      | 4                                                  | 5                                 | 6–8                 |
| --------- | ------------------------------------------------------- | ------------------------------------------------------ | -------------------------------------- | -------------------------------------------------- | --------------------------------- | ------------------- |
| Work item | In-process TMS plugin                                   | Work + policy_state + planner_state tables             | Planner enqueue + worker claim         | Scheduled compaction (§5.4)                        | Track A / B                       | Profile API, harden |
| Notes     | Feature package on **8090**                             | Materialize three tables; nearest-wins at plan time (§5.2, §6) | Planner CAS + work CAS (§5.3) | All four types on planner schedule                 | Hot pipeline + orphan (§5.5–§5.6) | Profile API         |

#### Phase 1–4 checklist

- [ ] Phase 1: TMS plugin + scheduler enablement (§7.1).
- [ ] Phase 3: planner singleton; crontab-due policies; catalog/schema/table expansion
      (§5.3.1.1–§5.3.1.3); `max-enqueue-per-round`; workers claim `table_maintenance_work`;
      same-policy mutex only; heartbeats; all four types; Track A; orphan track; multi-node claim
      tests (§5.2–§5.6, §6.1–§6.2).
- [ ] Phase 4: claim + `minIntervalMs` vs double-submit (§5.3, §7.2).

### 8.2 Review Checklist

|           | Deployment                                     | Policy                                                                          | Trigger                                      | Plan / work                                                                                    | Multi-node                                                        | Durability                                              | Orchestration                         | Industry                             |
| --------- | ---------------------------------------------- | ------------------------------------------------------------------------------- | -------------------------------------------- | ---------------------------------------------------------------------------------------------- | ----------------------------------------------------------------- | ------------------------------------------------------- | ------------------------------------- | ------------------------------------ |
| Checklist | `extensionPackages`; IRC same JVM on **8090**. | Four types; nearest-wins on expand; crontab on policy content (§5.2). | All types: planner + work claim (§5.3–§5.5). | Due policy → catalog/schema/table expand; enqueue cap; workers claim (§5.3.1–§5.3.2). | No execution leader; per `(table, policy)` CAS (§5.3, §4.3). | `work` + `policy_state` + `planner_state` (§6.2). | Track A; orphan separate (§5.5–§5.6). | §4.2 / §4.3 / §4.4 (row CAS chosen). |

---

## 9. References

1. [Gravitino Iceberg REST](../docs/iceberg-rest-service.md); [policies](../docs/manage-policies-in-gravitino.md); [compaction policy](../docs/iceberg-compaction-policy.md)
2. [Expire](./iceberg-expire-snapshots-maintenance-job.md) / [rewrite-manifests](./iceberg-rewrite-manifests-job.md) / [remove-orphan](./iceberg-remove-orphan-files-maintenance-job.md) design docs
3. [Optimizer overview](../docs/table-maintenance-service/optimizer.md)
4. [Amoro AIP-3](https://cwiki.apache.org/confluence/display/AMORO/AIP-3%3A+Event-Triggered+Optimization+of+Iceberg+Tables+in+Amoro); [Amoro configs](https://amoro.apache.org/docs/latest/configurations/)
5. [Floe policies](https://github.com/nssalian/floe/blob/main/docs/policies.md); [AWS Glue optimizers](https://docs.aws.amazon.com/glue/latest/dg/table-optimizers.html)
6. [Databricks auto compaction](https://docs.databricks.com/aws/en/tables/tune-file-size); [OpenHouse](https://github.com/linkedin/openhouse/blob/main/ARCHITECTURE.md)
7. Gravitino `IcebergCleanupManager` / `IcebergCleanupJobStore.takePendingJob`
