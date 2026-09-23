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
- **Scheduled path** — each node runs a **`MaintenancePoller`** (`takePendingDue` per-row claim).
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
4. **Dual trigger (option B)**: **Compaction** on **commit** (§5.4.1) **and** on the **poller** at
   wall-clock schedule (§5.4.2). Manifest rewrite, snapshot expiry, and orphan cleanup use the
   poller only (§5.5–§5.6). Policy **schedule** drives `next_due_at` for the **poller only**
   (§5.2.4).
5. **Wall-clock schedules in Gravitino**: Schedules live in Gravitino and are read by the poller,
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
   **per-node pollers** and **per-row claims** (§5.3).
4. **Provider SPI rewrite**: Does not replace `StatisticsUpdater`, `StatisticsCalculator`,
   `StatisticsProvider`, `StrategyProvider`, `TableMetadataProvider`, or `JobSubmitter`.
5. **Engine-side commit report**: Engines that bypass Gravitino Iceberg REST are out of scope for
   commit-path compaction.
6. **Commit-path HTTP or Kafka**: No `POST …/events/iceberg-commit`, no health resource, no Kafka.
   Commit handling is **in-process only** (§5.1.1).
7. **External clock APIs**: No `POST …/maintenance/run-due` (or CronJob) as an alternate timed clock.
   The built-in `MaintenancePoller` is the only schedule driver; §7 is for manual / CLI runs only.

---

## 4. Solution Investigations

### 4.1 Deployment options

| Approach                          | Pros                         | Cons / why rejected                                      | Decision   |
| --------------------------------- | ---------------------------- | -------------------------------------------------------- | ---------- |
| A: Process-local only             | Simple; no new listener      | No IRC target; no central automated maintenance          | Rejected   |
| C: Separate TMS process           | Full JVM isolation           | Extra deployable; duplicates main-server plugin patterns | Rejected   |
| D: Aux Jetty listener (:9301)     | Classpath isolation like IRC | Extra port; diverges from **8090** `extensionPackages`   | Rejected   |
| **B: In-process plugin (Chosen)** | See below                    | —                                                        | **Chosen** |

**Option B (Chosen):** Register TMS as a Jersey 2 `Feature` via
`gravitino.server.rest.extensionPackages` (same as IdP) inside the main server. Commit path does
**not** use HTTP. After each Iceberg commit, the colocated IRC hook INSERTs
`table_maintenance_event` (§6.3) and invokes an in-process callback; TMS runs compaction evaluate →
submit on a bounded executor (§5.4.1). Timed work uses `MaintenancePoller` on every node (§5.3).

**Pros:** No extra process or port; no remote hop on commit; reuses Policy + Jobs; peers stay equal
(no maintenance leader).

### 4.2 Industry survey: commit / write-path triggers

**Compaction** is most often tied to **writes/commits**. Manifest rewrite, expire, and orphan usually
are **not** run on every commit.

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

**TMS decision:** IRC commit path runs **`system_iceberg_compaction` only** (§5.4.1). Poller runs
**all four** types on schedule (§5.4.2, §5.2.4). Manifest / expire / orphan are **poller-only**.
Nightly compaction covers tables that stop receiving commits.

### 4.3 Industry: how defaults reach tables, and why TMS uses discovery

Products close the gap from catalog/scope defaults to runnable table work differently:

| Product            | How a catalog / scope default becomes runnable work                                                                                                                                                                                                                                                                                                |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **AWS Glue**       | Catalog stores a **default template**. A table is not optimized until a **table-level optimizer** exists — usually copied from the catalog on **CreateTable / UpdateTable**. Enabling catalog defaults does **not** instantly arm every existing table.                                                                                            |
| **Apache Amoro**   | Catalog `self-optimizing.*` (and related) settings are **merged at runtime** into table config for tables AMS already manages. A table that is not yet in AMS / not yet seen by the periodic scheduler does not run expire / orphan. Catalog changes apply on the next config read for tables **without** table-level overrides (table props win). |
| **Floe / CronJob** | On each **cron tick**, the service **lists tables in the policy scope** from an external catalog (metadata need not live in Floe) and runs work. Tables or policies created between ticks wait for the **next** tick.                                                                                                                              |

**Gravitino constraint:** policies live in Gravitino, but Iceberg inventory for an IRC Hive backend
may live only in **HMS** (tables outside IRC never appear in `table_meta`). TMS cannot assume a
complete in-process table list or Create/Update-only copy.

**Alternatives considered for above-table attachments:**

| Approach                                                     | Pros                                      | Cons for Gravitino                                                                                                                            |
| ------------------------------------------------------------ | ----------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| Cron tick: list whole schema/catalog, resolve policy, submit | Conceptually simple (Floe-like)           | Every due cycle re-lists large scopes; multi-node needs a leader or duplicate submits; hard to share claim/`next_due_at` with the commit path |
| Glue-like copy on Create/Update only                         | Cheap when all creates go through one API | Misses HMS-only tables; existing tables not re-armed when catalog policy changes                                                              |
| Amoro-like runtime merge over Gravitino entities only        | No separate discovery loop                | Incomplete inventory when metadata is outside Gravitino                                                                                       |

**TMS decision — discovery (§5.2.5, §5.3.5):**

1. **Table attachment:** write `table_maintenance_state` immediately (O(1)).
2. **Above-table:** periodic **discovery** lists scope from Iceberg/HMS, resolves nearest-wins
   `effective_policy`, UPSERTs missing state rows with `next_due_at`.
3. **`takePendingDue`:** cheap indexed claim on existing rows, separate from discovery.

Bounded lag (default 1h) before a new table is scheduled is the same class of gap other products
have, while due-work stays scalable with per-row claims.

### 4.4 Industry: multi-node schedule coordination without HA

Peer nodes typically use one of four patterns: **1–2** = policy/job grain; **3** = row grain;
**4** = external Cron + queue.

| Pattern                                       | Typical products                                                                                        | Pros                                                                                  | Cons                                                                                                                            | Chosen? Reason                                                                                                                                                  |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **1. Per-node cron + distributed lock**       | ShedLock; Spring + Redis/DB lock                                                                        | Simple; no leader election; prevents double runs of the same job                      | Often a **single lock for the whole job** — hard to parallelize **per table**                                                   | **Rejected.** Coarse lock serializes all tables under one policy/job; TMS must run many tables concurrently across peers.                                       |
| **2. Shared DB compete for trigger**          | Quartz JDBC Cluster                                                                                     | Mature; peers share work **by job**; one winner per fire                              | Trigger is typically **policy/job-scoped**; locking can bottleneck many short fires; heavier stack                              | **Rejected.** Same grain as pattern 1: winner then lists the policy scope. Does not give per-table claim shared with the commit path.                           |
| **3. Due rows + row-level CAS**               | TMS / `IcebergCleanupManager`; Temporal lease; Hangfire; db-scheduler; SQS visibility timeout (analogy) | No leader; **row-level** parallelism; lease reclaim on crash; fits large table counts | Needs a **state table** + lease; needs **materialization / discovery** for above-table policies                                 | **Chosen** (§5.3). Matches in-tree cleanup; poller and commit path share the same `(table, policy)` claim; scales with due rows, not with a single job lock.    |
| **4. External cron enqueue + multi-consumer** | OpenHouse CronJob; Floe; cloud Cron → SQS                                                               | Decouples trigger from execution; consumers scale on the **external** queue           | Requires **extra components** (external Cron and/or message queue); duplicate-enqueue and consumer **idempotency** still needed | **Rejected.** TMS must not introduce other runtime components beyond Gravitino and its entity DB. Pattern **3** keeps coordination in-process + existing store. |

**TMS mapping:** discovery (§4.3 / §5.3.5) materializes due rows; `takePendingDue` + CAS is
pattern **3**.

---

## 5. Proposal

### 5.1 Architecture

```text
Spark / Flink / Trino → Iceberg REST commit
        v
Gravitino IRC (:9001, same JVM as main server)
        └─ post-commit hook (§5.1.1, §5.4.1)
                ├─ INSERT table_maintenance_event (§6.3)
                └─ in-process callback → bounded executor → compaction only

MaintenancePoller (every node — §5.3)
        takePendingDue → claim → evaluate → submit
        ├─ Compaction due (§5.4.2) — e.g. Daily · 02:00
        ├─ Track A (§5.5): manifest | expire (soft: manifest before expire; worst-first)
        ├─ Track B (§5.6): orphan (oldest-cleanup-first; olderThan floor)
        └─ shared: optional maintenance window + maxConcurrentJobs
        v
Gravitino Job framework + job_run_meta (§6.5)
```

#### 5.1.1 In-process commit callback

After a successful Iceberg commit, the IRC hook **INSERTs** one `table_maintenance_event` (§6.3)
and invokes `IcebergCommitEventHandler`, which enqueues compaction on the bounded executor (§5.4.1).
No non-compaction policy resolve, Recommender, or submit on the IRC thread.

| Requirement     | Detail                                                                          |
| --------------- | ------------------------------------------------------------------------------- |
| Deployment      | IRC and main server share **one JVM**.                                          |
| Transport       | In-process only — **no** HTTP, **no** Kafka.                                    |
| Payload         | Normalized `table_identifier`; event row in §6.3.                               |
| Commit scope    | **`system_iceberg_compaction` only** (§5.4.1).                                  |
| IRC thread cost | One event `INSERT` + callback hand-off; evaluate/submit on the bounded executor. |

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
[expire](./iceberg-expire-snapshots-maintenance-job.md),
[rewrite-manifests](./iceberg-rewrite-manifests-job.md),
[remove-orphan](./iceberg-remove-orphan-files-maintenance-job.md).

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

#### 5.2.4 Policy schedule (UI: Daily · 02:00, Sun · 03:00, Weekly, Paused)

Each policy stores a **schedule** in `policy_meta.content`; TMS sets wall-clock **`next_due_at`** from it.

**Illustrative UI rows and content:**

| Policy name (UI)         | Applies to   | Schedule (UI) | Status | `content.schedule` (stored)                                            |
| ------------------------ | ------------ | ------------- | ------ | ---------------------------------------------------------------------- |
| `nightly_compaction`     | All Iceberg… | Daily · 02:00 | Active | `{ "type": "daily", "at": "02:00", "timezone": "UTC" }`                |
| `weekly_snapshot_expiry` | All Iceberg… | Sun · 03:00   | Active | `{ "type": "weekly", "day": "SUN", "at": "03:00", "timezone": "UTC" }` |
| `manifest_rewrite`       | `events.*`   | Daily · 04:00 | Active | `{ "type": "daily", "at": "04:00", "timezone": "UTC" }`                |
| `orphan_cleanup`         | All          | Weekly        | Paused | `{ "type": "weekly", "day": "SUN", "at": "04:00" }` + `enabled: false` |

**`next_due_at` computation:**

```text
on create/schedule change: next_due_at = nextOccurrence(schedule, timezone)
on job finish: next_due_at = nextOccurrence(schedule, timezone, after = job_finished_at)
```

**Paused:** `enabled = false` → poller and commit path skip.

**`minIntervalMs`:** min gap between runs via `last_job_id` → `job_finished_at` on **both** paths.

**Commit vs poller:** `schedule` / `next_due_at` are **poller only**; commit neither reads nor advances them.

#### 5.2.5 State materialization (where `next_due_at` is written)

`next_due_at` lives only on table-level `table_maintenance_state` rows.

| Attachment grain                              | When state rows are written                             | Behavior                                                                                          |
| --------------------------------------------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------------------- |
| **Table**                                     | **Immediately** on associate / schedule change / detach | O(1) UPSERT/DELETE; set `next_due_at = nextOccurrence(schedule)`                                  |
| **Above table** (schema / catalog / metalake) | **Timed discovery** (§5.3.5)                            | Bind association only; discovery lists scope, nearest-wins, UPSERT missing rows with `next_due_at` |

```text
effective_policy(table, type) → policy_id   // nearest Active along table → schema → catalog → metalake

UPSERT table_maintenance_state
  (metalake_id, table_identifier, policy_id, state='IDLE',
   next_due_at = nextOccurrence(schedule), …)
```

Only **one** state row per `(table, maintenance_type)` effective policy.

**Optional create-table hook (§5.3.5):** after IRC `createTable`, UPSERT state for ancestor policies;
discovery still covers HMS-only tables.

---

### 5.3 Scheduled path: `MaintenancePoller` (poll + `takePendingDue`)

Same model as **`IcebergCleanupManager`**: every node polls due rows and claims with CAS.

#### 5.3.1 Poller lifecycle

On start (`gravitino.maintenance.poller.enabled=true`, §8.1): start `workerThreads`, shared
`refreshClaimHeartbeats` (poller + commit claims), and discovery (§5.3.5). Each worker: if at
`maxConcurrentJobs` sleep; else `takePendingDue` (one track, §5.3.2) → claim → evaluate → submit.
Shutdown stops loops; stale claims expire via `heartbeatTimeoutMs`.

`takePendingDue` vs discovery:

| Loop           | Default interval                        | Work                                                       |
| -------------- | --------------------------------------- | ---------------------------------------------------------- |
| Claim due work | `pollIntervalSecs` = **30**             | Select existing state rows with `next_due_at <= now`       |
| Discovery      | `discoveryIntervalSecs` = **3600** (1h) | List tables in attachment scope; INSERT missing state rows |

#### 5.3.2 Due rows (`next_due_at`)

Each `table_maintenance_state` row for an attached, enabled policy carries:

| Field                                | Role                                                                                    |
| ------------------------------------ | --------------------------------------------------------------------------------------- |
| `next_due_at`                        | Epoch millis when this row becomes eligible for `takePendingDue` (from §5.2.4 schedule) |
| `state`                              | `IDLE` (claimable) or `RUNNING` (a node owns evaluate → submit)                         |
| `claim_lease_expires_at` / heartbeat | Reclaim stale `RUNNING` like `iceberg_cleanup_job.heartbeat_at`                         |

**When `next_due_at` is set:** on materialize → `nextOccurrence(schedule)`; after success →
`nextOccurrence(schedule, after = job_finished_at)`. All four types when `next_due_at <= now` and
enabled.

**Candidate selection:** separate `takePendingDue` per track. `health_score` /
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

#### 5.3.3 Multi-node behavior

```text
Node A/B/C/D claim different (table, policy) rows in parallel — no leader
Node A dies → heartbeat expires → peer reclaim via takePendingDue
```

`maxConcurrentJobs` (§8.1) caps cluster in-flight Spark jobs:

```sql
SELECT COUNT(*) FROM table_maintenance_state
 WHERE state = 'RUNNING' AND job_id IS NOT NULL
```

If `COUNT >= maxConcurrentJobs`, do not claim (small race overshoot OK).

#### 5.3.4 External clock APIs (out of scope)

Do **not** expose `POST …/maintenance/run-due` as an alternate timed clock. `MaintenancePoller` is
the only schedule driver; §7 is for manual / CLI runs only.

#### 5.3.5 Scope discovery (above-table attachments)

Discovery expands schema / catalog / metalake attachments into **table-level** state rows (§5.2.5).
It does **not** replace `takePendingDue`. **Why discovery:** §4.3.

**Catalog source:** list tables from the **Iceberg/HMS** backend used by IRC — not only Gravitino
`table_meta`.

**One discovery round (illustrative):**

```text
1. Load enabled policies + attachments
2. For each above-table attachment: listTables(scope) via Iceberg/HMS
3. policy_id = effective_policy(table, type)  // nearest-wins
4. INSERT missing state rows with next_due_at = nextOccurrence(schedule)
5. Reconcile (required): DELETE stale (table, policy) rows; drop tables that left scope
6. Cap with discoveryBatchSize
```

**Optional IRC create-table hook:** UPSERT ancestor state on IRC `createTable`; discovery still covers
non-IRC creates.

**Examples (scheduled compaction on a schema):**

| Situation                                                                      | Expected                                                                            |
| ------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------- |
| Table created outside IRC; later **commit via IRC**                            | Commit path: event INSERT → compaction accelerator (§5.4.1)                         |
| Table created outside IRC; **no** IRC commits; schema has scheduled compaction | Discovery sees the table in HMS → state row → poller runs compaction when due       |
| Table created outside IRC; commits also bypass IRC                             | No commit-path compaction; scheduled path still works if discovery listed the table |

---

### 5.4 Compaction: commit path + poller schedule (option B)

Compaction is the **only** type with two wake sources: IRC commit (§5.4.1) and poller schedule
(§5.4.2).

#### 5.4.1 Commit path (compaction only)

```text
IRC commit succeeded → post-commit hook (§5.1.1)
  ├─ INSERT table_maintenance_event (§6.3)   ← never UPDATE
  └─ IcebergCommitEventHandler → bounded executor:
        resolve effective compaction policy (§5.2.3); skip if disabled
        UPSERT state if missing (next_due_at = nextOccurrence(schedule))
        minIntervalMs gate (event.created_at, last_job_id — §6.3)
        claim row (§6.1) + register refreshClaimHeartbeats
        Recommender → submit; record job_run_meta (§6.5); release to IDLE
        // do not advance next_due_at — poller owns schedule (§5.2.4)
```

IRC thread: event INSERT + hand-off only. No `next_due_at` check; `minIntervalMs` + Recommender
decide submit. Missing state → UPSERT before claim. Executor queue full → best effort (§10); next
commit or poller can still drive. Multi-node: claim (§6.1) prevents double-submit.

#### 5.4.2 Poller path (scheduled compaction)

Poller claims compaction rows with `next_due_at <= now`, then evaluate → submit; after success
`next_due_at = nextOccurrence(schedule)` (§5.2.4). Inactive tables still compact on schedule;
active ones may no-op via `minIntervalMs` + Recommender. Manifest / expire / orphan: **poller only**.

#### 5.4.3 When commit and poller meet

Same row: **claim** + `minIntervalMs` + in-flight `job_id` prevent duplicate Spark jobs (§6.1).

---

### 5.5 Hot pipeline (scheduled — Track A)

Track A: **manifest** and **expire**, each with its own state row / claim — **no** multi-policy claim.
Soft order (§5.3.2): prefer manifest before expire; skip expire while that table has a due/RUNNING
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
release and push `next_due_at` (§5.3.2). Enforce `olderThan` floor on evaluate and policy write (§8.1).

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

Pattern: `IcebergCleanupManager` / `takePendingJob` (§5.3).

---

### 5.8 User process

1. Enable TMS plugin + `iceberg-rest` in the same JVM; turn on in-process callbacks (§5.1.1 / §8.2).
2. Apply `standard` profile or create/attach four policies; set schedules (§5.2.4).
3. Enable poller (`gravitino.maintenance.poller.enabled=true`, §8.1).
4. IRC commits INSERT events + compaction callback (§5.4.1); poller claims due rows (§5.3).
5. Observe via Jobs UI; manual runs via §7.

---

## 6. Multi-node coordination (shared claim)

Shared `table_maintenance_state` in the entity DB; identity is `table_identifier`
(`catalog.schema.table`), not `table_meta.table_id`.

| Table                     | Role                                                                                    |
| ------------------------- | --------------------------------------------------------------------------------------- |
| `table_maintenance_event` | **Commit log** — one INSERT per successful Iceberg commit (§6.3)                        |
| `table_maintenance_state` | Multi-node **claim**, in-flight `job_id`, finished `last_job_id` per policy (§6.1–§6.2) |

Every node polls; **per-row CAS** picks the winner. PK: `(metalake_id, table_identifier, policy_id)`.

### 6.1 Claim flow (`takePendingDue` / commit path)

**Poller** (same CAS as `iceberg_cleanup_job.markRunning`):

```text
Both nodes SELECT due candidates → both CAS claim (IDLE or stale RUNNING):
  UPDATE … SET state=RUNNING, claimed_by=:nodeId, claim_lease_expires_at=…
  WHERE … AND (state=IDLE OR (state=RUNNING AND claim_lease_expires_at < now))
  winner (rows_affected=1) → heartbeat → evaluate → submit → IDLE + next_due_at
  (or interval gate fail → IDLE + push next_due_at, §5.3.2)
  loser → next candidate
refreshClaimHeartbeats covers poller + commit-executor claims
```

**Commit path** uses the same CAS and **must** register heartbeats; it does **not** read/advance
`next_due_at`. Claim + `minIntervalMs` bound duplicates; **claim is the write lock** (gates alone
race).

### 6.2 State table (shared store)

**Table name:** `table_maintenance_state`

| Column                       | Type                       | Notes                                    |
| ---------------------------- | -------------------------- | ---------------------------------------- |
| `metalake_id`                | `BIGINT UNSIGNED NOT NULL` | Metalake owning the policy               |
| `table_identifier`           | `VARCHAR(512) NOT NULL`    | Normalized `catalog.schema.table`        |
| `policy_id`                  | `BIGINT UNSIGNED NOT NULL` | `policy_meta.policy_id`                  |
| `state`                      | `VARCHAR(16) NOT NULL`     | `IDLE` / `RUNNING`                       |
| `next_due_at`                | `BIGINT NOT NULL`          | Poller eligibility, epoch millis (§5.3)  |
| `updated_at`                 | `BIGINT NOT NULL`          | Claim / reclaim / heartbeat              |
| `job_id`                     | `BIGINT UNSIGNED NULL`     | In-flight `job_run_id`                   |
| `last_job_id`                | `BIGINT UNSIGNED NULL`     | Last finished; drives min-interval       |
| `last_measured_snapshot_id`  | `BIGINT NULL`              | Snapshot at last evaluate (§10.3)        |
| `claimed_by`                 | `VARCHAR(128) NULL`        | Node holding `RUNNING` (§6.1)            |
| `claim_lease_expires_at`     | `BIGINT NULL`              | Lease expiry; reclaim after (§10.3)      |
| `submission_idempotency_key` | `VARCHAR(64) NULL`         | Written before submit (§10.3)            |

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

IRC INSERTs one row per successful commit (TMS never UPDATEs). Keyed by `table_identifier` string,
not `table_meta.table_id`. Records that a commit happened; no `snapshot_id`.

```text
IRC post-commit hook → INSERT event → callback → bounded executor (§5.4.1)
```

| Column             | Type                       | Notes                             |
| ------------------ | -------------------------- | --------------------------------- |
| `event_id`         | `BIGINT UNSIGNED NOT NULL` | Surrogate PK (auto-increment)     |
| `metalake_id`      | `BIGINT UNSIGNED NOT NULL` | Metalake from config / resolution |
| `table_identifier` | `VARCHAR(512) NOT NULL`    | Normalized `catalog.schema.table` |
| `created_at`       | `BIGINT NOT NULL`          | Insert time, epoch millis         |

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

Commit path and poller do **not** call these routes. They replace the `gravitino-optimizer` CLI on
**8090**.

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

| Key                                                  | Default     | Description                                                         |
| ---------------------------------------------------- | ----------- | ------------------------------------------------------------------- |
| `gravitino.server.rest.extensionPackages`            | none        | TMS Feature package.                                                |
| `gravitino.auxService.names`                         | none        | Include `iceberg-rest` when using IRC.                              |
| `gravitino.maintenance.claimLeaseMs`                 | `300000`    | Claim lease → `claim_lease_expires_at` (§6.1).                      |
| `gravitino.maintenance.executor.threads`             | `4`         | Bounded executor threads (§5.4.1).                                  |
| `gravitino.maintenance.executor.queueSize`           | `10000`     | Bounded executor queue depth.                                       |
| `gravitino.maintenance.poller.enabled`               | `true`      | Enable `MaintenancePoller` (§5.3).                                  |
| `gravitino.maintenance.poller.workerThreads`         | `2`         | Poller workers per node.                                            |
| `gravitino.maintenance.poller.pollIntervalSecs`      | `30`        | Sleep when no due row claimed.                                      |
| `gravitino.maintenance.poller.discoveryIntervalSecs` | `3600`      | Discovery interval for above-table attachments (§5.3.5).            |
| `gravitino.maintenance.poller.discoveryBatchSize`    | `500`       | Max tables per discovery round.                                     |
| `gravitino.maintenance.poller.heartbeatTimeoutSecs`  | `300`       | Reclaim stale `RUNNING`.                                            |
| `gravitino.maintenance.poller.candidateWindow`       | `8`         | Max candidates per `takePendingDue`.                                |
| `gravitino.maintenance.poller.maxConcurrentJobs`     | `10`        | Cap: COUNT `RUNNING` + `job_id` (§5.3.3).                           |
| `gravitino.maintenance.poller.maintenanceWindow`     | none        | Optional UTC window; skip submit outside.                           |
| `gravitino.maintenance.orphan.olderThanMinMs`        | `259200000` | Min `olderThan` (3 days) for orphan (§5.6).                         |

Per-table cadence is **`minIntervalMs`** / **`next_due_at`**. `pollIntervalSecs` = claim frequency;
`discoveryIntervalSecs` = expansion frequency.

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.auxService.names = iceberg-rest
gravitino.iceberg-rest.tableMaintenance.inProcess = true
gravitino.maintenance.poller.enabled = true
gravitino.maintenance.poller.maxConcurrentJobs = 10
```

### 8.2 Iceberg REST → TMS in-process event keys

| Key (illustrative)                                  | Default | Description                                   |
| --------------------------------------------------- | ------- | --------------------------------------------- |
| `gravitino.iceberg-rest.tableMaintenance.inProcess` | `false` | IRC invokes compaction callback after commit. |

### 8.3 Task types and minimum interval (per policy type)

Each type has its own `minIntervalMs`, compared per `(table, policy_id)` via
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

| Phase | Work item                        | Notes                                                          |
| ----- | -------------------------------- | -------------------------------------------------------------- |
| 1     | In-process plugin + commit event | Feature; IRC INSERT + callback; bounded executor (§5.4.1)      |
| 2     | State + event tables + claim     | Event + state materialization; nearest-wins (§5.2, §6)         |
| 3     | `MaintenancePoller` + discovery  | `takePendingDue` + Iceberg/HMS discovery (§5.3)                |
| 4     | Compaction on poller schedule    | Poller compaction; commit path unchanged (§5.4)                |
| 5     | Track A / B                      | Hot pipeline + orphan (§5.5–§5.6)                              |
| 6–8   | Profile API, ops APIs, harden    | §5.2.2, §7, §10                                                |

#### Phase 1–4 checklist

- [ ] Phase 1: event migration; IRC INSERT + callback; bounded executor (§5.4.1, §6.3, §8.2).
- [ ] Phase 3: poller workers; discovery from Iceberg/HMS; immediate table attach; CAS
      `takePendingDue`; heartbeats; `next_due_at`; all four types; Track A soft order; orphan
      oldest-first + interval gate; `maxConcurrentJobs`; multi-node claim tests; commit-path
      heartbeat registration; discovery reconcile (§5.2–§5.6, §6.1).
- [ ] Phase 4: poller advances compaction `next_due_at`; commit path does not; claim +
      `minIntervalMs` vs double-submit; commit ignores non-compaction (§5.4).

### 9.2 Review Checklist

| Area            | Checklist                                                                                         |
| --------------- | ------------------------------------------------------------------------------------------------- |
| Deployment      | `extensionPackages`; IRC same JVM; ops on **8090** (§7).                                          |
| Policy          | Four types; nearest-wins; table attach immediate; above-table discovery (§5.2).                   |
| Trigger         | Compaction: commit + poller; others: poller + `next_due_at` (§5.4, §5.2.4).                       |
| Discovery       | Iceberg/HMS list; separate from `takePendingDue` (§5.3.5, §4.3).                                  |
| Multi-node      | No leader; per-row CAS like `IcebergCleanupManager` (§5.3, §4.4).                                 |
| Executor        | Bounded; evaluate **off** commit thread (§5.4.1).                                                 |
| Durability      | Event INSERT per commit (§6.3); state for claim/schedule (§6.2).                                  |
| Orchestration   | Track A soft order; orphan separate (§5.5–§5.6).                                                  |
| Industry        | §4.2 / §4.3 / §4.4 (row CAS chosen).                                                              |
| Fault tolerance | Poller at-least-once latest-state; commit best effort (§10).                                      |

---

## 10. Fault tolerance and delivery guarantees

### 10.1 Delivery models

| Model                      | TMS target                                                               |
| -------------------------- | ------------------------------------------------------------------------ |
| Best effort                | **Commit compaction path** (§5.4.1)                                      |
| At-least-once latest-state | **`MaintenancePoller` path** — recovery from table state, not event rows |
| Exactly-once job effect    | **Not required** — claims + idempotency key bound duplicates             |

**Commit path:** durable after event INSERT; dropped executor task → next commit or poller. Manifest /
expire / orphan are poller-only.

**Poller path:** coalescing OK — act on **current** state; due row stays due until claim + complete.

### 10.2 Recovery is driven by table state

Recovery / interval gates use current snapshot vs `last_measured_snapshot_id`, per-policy
`last_job_id` / `job_id`, and `minIntervalMs` (commit path joins `event.created_at` — §6.3).

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

1. [Gravitino Iceberg REST](../docs/iceberg-rest-service.md); [policies](../docs/manage-policies-in-gravitino.md); [compaction policy](../docs/iceberg-compaction-policy.md)
2. [Expire](./iceberg-expire-snapshots-maintenance-job.md) / [rewrite-manifests](./iceberg-rewrite-manifests-job.md) / [remove-orphan](./iceberg-remove-orphan-files-maintenance-job.md) design docs
3. [Optimizer overview](../docs/table-maintenance-service/optimizer.md)
4. [Amoro AIP-3](https://cwiki.apache.org/confluence/display/AMORO/AIP-3%3A+Event-Triggered+Optimization+of+Iceberg+Tables+in+Amoro); [Amoro configs](https://amoro.apache.org/docs/latest/configurations/)
5. [Floe policies](https://github.com/nssalian/floe/blob/main/docs/policies.md); [AWS Glue optimizers](https://docs.aws.amazon.com/glue/latest/dg/table-optimizers.html)
6. [Databricks auto compaction](https://docs.databricks.com/aws/en/tables/tune-file-size); [OpenHouse](https://github.com/linkedin/openhouse/blob/main/ARCHITECTURE.md)
7. Gravitino `IcebergCleanupManager` / `IcebergCleanupJobStore.takePendingJob`
