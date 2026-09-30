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

1. There is no stable **server-side commit event** after Iceberg commits. Engines would otherwise each need a
   TMS-specific listener, or operators must schedule maintenance externally.
2. Configuration, audit, and service-level metrics are hard to centralize when execution is
   ad hoc and process-local.
3. Each run creates its own runtime and provider instances instead of a shared service lifecycle.
4. When Spark maintenance work is needed, submitted work already returns a `jobId` owned by the
   Gravitino job framework. That job-status boundary should stay.
5. There is no **embedded scheduler** for stale-claim reclaim on multi-node deployments.

This design turns TMS into a **main-server REST plugin** on port **8090** (same pattern as IdP
via `gravitino.server.rest.extensionPackages`) so colocated IRC can **enqueue** per-policy expand
work after commits. **db-scheduler** owns leases on `scheduled_tasks` for **two** task kinds:
**(1) policy-expand** (parse a policy into Spark work units; long-lived) and **(2) spark-submit**
(one-shot; deleted after pick). One Gravitino table (`table_maintenance_job`) stores each
maintenance job run, Validation snapshots, and submit gates. The optimizer execution core runs
in-process after a spark-submit pick.

---

## 2. Goals

1. **In-process plugin on the main server**: Load Table Maintenance through
   `gravitino.server.rest.extensionPackages` (Jersey 2 `Feature`, same pattern as IdP) so the IRC
   callback is registered in the main JVM. The commit path does **not** use HTTP. Operator calls that
   replace the optimizer CLI are the ops APIs in **§7**.
2. **IRC in-process commit event**: After successful Iceberg commits via IRC, TMS receives a commit
   event through a **main-server-registered in-process callback / SPI** (IRC and main server share one
   JVM; see **§5.1.1**). The handler resolves attached, commit-allowed types for that table and drives
   the **ordered** expand / ② chain (§5.7.1; skip types not attached; **no** orphan-cleanup) — it does
   **not** call `runJob` on the commit thread (§5.4).
3. **Two-tier db-scheduler tasks on `scheduled_tasks`**:
   - **`tms-policy-expand`** (type ①): created when the maintenance policy is created / enabled;
     instance key includes **`policy_id`**. Parses the policy into zero or more Spark work units and
     **INSERT**s **`tms-spark`** rows. The expand row is **retained** (crontab / next due); delete or
     rewrite only when `policy_meta` is altered / disabled / dropped (§5.5, §6.1).
   - **`tms-spark`** (type ②): one-shot Spark submit units written by expand. When a node **picks**
     and finishes the submit pipeline (gates → `runJob` → `table_maintenance_job` INSERT), that
     **`tms-spark` row is DELETED**. Spark itself continues asynchronously in the job framework.
   All nodes poll; **N compete, one pick wins** per due instance. Do **not** hold a pick until Spark
   finishes.
4. **Reuse existing optimizer execution core**: Scheduler task handlers invoke the same `Updater` /
   `Recommender` / job-submit paths already present in `maintenance/optimizer`, as **in-process
   methods**, not as a second copy of the logic.
5. **Job framework compatibility**: Spark maintenance work continues to use the Gravitino job
   framework. TMS records each run in `table_maintenance_job` but does not own job status.
   Automate Jobs **Validation** before/after metrics live on the same row (§6.2).
6. **Govern Policy reuse**: Maintenance policies stay on existing `policy_meta` and metalake Policy
   APIs (create / alter / enable / disable / associate). TMS does **not** introduce a parallel policy
   store or `/api/maintenance/table/policies` CRUD.
7. **Multi-node safe execution**: db-scheduler pick + heartbeat is the mutex for each
   `scheduled_tasks` instance — expand and spark-submit alike (§5.5, §6). Concurrent Spark submits
   for the same `(table, policy)` are also gated by an in-flight `table_maintenance_job` row
   (`finished_at IS NULL`) (§6.2).
8. **Per-run maintenance job table**: Gravitino persists one `table_maintenance_job` row per Spark
   `job_run_id` (Validation JSON + `finished_at`). Enqueue / reclaim for **scheduler tasks** live in
   `scheduled_tasks` (two kinds above).
9. **Commit / crontab drive expand; expand drives spark rows**: IRC commit and crontab only make
   **`tms-policy-expand` due** (§5.4, §5.7). Expand applies **`minIntervalMs`** (and in-flight) per
   work unit, then writes **`tms-spark`** rows using `table_maintenance_job` (`MAX(finished_at)` /
   `finished_at IS NULL`) (§5.5, §6.2). ② re-checks **in-flight** before `runJob`.
10. **Dedicated TMS execution principal**: All automated maintenance (event enqueue after commit
    and timed policy due) submits Jobs as a built-in metalake user **`tms`**, not as the operator
    who created the policy (§5.6).
11. **Policy evaluate triggers on policy**: Automated maintenance uses `onCommit` and/or `crontab`
    in `policy_version_info.content.schedule` (§5.7). One policy row holds both triggers when needed;
    separate policies are for different maintenance **types**, not for separate trigger modes.
12. **Job template parameters on policy**: Non-auth Spark / job-template parameters live in
    maintenance policy content (`jobOptions` / existing `rewriteOptions`). Attach the same policy
    type at catalog / schema / table; **nearest attachment wins** (table > schema > catalog)
    (§5.8).
13. **Auth credentials via SecretManager**: Passwords, tokens, and access keys used to run Spark
    Jobs are **not** stored in `policy_meta` or a TMS-owned credential table. Sensitive values are
    referenced through Gravitino **SecretManager** (URN / provider), aligned with the pluggable
    secret approach for server config. Opt-in: plaintext / missing secrets still work when the
    secret solution is not enabled (§5.9).

---

## 3. Non-Goals

1. **Standalone maintenance daemon**: No separate process or
   `gravitino-iceberg-rest-server.sh`-style entrypoint.
2. **Dedicated auxiliary HTTP listener**: No `GravitinoAuxiliaryService`, no isolated
   `gravitino.maintenance.classpath`, and no dedicated TMS port (for example **9301**). TMS is not
   a dedicated listener like `iceberg-rest` / `lance-rest`.
3. **Provider SPI rewrite**: Does not replace `StatisticsUpdater`, `StatisticsCalculator`,
   `StatisticsProvider`, `StrategyProvider`, `TableMetadataProvider`, or `JobSubmitter` contracts
   used by the event pipeline.
4. **Engine-side commit report path**: Engines that bypass Gravitino Iceberg REST are out of scope
   for event-driven path.
5. **Commit-path HTTP or Kafka**: No `POST …/events/iceberg-commit`, no health resource, and no Kafka
   produce/consume path. Commit handling is **in-process only** (§5.1.1). APIs that replace the
   optimizer CLI are **§7**, and they are not a commit ingress. Remote IRC / cross-JVM delivery is
   out of scope (follow-up if needed).
6. **Holding db-scheduler pick until Spark completes**: Expand and spark-submit callbacks must return
   after enqueue / submit (or skip). Long Spark lifetimes are gated by `table_maintenance_job` and
   the job framework, not by holding `picked` on a `tms-spark` row (that row is deleted after
   submit).
7. **Per-human-user templates for automated TMS**: Manual Automate Jobs UI may later store
   per-user defaults; automated event/timed runs always use the **`tms`** principal and policy
   `jobOptions` (§5.6–§5.8).

## 4. Solution Investigations

### 4.1 Option A: Keep process-local execution only

Continue running all optimizer work in ad hoc local processes, with no TMS service endpoint.

**Pros:** No new listener. Minimal implementation work.

**Cons:** No IRC event target; no centralized service for event-driven expand → spark-submit.

**Decision:** Rejected.

### 4.2 Option B: In-process plugin on the main server (Chosen)

Register Table Maintenance as a Jersey 2 `Feature` through
`gravitino.server.rest.extensionPackages` (same pattern as IdP) so it runs inside the main server
process. The commit path does **not** use HTTP. After each Iceberg commit, the **colocated** IRC hook
invokes a main-server-registered **in-process** callback that **bumps** long-lived
**`tms-policy-expand`** tasks. **All** TMS nodes run db-scheduler; one node picks each due instance
(expand ① or spark-submit ②) under a short lease.

**Pros:** No extra process or port; no remote event hop on the commit path; work is distributed
across replicas via scheduler pick; reuses Policy + Jobs; matches plugin packaging; lease heartbeat
is built into db-scheduler.

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

### 4.5 Two-tier scheduler tasks + per-run job records (db-scheduler — Chosen)

TMS needs cluster-safe scheduling for **two** kinds of work, both on `scheduled_tasks`:

1. **Policy-expand** — parse a maintenance policy into Spark work units (long-lived row per
   `policy_id`).
2. **Spark-submit** — one-shot units that actually call `runJob` (many rows; **deleted after pick**).

Plus durable **`table_maintenance_job`** rows so in-flight Spark and `minIntervalMs` / Validation
survive beyond the spark-submit pick.

Holding `picked` until Spark finishes ties scheduler threads to long jobs and is rejected
(Non-Goal #6). Spark execution stays in the Gravitino job framework; `tms-spark` only covers the
short submit callback.

#### Industry and in-project alternatives

| --------------------------------- | -------------------------------------------------------------------- | -------------------------------------------- | ---------------------------------------------------- | -------------------------------------------------------- |
| -------------------------------- | -------------------------------------------------------------------- | -------------------------------------------- | ---------------------------------------------------- | -------------------------------------------------------- |
|                                  | **[db-scheduler](https://github.com/kagkarlsson/db-scheduler)**      | [JobRunr](https://www.jobrunr.io/)           | [ShedLock](https://github.com/lukas-krecan/ShedLock) | [Quartz](https://www.quartz-scheduler.org/) JDBC cluster |
| License                          | Apache 2.0                                                           | LGPL v3 (+ commercial)                       | Apache 2.0                                           | Apache 2.0                                               |
| Embed in main server             | Yes                                                                  | Yes                                          | Yes                                                  | Yes                                                      |
| Cluster CAS / single-flight      | Yes (optimistic lock / `SKIP LOCKED` on `scheduled_tasks`)           | Yes                                          | Lock only                                            | Yes (`QRTZ_*` row locks)                                 |
| Heartbeat for short lease        | Yes (`last_heartbeat`)                                               | Yes                                          | N/A                                                  | Yes                                                      |
| Fits per-policy + one-shot Spark | Yes                                                                  | Yes                                          | No (lock only)                                       | Yes (heavier)                                            |
| H2 unit-test path                | Degraded: disable scheduler; run pipeline directly in tests (§5.5.3) | Better H2 story                              | Yes                                                  | RAMJobStore only in tests                                |
| Extra ops component              | No                                                                   | Optional dashboard server                    | No                                                   | No                                                       |
| Decision                         | **Chosen**                                                           | Rejected — license + overlaps Gravitino Jobs | Rejected — not a scheduler                           | Rejected — ~11 tables, heavy                             |

#### Why db-scheduler + two task kinds + job table

1. **Apache License 2.0** — safe for an ASF project; JobRunr is LGPL v3.
2. **Embeddable and light** — one `scheduled_tasks` table for both expand and spark-submit leases;
   starts/stops with `TableMaintenanceRESTFeature`.
3. **Built-in heartbeat** — while a short callback runs, db-scheduler refreshes `last_heartbeat`.
4. **Clear split** — `tms-policy-expand` = parse policy → enqueue spark units; `tms-spark` =
   submit one unit then **DELETE**; `table_maintenance_job` = Validation + submit gates; Gravitino
   Jobs runs Spark.
5. **No fat TMS state machine** — no `evaluate_pending` / `IDLE`/`RUNNING` twin of `picked`.

**Decision:** **Chosen** — db-scheduler with two task names; `table_maintenance_job` for per-run
records and submit gates.

### 4.6 Where automated Job configuration lives

#### 4.6.1 Job template parameters (`jobOptions`)

Non-auth Spark / job-template parameters (for example executor memory, shuffle partitions, Iceberg
`rewriteOptions`) for automated maintenance runs.

|          | Extra table keyed by catalog / schema / table                 | Policy content `jobOptions` (Chosen)                                              |
| -------- | ------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| Pros     | Explicit                                                      | Reuses `policy_meta` / `policy_relation_meta`; nearest attachment already defined |
| Cons     | Parallel attachment + precedence + UI beside policies; drifts | Auth material belongs in §4.6.2, not here                                         |
| Decision | Rejected                                                      | **Chosen** (§5.8)                                                                 |

#### 4.6.2 Authentication credentials

IRC client auth and credential-vending secrets for TMS Jobs (passwords, OAuth client credentials,
keytabs, and similar).

|          | Auth keys in policy `jobOptions`           | Dedicated `tms_credential` table                  | SecretManager + URN (Chosen)                                              |
| -------- | ------------------------------------------ | ------------------------------------------------- | ------------------------------------------------------------------------- |
| Pros     | One map with Spark options                 | Explicit TMS overlay                              | Pluggable providers (file / Vault / KMS); opt-in; shared with server conf |
| Cons     | Policies are widely readable; secrets leak | Second keystore beside SecretManager; no KMS path | Needs bootstrap for TMS principal secrets                                 |
| Decision | Rejected                                   | Rejected                                          | **Chosen** (§5.9)                                                         |

---

## 5. Proposal

### 5.1 Architecture

TMS uses **two kinds of rows in `scheduled_tasks`** (db-scheduler). Writing a row does **not** mean
every node runs it: **every** node polls; **N compete, one pick wins** per due instance.

| Kind                    | `task_name` (illustrative) | When created                                        | Instance key                                                                      | After pick                                                                            |
| ----------------------- | -------------------------- | --------------------------------------------------- | --------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- |
| ① Policy → Spark expand | `tms-policy-expand`        | Policy **create / enable** (and reconcile on alter) | `{policy_id}`                                                                     | Parse policy → **INSERT** zero or more ② rows; **keep** ① (set next `execution_time`) |
| ② Spark submit unit     | `tms-spark`                | Written by ① when expand runs                       | Crontab: `{policy_id}:{table_id}` / batch; **commit:** `{table_id}:{policy_id}[:{policy_id}…]` (§5.5.2) | Submit head → `table_maintenance_job` → **DELETE** ②; commit may enqueue remainder     |

**Lifecycle sketch:**

```text
Create / enable maintenance policy (policy_id = P)
  → INSERT scheduled_tasks ① tms-policy-expand / {policy_id}
     execution_time = next crontab (or far-future if onCommit-only until first commit)

onCommit / crontab due
  → bump ① execution_time = now (commit) or leave crontab due time

① picked (one node)
  → read policy_meta + attachments; list / filter tables (§5.5.4)
  → apply expand gates (in-flight / minIntervalMs) per candidate
  → crontab: INSERT ② per ungated unit (`{policy_id}:{table_id}`)
  → commit: INSERT one ② `{table_id}:{policy_id}:…` for ungated policies in §5.7.1 order
       (gated-out policies omitted — fewer policy_ids in the key)
  → set ① next execution_time from crontab (or leave until next commit bump)
  → do NOT delete ① (unless policy disabled / dropped / replaced)

② picked (one node per row; crontab ② may run in parallel across units)
  → gate (in-flight) → Recommender → runJob as tms (commit: first policy_id in the key)
  → INSERT table_maintenance_job (job_run_id + keys; finished_at NULL)
  → DELETE this ② scheduled_tasks row
  → return; Spark continues in job framework
  → terminal: UPDATE metrics; commit chain: if policy_ids remain, INSERT next ② with shorter key
```

```text
Spark / Flink / Trino
        │  Iceberg REST commit
        v
Gravitino IRC (:9001)
        │
        └─ post-commit → IcebergCommitEventHandler (§5.4)
                ├─ resolve attached onCommit types for this table
                ├─ order: compaction → manifest-rewrite → snapshot-expiry (§5.7.1)
                ├─ expand gates → drop policies still in minInterval / in-flight
                └─ enqueue commit ② `task_instance = {table_id}:{policy_id}:…`
                      (only remaining policy_ids; skip if none)

Node A / Node B / Node C  — each polls db-scheduler (§5.5); N compete, one pick wins
        │
        ├─ pick due ① tms-policy-expand
        │     ├─ expand policy → INSERT N × ② tms-spark (execution_time = now)
        │     └─ retain ①; set next crontab due (if any); release pick
        │
        ├─ pick due ② tms-spark
        │     ├─ ensureTableImported (§5.5.4)
        │     ├─ gate: in-flight? → else DELETE ② and return
        │     ├─ Recommender → runJob as `tms` → job_run_id
        │     ├─ INSERT table_maintenance_job (§6.2)
        │     ├─ DELETE this ② scheduled_tasks row
        │     └─ return (do NOT wait for Spark)
        v
                 Gravitino Job framework (async)
                 sample before_metrics before mutation; terminal UPDATE metrics (§6.2)

        ┌──────────────────────────────────────────────────────────────┐
        │  scheduled_tasks                                              │
        │  • ① tms-policy-expand — long-lived per policy_id             │
        │  • ② tms-spark — one-shot; DELETE after successful pick path  │
        │  • due ⇒ all nodes poll; only one pick per instance           │
        └──────────────────────────────────────────────────────────────┘
```

| Table                                  | Role                                                                                    |
| -------------------------------------- | --------------------------------------------------------------------------------------- |
| `policy_meta` / `policy_relation_meta` | **What** to expand, `schedule` triggers (§5.7), and non-auth `jobOptions` (§5.8)        |
| `scheduled_tasks`                      | ① expand + ② spark-submit leases (§5.5, §6.1)                                           |
| `table_maintenance_job`                | Per-run Validation JSON + `finished_at`; submit gates (§6.2)                            |
| SecretManager / SecretProvider         | TMS Spark / Iceberg **auth** material via URN (§5.9); not a TMS-owned table             |
| `user_meta`                            | Built-in metalake user `tms` when authorization is enabled (§5.6)                       |
| `job_run_meta`                         | Spark job run **record** (status + `runtime_job_template` snapshot); not default config |

#### 5.1.1 In-process commit event

Commit events are delivered **only in-process**. After a successful Iceberg commit, the **IRC
post-commit hook** invokes a **main-server-registered callback / SPI** (for example on
`GravitinoEnv`). That callback resolves Active policies with `onCommit`, builds the **ordered type chain** for the
committed table (§5.7.1), and drives expand / ② enqueue accordingly. It does **not** call `runJob`
on the commit path.
TMS does **not** persist a separate row per commit; the committed `snapshot_id` remains in Iceberg
table metadata (optional to record on `table_maintenance_job.before_metrics` when sampled — §6.2).

| Requirement | Detail                                                                                                                                      |
| ----------- | ------------------------------------------------------------------------------------------------------------------------------------------- |
| Deployment  | IRC (`iceberg-rest`) and the main Gravitino server share **one JVM**.                                                                       |
| Transport   | In-process callback / SPI only — **no** HTTP `POST …/events/iceberg-commit`, **no** Kafka.                                                  |
| Payload     | Normalized `table_identifier` (`catalog.schema.table`) and the committed `snapshot_id`. Policy selection uses Active policies + `onCommit`. |
| Enqueue     | Resolve attached `onCommit` types; drive ordered expand / ② chain (§5.7.1). Reject orphan-cleanup on commit.                                |
| Execute     | All nodes poll; one pick per instance; commit chain starts next type only after previous terminal (§5.5, §5.7.1).                           |
| Scheduling  | db-scheduler embedded in the TMS plugin; same JDBC DataSource as the entity store on MySQL / PostgreSQL.                                    |

Deployment:

1. Package the TMS plugin jars with the main Gravitino server and set
   `gravitino.server.rest.extensionPackages` to include the TMS Feature package (see §8.1).
2. Enable `iceberg-rest` in `gravitino.auxService.names` (same process as the main server).
3. Enable in-process commit events (`tableMaintenance.inProcess` — §8.2).
4. Enable the embedded scheduler (`gravitino.maintenance.scheduler.enabled` — §8.4).
5. Attach Govern maintenance policies (e.g. `system_iceberg_compaction`) to catalogs/schemas/tables
   via existing Policy APIs on the main server (**8090**).

### 5.2 Internal structure

| Part                             | Responsibility                                                                                                                         |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| `TableMaintenanceRESTFeature`    | Jersey 2 `Feature`; starts/stops db-scheduler; registers in-process callback and ops (§7); bootstraps `tms` user (§5.6).               |
| `TableMaintenanceScheduler`      | Wraps db-scheduler; registers **`tms-policy-expand`** and **`tms-spark`** handlers; reconciles ① on policy create/alter (§5.5).        |
| `IcebergCommitEventHandler`      | IRC commit callback; ordered commit chain for attached `onCommit` types (§5.4, §5.7.1).                                                |
| `PolicyExpandPipeline`           | Runs inside ① pick: resolve → list tables → expand gates (`minIntervalMs` / in-flight) → INSERT ungated **`tms-spark`**; retain ① (§5.5). |
| `MaintenanceSparkSubmitPipeline` | Runs inside ② pick: `ensureTableImported` → in-flight re-check → `Recommender` → `runJob` as `tms` → `table_maintenance_job` → **DELETE** ② (§5.5). |
| `GravitinoTableImportService`    | Lazy import into `table_meta` via `TableDispatcher.loadTable` (§5.5.4); backend-aware owner resolution.                                |
| `TmsPrincipalBootstrapListener`  | `EventListenerPlugin` on `CreateMetalakeEvent`; ensures metalake user `tms` + built-in role when authorization is enabled (§5.6).      |
| `TmsAuthConfigResolver`          | Resolves IRC auth + credential-vending Spark conf; loads secrets via SecretManager (§5.9).                                             |
| `TableMaintenanceJobStore`       | Read/write `table_maintenance_job` per-run rows; submit gates + Validation JSON (§6.2–§6.3).                                           |
| `IcebergTableLifecycleHook`      | In-process IRC **drop** hook: delete related ② rows + `table_maintenance_job`; ① unchanged unless policy removed (§6.3).               |
| Existing optimizer classes       | `Updater`, `Recommender`, providers, `JobSubmitter` — unchanged contracts for spark-submit path.                                       |
| db-scheduler `scheduled_tasks`   | ① long-lived expand + ② one-shot spark-submit. Not a substitute for `table_maintenance_job` or `job_run_meta`.                         |

### 5.3 User process

1. Operator enables the TMS REST plugin (`extensionPackages`), `iceberg-rest` **in the same JVM**,
   in-process commit events (§5.1.1 / §8.2), and the embedded scheduler (§8.4). If authorization is
   enabled, TMS bootstraps the metalake user `tms` and grants (§5.6).
2. Operator creates / enables a maintenance policy (including non-auth `jobOptions`) and associates
   it to catalogs / schemas / tables via metalake Policy APIs. On create/enable, TMS **INSERT**s
   **`tms-policy-expand`** ① for that `policy_id` (§5.5, §6.1). Example:

   ```bash
   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     -d '{
       "name": "iceberg_compaction_default",
       "comment": "Built-in Iceberg compaction policy",
       "policyType": "system_iceberg_compaction",
       "enabled": true,
       "content": {
         "rewriteOptions": {
           "target-file-size-bytes": "536870912"
         },
         "jobOptions": {
           "spark.executor.memory": "8g",
           "spark.sql.shuffle.partitions": "200"
         }
       }
     }' \
     http://localhost:8090/api/metalakes/test/policies

   curl -X POST -H "Accept: application/vnd.gravitino.v1+json" \
     -H "Content-Type: application/json" \
     -d '{"policiesToAdd": ["iceberg_compaction_default"]}' \
     http://localhost:8090/api/metalakes/test/objects/table/rest_catalog.db.t1/policies
   ```

3. Engines write through Gravitino Iceberg REST. On commit success, the **IRC hook** drives the
   **ordered** commit chain for attached `onCommit` types on that table (§5.4, §5.7.1).
4. Scheduler picks expand / `tms-spark` ② in chain order: gates → `runJob` → `INSERT` job row →
   **DELETE** ② → on terminal enqueue the next attached type. Only **one** node runs each instance.
5. Operators observe runs in the Gravitino **Jobs** UI / APIs (including Validation from
   `table_maintenance_job`). Automated Job `audit.creator` is **`tms`**. Manual ops APIs in §7 may
   bump ① or enqueue ② under test hooks.

### 5.4 Commit path — ordered expand for the committed table

```text
IRC commit succeeded (same JVM)
  └─ IRC post-commit hook (§5.1.1)
        │
        └─ in-process callback / SPI
              │
              v
        IcebergCommitEventHandler
              │
              ├─ resolve Active onCommit policies attached to this table
              │     (table / schema / catalog; nearest attachment wins per type)
              ├─ filter to commit-allowed types; drop orphan-cleanup (§5.7)
              ├─ sort remaining types by fixed order (§5.7.1)
              ├─ expand gates per policy (in-flight / minIntervalMs; §5.5.3)
              │     → drop gated policies from the run list
              └─ if any policy_ids remain: enqueue **one** commit ②
                    task_instance = {table_id}:{policy_id1}:{policy_id2}:…
                    (policy_ids = ungated set, already in §5.7.1 order)
                    task_data marks path=commit (disambiguates from crontab ②)
```

The IRC path **does not** call `runJob`. **`task_instance` carries only the policies that will
run** — if some are still inside `minIntervalMs` (or in-flight), they are **omitted**, so the key
has fewer `policy_id` segments. If every candidate is gated, **do not** insert ②.

Concurrent commits for the same table coalesce on the pending commit-chain ② for that `table_id`
(do not start a second chain while one is in flight / pending).

### 5.5 Execute path — expand pick + spark-submit pick

```text
db-scheduler (every TMS node polls; only one pick wins per due instance)

① tms-policy-expand due:
        ├─ pick ① (heartbeat while running)
        ├─ PolicyExpandPipeline:
        │     ├─ load policy_meta / content.schedule / attachments
        │     ├─ resolve target tables (commit hint → one table; crontab → list under attach)
        │     ├─ ensureTableImported for candidates (§5.5.4)
        │     ├─ for each candidate: apply expand gates (in-flight / minIntervalMs; §5.5.3)
        │     │     → gated candidates omitted from enqueue
        │     ├─ crontab path: INSERT one ② per ungated unit (execution_time = now)
        │     │     task_instance = {policy_id}:{table_id}
        │     │     task_data.path = crontab
        │     ├─ commit path: INSERT **one** ② for the table (execution_time = now)
        │     │     task_instance = {table_id}:{policy_id1}:{policy_id2}:…
        │     │       (ungated policy_ids only, §5.7.1 order; fewer ids if some gated)
        │     │     task_data.path = commit
        │     ├─ set ① next execution_time from crontab (if any); else wait for next commit bump
        │     └─ return (① NOT deleted)
        └─ dead JVM → missed heartbeats → ① runnable again

② tms-spark due:
        ├─ pick ②
        ├─ MaintenanceSparkSubmitPipeline:
        │     ├─ ensureTableImported (§5.5.4)
        │     ├─ if in-flight (finished_at IS NULL) → DELETE ②; return
        │     ├─ resolve target policy_id:
        │     │     crontab → the single policy_id in the key
        │     │     commit → **first** policy_id after table_id in the key
        │     ├─ Recommender → overlay jobOptions / SecretManager; runJob as tms
        │     ├─ INSERT table_maintenance_job (job_run_id + keys; finished_at NULL) (§6.2)
        │     ├─ DELETE this ② scheduled_tasks row
        │     ├─ commit chain: on job terminal, if more policy_ids remain in the former key,
        │     │     INSERT next ② with task_instance = {table_id}:{remaining_policy_ids…}
        │     └─ return (Spark async)
        └─ dead JVM mid-callback → missed heartbeats → ② may be picked again
           (idempotent gates + unique work-unit keys avoid double submit)
```

**Pick semantics:** Enqueueing into `scheduled_tasks` only sets **when** a row may run. Nodes do
**not** all execute the same instance.

**Why delete ② but keep ①:** ① is the durable “parse this policy on a schedule / commit” lease keyed
by `policy_id`. ② are ephemeral work units; once submitted (or skipped by gates), the scheduler row
must not run again — **DELETE** after the pick path finishes. Spark lifetime is tracked by
`table_maintenance_job` + `job_run_meta`, not by keeping ②.

**Why not wait for Spark inside the ② pick:** Same as before — short callbacks only; `finished_at IS
NULL` gates further submits for that `(table, policy)`.

#### 5.5.1 Where schedule state lives

| What                                             | Where it lives                          | Notes                                      |
| ------------------------------------------------ | --------------------------------------- | ------------------------------------------ |
| Policy expand due / pick / heartbeat             | `scheduled_tasks` ① `tms-policy-expand` | One long-lived instance per `policy_id`    |
| Spark submit unit due / pick                     | `scheduled_tasks` ② `tms-spark`         | Many rows; **DELETE** after pick path      |
| Policy type, thresholds, jobOptions, attachments | `policy_meta` / `policy_relation_meta`  | **What** to expand + non-auth job params   |
| Spark / Iceberg auth keys                        | SecretManager (URN)                     | TMS principal; not in policy               |
| Per-run Validation + submit gates                | `table_maintenance_job`                 | In-flight row + before/after JSON (§6.2)   |
| Job run snapshot                                 | `job_run_meta.runtime_job_template`     | What **this run** used; not default config |

#### 5.5.2 db-scheduler tasks

| Task name           | Instance key                                               | When created / due                                     | Action                                                |
| ------------------- | ---------------------------------------------------------- | ------------------------------------------------------ | ----------------------------------------------------- |
| `tms-policy-expand` | `{policy_id}` | Policy create/enable; due on crontab or commit bump | Expand → INSERT ②; retain ① |
| `tms-spark` (crontab) | `{policy_id}:{table_id}` or `{policy_id}:batch:{batch_id}` | Written by ① crontab expand; due immediately | Submit Spark → `table_maintenance_job` → **DELETE** ② |
| `tms-spark` (commit) | `{table_id}:{policy_id}[:{policy_id}…]` | Written by commit expand after gates; due immediately | Submit **first** policy_id → DELETE ②; on terminal enqueue remainder if any |

Instance keys use surrogate ids only (`policy_id`, `table_id`). Those ids are treated as **globally
unique** in the entity store, so `metalake_id` is not required in `task_instance` (metalake remains
available via `policy_meta` / `table_meta` when needed).

**Commit ② naming:** `{table_id}` first, then each **`policy_id` that will execute**, colon-separated,
already sorted by §5.7.1. Expand gates run **before** building the key — policies still inside
`minIntervalMs` (or in-flight) are **not** appended, so a three-policy table may enqueue
`{table_id}:{p_compaction}:{p_expiry}` if manifest-rewrite was gated out. Prefer `task_data.path =
commit|crontab` so a two-segment key is never ambiguous.

**Policy lifecycle → ①:** create/enable → INSERT ①; alter schedule → update ① `execution_time` /
`task_data`; disable/drop → DELETE ① and DELETE outstanding ② for that `policy_id`.

`table_id` comes from `table_meta` after lazy import (§5.5.4) before writing a ② row.

#### 5.5.3 H2 and test backends

| Entity-store backend            | Scheduler behavior                                                                                           |
| ------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| MySQL / PostgreSQL (production) | db-scheduler **enabled**; `scheduled_tasks` migrated with entity store                                       |
| H2 (unit / local tests)         | `gravitino.maintenance.scheduler.enabled = false`; tests call the pipeline **directly** after a fake enqueue |

**Expand gates** (inside **① `tms-policy-expand`**, per candidate work unit, **before** INSERT ②):

1. `ensureTableImported` (§5.5.4) — skip unit on import failure.
2. If an in-flight row exists for this `(metalake_id, table_id, policy_id)` (`finished_at IS NULL`):
   **skip** (do not INSERT ②).
3. Else if `MAX(finished_at)` for that key is still within the resolved `minIntervalMs` (table prop →
   global conf → code default; §8.3): **skip** (do not INSERT ②).

**Submit path** (inside **② `tms-spark`** pick):

1. `ensureTableImported` (§5.5.4) — must succeed when import is enabled.
2. Re-check in-flight (`finished_at IS NULL`): if found, **DELETE** this ② and return (race after
   expand wrote ②).
3. Policy trigger (`Recommender`).
4. Overlay nearest-policy `jobOptions` (§5.8) and SecretManager auth / credential-vending conf
   (§5.9); `runJob` as principal `tms` → `job_run_id`.
5. **Immediately** `INSERT` `table_maintenance_job` with `job_run_id`, `metalake_id`, `table_id`,
   `policy_id`, and `finished_at = NULL` (§6.2).
6. **DELETE** this ② `scheduled_tasks` row. Do **not** wait for Spark. `before_metrics` must be
   **sampled before** table mutation and **may** be persisted on INSERT or deferred to the terminal
   `UPDATE` with `after_metrics` + `finished_at`.

**`minIntervalMs` is judged on ①**, so cooldown does not create throwaway ② rows. ② keeps the
in-flight re-check only.

#### 5.5.4 Lazy Gravitino metadata import (`table_meta`)

TMS needs a stable `table_id` for `table_maintenance_job` and `scheduled_tasks` keys. That id lives
in Gravitino **`table_meta`**, not in the Iceberg catalog backend alone.

This is **Gravitino import**, not Iceberg REST `registerTable`. Reuse core
`TableDispatcher.loadTable(NameIdentifier)` — it loads from the catalog backend and, when the table is
not yet imported, writes a `TableEntity` row to **`table_meta`** (and imports the parent schema into
`schema_meta` first).

| Piece         | Detail                                                                                                                        |
| ------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| API           | `TableDispatcher.loadTable` on `metalake.catalog.schema.table` (same path IRC `importTableEntity` uses).                      |
| **Not**       | Iceberg REST `registerTable`, a new TMS table, or direct `INSERT` into `table_meta`.                                          |
| When (commit) | Optional: import committed table before bumping ①, or during ① expand when the commit hint is present.                        |
| When (expand) | When expanding catalog / schema attachments: **list tables from the catalog backend**, import each candidate before INSERT ②. |
| Idempotent    | If `table_meta` already has the table, `loadTable` is a no-op import.                                                         |
| Failure       | Log and **skip** schedule / submit for that table; do not write `table_maintenance_job` without a `table_id`.                 |

**Owner after import** (optional, separate from `table_meta` row):

1. Resolve owner from the **catalog backend** (for example Iceberg table property `owner`) via a
   backend-aware resolver — not from the `tms` principal.
2. If resolved and the user exists in metalake `user_meta`, call `OwnerDispatcher.setOwner`.
3. If missing or unknown user, leave Gravitino owner unset — **do not** default to `tms`.
4. If the table was already imported, do not overwrite an existing owner.

**Why not `schema_id` on `table_maintenance_job`:** rows are per **table** + policy. `table_id` already
identifies the schema through `table_meta` namespace (`metalake.catalog.schema`). `schema_id` would
only matter for schema-scoped TMS state, which this design does not have.

### 5.6 TMS execution principal (`tms`)

Automated **event** (commit enqueue) and **timed** (policy due) maintenance both submit Spark Jobs
as one built-in metalake user named **`tms`**. Policy authors are not the Job creator. This keeps
audit, privileges, and credentials on a service identity.

#### When the user is created

| Server mode                                                             | Bootstrap                                                                                                                     |
| ----------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| `gravitino.authorization.enable = false` (typical `simple` / none auth) | **Do not** insert `user_meta`. There is no RBAC identity. Jobs still set `audit.creator = tms` as a literal principal name.   |
| `gravitino.authorization.enable = true`                                 | TMS plugin start reconciles **all existing** metalakes; each new metalake is reconciled on `CreateMetalakeEvent`. Idempotent. |

The username is fixed (`tms`). Operators must not reuse it as an interactive login.

#### Bootstrap mechanism (authorization enabled)

Use an **`EventListenerPlugin`**, not an IRC hook and **not** a change to the metalake create API or
`MetalakeHookDispatcher`. Follow the same pattern as `BuiltInJobTemplateEventListener` in core.

| Piece            | Detail                                                                                                                                                                                               |
| ---------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Listener class   | `TmsPrincipalBootstrapListener` implements `EventListenerPlugin`.                                                                                                                                    |
| Registration     | `TableMaintenanceRESTFeature` registers it on `GravitinoEnv.eventListenerManager().addEventListener("tms-principal", …)` when the TMS plugin starts.                                                 |
| Plugin `start()` | List all in-use metalakes and reconcile `tms` user + `tms_maintenance` role + grants on each (covers existing metalakes and clusters where authorization was enabled after TMS was already running). |
| `onPostEvent`    | On `CreateMetalakeEvent`, reconcile the **new** metalake only.                                                                                                                                       |
| `mode()`         | `ASYNC_ISOLATED` — do not block metalake creation (same as built-in job-template listener).                                                                                                          |
| Reconcile        | Idempotent create-or-skip: user `tms` in `user_meta`, role `tms_maintenance`, metalake-level grants in §5.6 below.                                                                                   |

**Out of scope for bootstrap:** IRC commit/drop hooks (§5.1.1, §6.3), `MetalakeHookDispatcher`
(creator membership is already handled in core), and extending `POST /api/metalakes`.

#### Built-in role and privileges (authorization enabled)

TMS creates a built-in role (illustrative name `tms_maintenance`) granted to user `tms` on the
**metalake** so grants apply to all catalogs / schemas / tables under it.

Product requirement → Gravitino privilege mapping:

| Product wording  | Privilege(s) on metalake          | Why                                                                             |
| ---------------- | --------------------------------- | ------------------------------------------------------------------------------- |
| List catalogs    | `USE_CATALOG`                     | There is no separate `LIST_CATALOG`; listing/using catalogs uses `USE_CATALOG`. |
| List schemas     | `USE_SCHEMA`                      | Listing/using schemas.                                                          |
| List tables      | `USE_SCHEMA` + `PROBE_TABLE_LIKE` | Probe/list table-like objects without implying SELECT data.                     |
| Write all tables | `MODIFY_TABLE`                    | Iceberg rewrite / expire / orphan cleanup mutate table data and metadata.       |

Also grant so TMS can read policies and submit Jobs:

| Privilege          | Why                                                  |
| ------------------ | ---------------------------------------------------- |
| `VIEW_POLICY`      | Read attached maintenance policies and `jobOptions`. |
| `USE_JOB_TEMPLATE` | Use built-in maintenance job templates.              |
| `RUN_JOB`          | Submit Spark maintenance Jobs.                       |

Do **not** grant `MANAGE_USERS`, `CREATE_CATALOG`, or `MANAGE_GRANTS`.

If authorization is later enabled on a cluster that already had TMS running, the next plugin start
bootstraps missing `tms` users and grants.

### 5.7 Policy evaluate triggers (`onCommit` + `crontab`)

Automated TMS is **policy-gated**: without an Active, enabled maintenance policy attached to the
table (directly or via schema / catalog), commit events do nothing and no crontab evaluate is
scheduled. The policy also defines **when to evaluate** — not only thresholds and job parameters.

**Product model:** two automated evaluate triggers (combinable on the **same** policy — **one**
`policy_meta` row, one `content.schedule` object; **not** two policy records just because both
triggers are enabled):

| Trigger    | Meaning                                        | Typical use                                     |
| ---------- | ---------------------------------------------- | ----------------------------------------------- |
| `onCommit` | After IRC commit, drive ordered types (§5.7.1) | Write-heavy tables; types that allow `onCommit` |
| `crontab`  | Periodic expand on a crontab expression        | Timed maintenance; required for orphan-cleanup  |

Use **two policies** only when maintenance **types** differ, not because `onCommit` and `crontab`
are both set on one policy.

These are **not** the same as `minIntervalMs` (§8.3): `onCommit` / `crontab` decide **when
① expand runs**; `minIntervalMs` caps how soon expand may enqueue another ② after the last
finished job (`MAX(finished_at)` on `table_maintenance_job` for that `(table, policy)`).

**`orphan-cleanup` must not use `onCommit`.** Policy create/alter rejects `schedule.onCommit = true`
for orphan-cleanup (or ignores it). Orphan cleanup is **crontab-only** (or ops API).

#### 5.7.1 Commit-driven type order

On the **commit path** for a given table, TMS does **not** run every attached `onCommit` type in
parallel. It builds the set of Active, attached, commit-allowed types for that table, then runs
**only those types that are actually attached**, in this **fixed** order:

```text
1. compaction
2. manifest-rewrite
3. snapshot-expiry
```

Types that are **not** attached (or not `onCommit`) are **skipped** — the chain continues with the
next attached type in the list. Never reorder attached types. Never insert a type that is not
attached.

**Sequencing:** the commit ② key lists every remaining `policy_id`. Submit the **first**; on that
job’s terminal status, enqueue a shorter key with the remaining ids (or stop). Do not submit the
next policy while the previous is in flight.

**Crontab path** remains per-policy expand (① per `policy_id`) and does **not** require this
cross-type order unless product later unifies timed runs the same way.

**Storage:** trigger configuration lives in **`policy_version_info.content`**, not in `policy_meta`
columns and not in `scheduled_tasks`. The UI reads and writes the same JSON the TMS scheduler
uses.

Illustrative `content.schedule` (exact field names may be finalized with the Policy API / UI):

```json
{
  "minDataFileMse": 1000,
  "rewriteOptions": { "target-file-size-bytes": "536870912" },
  "schedule": {
    "onCommit": true,
    "crontab": "0 2 * * *",
    "timezone": "Asia/Shanghai"
  }
}
```

| Field               | UI                           | TMS runtime                                                           |
| ------------------- | ---------------------------- | --------------------------------------------------------------------- |
| `schedule.onCommit` | Toggle “run on commit”       | IRC drives ordered commit chain for attached types (§5.4, §5.7.1)     |
| `schedule.crontab`  | Crontab picker               | Sets / refreshes ① next `scheduled_tasks.execution_time` after expand |
| `schedule.timezone` | Timezone for crontab display | Parse crontab when computing next due time for ①                      |

**Rules:**

- At least one of `onCommit` or `crontab` should be set for automated maintenance; both may be set
  on the same policy (same ① `task_instance = {policy_id}`), except **orphan-cleanup** which allows
  **`crontab` only**.
- `crontab` only — common for orphan-cleanup and low-write tables.
- `onCommit` — commit path applies §5.7.1 order across attached compaction / manifest-rewrite /
  snapshot-expiry policies; each step may still skip submit when gates or `Recommender` fail.
- Manual ops APIs (§7) are a separate, operator-initiated path and do not use `schedule`.

**Where state lives:**

```text
policy_version_info.content.schedule  →  what the UI shows; source of truth for triggers
scheduled_tasks ① tms-policy-expand   →  next expand pick (crontab or commit bump); long-lived
scheduled_tasks ② tms-spark           →  one-shot spark units; DELETE after pick path
table_maintenance_job + minIntervalMs →  in-flight + expand cooldown before INSERT ② (not crontab)
```

**Crontab timeline (example: `0 2 * * *`):**

```text
[policy create/enable] → INSERT ① tms-policy-expand; execution_time = next 02:00
02:00                  → N nodes poll; ONE picks ①
                       → expand INSERT N × ② tms-spark (execution_time = now)
                       → retain ①; set next crontab due
~02:00+                → nodes pick each ② (parallel across units / nodes)
                       → gates → runJob → INSERT table_maintenance_job → DELETE that ②
                       → Spark async; terminal UPDATE metrics on table_maintenance_job
```

`execution_time = 02:00` on ① is the **expand due time**. Spark units ② become due when expand
writes them (typically immediately after). They are **not** also stamped for wall-clock 02:00:00,
and they are **removed from `scheduled_tasks` after pick**, not retained like ①.

### 5.8 Job template parameters on policy (`policy_meta`)

`job_run_meta.runtime_job_template` is a **run snapshot** (what that Job actually used). Default
parameters for automated maintenance are **not** stored there.

Non-auth job-template parameters (Spark resource and strategy options such as
`spark.executor.memory`, `spark.sql.shuffle.partitions`, Iceberg rewrite options) live in
**maintenance policy content**, which is already persisted in `policy_meta`. Compaction already
forwards `rewriteOptions` as `job.options.*`; other built-in types (`system_iceberg_snapshot_expiration`,
orphan cleanup, manifest rewrite) use the same `jobOptions` map in content.

**Precedence** (first hit wins), matching policy attachment:

```text
table-attached policy of that type
  > schema-attached policy of that type
  > catalog-attached policy of that type
  > job template base configuration
```

Attach one policy of the type at catalog for cluster defaults. Attach another policy of the **same
type** at a schema or table only where parameters differ. TMS does **not** add a second table keyed
by catalog / schema / table for these options — that would duplicate `policy_relation_meta` and
drift.

At submit:

```text
job template base configs
  overlay nearest attached policy jobOptions  (non-auth only)
  overlay SecretManager-resolved auth + credential-vending keys (§5.9)
  → JobSubmitter.runJob(..., jobConfig) as tms
```

Policy create / alter **rejects** credential-shaped keys in `jobOptions` / `rewriteOptions`
(same name rules as property masking: `password`, `secret`, `token`, `access-key`, and similar).
Those keys belong in SecretManager (§5.9), referenced by URN where needed.

### 5.9 Credential vending and IRC authentication (SecretManager)

Automated TMS Jobs talk to Gravitino **Iceberg REST** as Spark catalogs. Two orthogonal concerns:

1. **Credential vending** — how Spark gets temporary storage credentials for table data.
2. **IRC authentication** — how Spark authenticates to the Iceberg REST endpoint (`none` /
   `basic` / `oauth` / `kerberos`).

Sensitive values (passwords, client secrets, keytabs contents, static cloud keys if ever needed)
must **not** sit in `policy_meta`. They are stored and resolved through Gravitino
**SecretManager** / **SecretProvider** (URN references), the same pluggable path Jerry asked for
on server JDBC passwords. The solution is **opt-in**: when SecretManager is not configured,
non-secret auth properties and plaintext test values can still be used for local runs.

Keys below are written into the Job's `spark_conf` map (Iceberg REST catalog properties). Exact
prefixing (`spark.sql.catalog.<name>.…`) is applied by the job template / submitter from the
catalog name in `jobConfig`.

#### Credential vending

When maintenance Jobs should use **vended** storage credentials (recommended for production),
configure these on the **Spark / Job** side for every auth mode:

| Property                             | Required          | Description                                                                                                     |
| ------------------------------------ | ----------------- | --------------------------------------------------------------------------------------------------------------- |
| `header.X-Iceberg-Access-Delegation` | yes (for vending) | Must be `vended-credentials` so IRC returns temporary storage credentials on `loadTable`.                       |
| `uri`                                | yes               | Iceberg REST base URI (for example `http://host:9001/iceberg`).                                                 |
| `type`                               | yes               | `rest`.                                                                                                         |
| `warehouse` / `prefix`               | as needed         | Catalog warehouse or REST prefix when the deployment uses them.                                                 |
| `io-impl`                            | usually yes       | Iceberg FileIO implementation matching the warehouse scheme (for example `org.apache.iceberg.aws.s3.S3FileIO`). |

**Do not** put long-lived `s3.access-key-id` / `s3.secret-access-key` (or equivalent) into TMS Job
`spark_conf` when vending is enabled — the client must consume the vended block from IRC. Mixing
static keys with the vending header causes engines to ignore temporary credentials.

**Catalog-side** (operator configures on the Gravitino catalog / IRC, not in policy `jobOptions`):

| Property                                                           | Required              | Description                                                                                                                                        |
| ------------------------------------------------------------------ | --------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `data-access` or IRC `header.X-Iceberg-Access-Delegation` defaults | recommended           | Advertise `vended-credentials` to clients.                                                                                                         |
| `credential-providers`                                             | yes for token vending | For example `s3-token`, `oss-token`, `adls-token`, `gcs-token` (see [Credential vending](../docs/security/credential-vending.md)).                 |
| Provider-specific keys (`s3-role-arn`, `s3-region`, …)             | yes per provider      | Server-side keys used to mint temporary credentials. Secrets here also go through SecretManager / catalog secret bindings, not TMS policy content. |

Auth mode subsections below only add IRC **client authentication** properties on top of this
credential-vending baseline.

#### Auth: none

No IRC client authentication (typical `simple` / open IRC in lab).

| Property                                                 | Required         | SecretManager?   | Description                                                                               |
| -------------------------------------------------------- | ---------------- | ---------------- | ----------------------------------------------------------------------------------------- |
| *(omit `rest.auth.type`)* or `rest.auth.type` = `none`   | no               | no               | No username / password / token.                                                           |
| Credential-vending properties in §5.9 Credential vending | if using vending | no (header only) | Still set `header.X-Iceberg-Access-Delegation=vended-credentials` when storage is vended. |

No SecretManager entries are required for IRC auth itself.

#### Auth: basic

HTTP Basic against Gravitino IRC (local users / basic authenticator). TMS Jobs authenticate as
the maintenance principal (usually user `tms` when authorization is on).

| Property                                                 | Required         | SecretManager? | Description                                                                                                            |
| -------------------------------------------------------- | ---------------- | -------------- | ---------------------------------------------------------------------------------------------------------------------- |
| `rest.auth.type`                                         | yes              | no             | `basic`.                                                                                                               |
| `rest.auth.basic.username`                               | yes              | no             | For automated TMS runs: `tms` (or the configured TMS username).                                                        |
| `rest.auth.basic.password`                               | yes              | **yes**        | Password for that user. Store as a SecretManager secret; Job config holds a URN or resolved value at submit time only. |
| Credential-vending properties in §5.9 Credential vending | if using vending | —              | Same as parent section.                                                                                                |

#### Auth: oauth

OAuth2 client credentials or bearer token against Gravitino IRC.

| Property                                                 | Required                                  | SecretManager? | Description                                                                        |
| -------------------------------------------------------- | ----------------------------------------- | -------------- | ---------------------------------------------------------------------------------- |
| `rest.auth.type`                                         | yes                                       | no             | `oauth2`.                                                                          |
| `token`                                                  | one of token **or** client-credential set | **yes**        | Bearer access token path (short-lived; often minted at submit rather than stored). |
| `oauth2-server-uri`                                      | for client-credential path                | no             | Token endpoint URI.                                                                |
| `credential`                                             | for client-credential path                | **yes**        | OAuth client id and secret, typically `client_id:client_secret`.                   |
| `scope`                                                  | recommended                               | no             | OAuth scope (Iceberg may default to `catalog` if omitted).                         |
| Credential-vending properties in §5.9 Credential vending | if using vending                          | —              | Same as parent section.                                                            |

Prefer client-credential + SecretManager for `credential` so TMS does not embed long-lived bearer
tokens in policy or templates.

#### Auth: kerberos

Kerberos / SPNEGO style access when Gravitino authenticators include `kerberos`. Spark / Job
runtime must be able to obtain a TGT for the TMS service principal.

| Property                                                                     | Required                                                                | SecretManager?                         | Description                                                                                                           |
| ---------------------------------------------------------------------------- | ----------------------------------------------------------------------- | -------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `rest.auth.type`                                                             | yes (when Iceberg REST client exposes it) / Gravitino client `authType` | no                                     | `kerberos` (or deployment-specific key used by the maintenance Job’s Iceberg / Gravitino client).                     |
| Kerberos principal                                                           | yes                                                                     | no                                     | Service principal used by TMS Jobs (for example `tms/_HOST@REALM`).                                                   |
| Keytab path or keytab material                                               | yes                                                                     | **yes** for keytab bytes / path secret | Keytab must not live in `policy_meta`. Reference via SecretManager or a host path provisioned outside policy content. |
| `java.security.krb5.conf` / Hadoop `hadoop.security.authentication=kerberos` | as required by the cluster                                              | no                                     | Cluster Kerberos wiring for the Spark driver/executors.                                                               |
| Credential-vending properties in §5.9 Credential vending                     | if using vending                                                        | —                                      | Same as parent section. Kerberos authenticates to IRC; storage access still uses vended credentials when enabled.     |

#### Resolution at submit

```text
1. Load non-auth spark_conf / jobOptions from template ⊕ nearest policy
2. Select IRC auth mode (none | basic | oauth | kerberos) from TMS / metalake settings
3. Add credential-vending keys when enabled (header.X-Iceberg-Access-Delegation=…)
4. Resolve SecretManager URNs for password / oauth credential / keytab
5. Redact secrets in logs and in persisted runtime_job_template snapshots
6. runJob as principal tms
```

No `tms_credential` table. Switching from a local file SecretProvider to Vault / cloud KMS is a
provider configuration change only.

---

## 6. Multi-node coordination

On **multiple** Gravitino / TMS nodes, a commit may bump ① on the IRC node, but **every** node runs
db-scheduler. Writing `scheduled_tasks` does **not** cause all nodes to execute; when
`execution_time` is due, nodes **compete** and **only one** picks a given instance (CAS + heartbeat).

**Enqueue ①:** policy create/enable INSERT; commit / crontab bump `execution_time` (§5.4, §5.7).

**Enqueue ②:** written only by a successful ① expand pick (§5.5).

**Expand mutex:** pick on ① `tms-policy-expand` (§6.1).

**Spark-submit mutex:** pick on each ② `tms-spark` row; after the pick path, **DELETE** that row.
Additional `(table, policy)` protection: expand-time `minIntervalMs` + in-flight `table_maintenance_job` (§5.5.3, §6.2).

**Dead worker:** missed heartbeats unlock / requeue the instance. An already-submitted Spark job is
**not** cancelled; `table_maintenance_job.finished_at IS NULL` still blocks a second submit.

| Table                                  | Role                                                                    |
| -------------------------------------- | ----------------------------------------------------------------------- |
| `policy_meta` / `policy_relation_meta` | **What** to expand, `schedule` (§5.7), and non-auth `jobOptions` (§5.8) |
| `scheduled_tasks`                      | ① long-lived expand + ② one-shot spark-submit (DELETE after pick)       |
| `table_maintenance_job`                | Per-run Validation JSON + submit gates (§6.2)                           |
| SecretManager / SecretProvider         | IRC auth secrets for `tms` (§5.9); no `tms_credential` table            |
| `user_meta`                            | Built-in `tms` user when authorization is enabled (§5.6)                |

Auth material uses SecretManager. Job-template **parameters** stay on policy attachments.

### 6.1 Two-tier leases (`scheduled_tasks`)

```text
Policy create → INSERT ① (policy_id)
Commit / crontab → bump ① due
Node A / B / C poll
        │
        ├─ pick ① → INSERT many ② → retain ① → unpick
        ├─ pick ②ₐ → submit → DELETE ②ₐ
        ├─ pick ②ᵦ → submit → DELETE ②ᵦ   (may be another node)
        └─ picker dies → missed heartbeats → instance runnable again
```

**Not** “write `scheduled_tasks` ⇒ every node executes.” **Yes** “when due ⇒ every node may try;
exactly one claim runs that instance.”

Illustrative `scheduled_tasks` usage (db-scheduler owned; MySQL-shaped):

```sql
-- owned by db-scheduler; see upstream DDL
-- PRIMARY KEY (task_name, task_instance)
-- ① task_name = 'tms-policy-expand', task_instance = '{policy_id}'
-- ② crontab: task_name = 'tms-spark', task_instance = '{policy_id}:{table_id}' or batch
-- ② commit:  task_name = 'tms-spark', task_instance = '{table_id}:{policy_id}:{policy_id}:…'
--            (only ungated policy_ids; task_data.path = commit)
-- execution_time, picked, picked_by, last_heartbeat, version, task_data, ...
-- After ② pick path completes successfully (submit or gated skip): DELETE that row.
-- Commit remainder (if any) is a new INSERT with a shorter commit key.
-- ① is deleted only on policy disable / drop / replace (§5.5.2).
```

### 6.2 Per-run maintenance job table (`table_maintenance_job`)

**Table name:** `table_maintenance_job`

One row per Spark `job_run_id`. The same table serves Automate **Jobs → Validation** (before/after
JSON), expand-time `minIntervalMs`, and in-flight submit guards. `minIntervalMs` uses `finished_at`
on this row as the task end time, not `job_run_meta.job_finished_at`.

| Column           | Type                       | Notes                                                                        |
| ---------------- | -------------------------- | ---------------------------------------------------------------------------- |
| `job_run_id`     | `BIGINT UNSIGNED NOT NULL` | `job_run_meta.job_run_id`; primary key                                       |
| `metalake_id`    | `BIGINT UNSIGNED NOT NULL` | Metalake id                                                                  |
| `table_id`       | `BIGINT UNSIGNED NOT NULL` | `table_meta` surrogate id (after §5.5.4 import)                              |
| `policy_id`      | `BIGINT UNSIGNED NOT NULL` | `policy_meta.policy_id`                                                      |
| `before_metrics` | `MEDIUMTEXT NULL`          | JSON sampled **before** table mutation; may stay null until terminal persist |
| `after_metrics`  | `MEDIUMTEXT NULL`          | JSON after job terminal status; null while pending                           |
| `finished_at`    | `BIGINT UNSIGNED NULL`     | Task end time (epoch millis); **null = in-flight**; drives `minIntervalMs`   |

**Primary key:** (`job_run_id`). No `table_identifier` or `schema_id` column — `table_id` is
sufficient and aligns with `policy_relation_meta.metadata_object_id` for table policies.

No `evaluate_pending`, no `IDLE`/`RUNNING`, no application lease heartbeat — those are
`scheduled_tasks` concerns.

| Store                   | Role                                   | Suitable for Jobs Validation?  |
| ----------------------- | -------------------------------------- | ------------------------------ |
| `statistic_meta`        | Latest statistic per object + name     | No — current value only        |
| `table_metrics`         | Append-only time series by `metric_ts` | No — not keyed by `job_run_id` |
| `job_metrics`           | Job-scoped optimizer time series       | No — not table before/after UI |
| `table_maintenance_job` | Frozen before/after JSON per job       | **Yes**                        |

**Expand / submit gates** (for a given `(metalake_id, table_id, policy_id)`):

1. **Min interval (on ①):** `SELECT MAX(finished_at) …` vs resolved `minIntervalMs` (§8.3) — if
   within cooldown, **do not INSERT** ②.
2. **In-flight (on ① and re-checked on ②):** `SELECT 1 … WHERE finished_at IS NULL LIMIT 1` — if
   found, skip enqueue / skip submit.

**Lifecycle:**

1. After gates + `Recommender` pass: `runJob` → obtain `job_run_id`.
2. **Immediately** `INSERT` (`job_run_id`, `metalake_id`, `table_id`, `policy_id`) with
   `finished_at = NULL` (in-flight claim). `before_metrics` / `after_metrics` may be null here.
3. Spark runs asynchronously. **Sample** `before_metrics` before any table mutation (TMS submit
   path or Spark job start). Persisting that JSON may wait until step 4.
4. On job terminal status (job callback / listener): `UPDATE` `before_metrics` (if not written
   yet), `after_metrics`, and `finished_at`.

Do **not** sample “before” metrics after rewrite / expire has already mutated the table.

Illustrative MySQL DDL:

```sql
CREATE TABLE IF NOT EXISTS `table_maintenance_job` (
    `job_run_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'job run id',
    `metalake_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'metalake id',
    `table_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'table id from table_meta',
    `policy_id` BIGINT(20) UNSIGNED NOT NULL COMMENT 'policy id from policy_meta',
    `before_metrics` MEDIUMTEXT NULL COMMENT 'JSON sampled before mutation; may persist at terminal',
    `after_metrics` MEDIUMTEXT NULL COMMENT 'JSON object string after job; null while pending',
    `finished_at` BIGINT(20) UNSIGNED NULL COMMENT 'maintenance task end time (epoch millis); null = in-flight',
    PRIMARY KEY (`job_run_id`),
    KEY `idx_tmj_table_policy_finished` (`metalake_id`, `table_id`, `policy_id`, `finished_at`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
  COMMENT 'per-run TMS job record: Validation JSON + submit gates';
```

**JSON format:** UTF-8 object with flat metric keys. Values are numbers, strings, or booleans.
Optional top-level fields:

- `task_type` — e.g. `compaction`, `snapshot-expiry` (recommended in `before_metrics`)
- `passed` — overall validation result (recommended in `after_metrics` only)

Illustrative `before_metrics` / `after_metrics` by task type:

| Task type          | Example keys in JSON                                      | Auto pass/fail            |
| ------------------ | --------------------------------------------------------- | ------------------------- |
| `compaction`       | `num_files`, `avg_file_size_bytes`, `snapshot_count`      | Recommended (`passed`)    |
| `snapshot-expiry`  | `snapshot_count`, optional `deleted_manifest_files_count` | Recommended (`passed`)    |
| `manifest-rewrite` | `manifest_file_count` (if collected)                      | Optional / skip           |
| `orphan-cleanup`   | delete summary counts; skip Files before/after compare    | Skip table-metric compare |

Example `after_metrics`:

```json
{
  "task_type": "compaction",
  "num_files": 42,
  "avg_file_size_bytes": 134217728,
  "snapshot_count": 8,
  "passed": true
}
```

### 6.3 Table drop lifecycle

After a successful Iceberg **table drop**, IRC invokes an in-process `IcebergTableLifecycleHook`:

1. Resolve dropped `catalog.schema.table` → `table_id` when present in `table_meta`.
2. `DELETE` outstanding **`tms-spark`** ② rows for that table (crontab keys containing
   `{table_id}`; commit keys starting with `{table_id}:`).
3. `DELETE` from `table_maintenance_job` for `(metalake_id, table_id)`.
4. Do **not** delete **`tms-policy-expand`** ① (policy-scoped); remaining tables under the policy
   still expand on the next due. In-flight Spark jobs are **not** cancelled by this hook.

**Table rename is out of scope.**

---

## 7. Optimizer CLI replacement APIs

The commit path stays in-process and does **not** call these APIs. They replace the
`gravitino-optimizer` CLI (`--type ...`) for operators and scripts. The same plugin serves them on
the main webserver (**8090**).

- Prefix: `/api/maintenance/table/ops/...`. Not shown in the Compact-policy UI.
- Caller must have **WRITE** on each target table. Missing privilege → **403**.
- `--conf-path` stays server configuration. `update-statistics` and `append-metrics` send JSON Lines
  in the body. The API does not accept a server `--file-path`.
- `dryRun=true` returns the recommendation or job config and does not submit.

| CLI `--type`              | Method | Path                                           | Body or query                                                                                      |
| ------------------------- | ------ | ---------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `submit-strategy-jobs`    | `POST` | `/api/maintenance/table/ops/strategy-jobs`     | `identifiers`, `strategyName`, `dryRun`, `limit`                                                   |
| `submit-update-stats-job` | `POST` | `/api/maintenance/table/ops/update-stats-jobs` | `identifiers`, `dryRun`, `updateMode` (`stats` / `metrics` / `all`), `updaterOptions`, `sparkConf` |
| `update-statistics`       | `POST` | `/api/maintenance/table/ops/statistics`        | `calculatorName`, `identifiers`, `statisticsPayload` (JSON Lines)                                  |
| `append-metrics`          | `POST` | `/api/maintenance/table/ops/metrics`           | `calculatorName`, `identifiers`, `statisticsPayload` (JSON Lines)                                  |
| `monitor-metrics`         | `POST` | `/api/maintenance/table/ops/metrics/monitor`   | `identifiers`, `actionTime`, `rangeSeconds`, `partitionPath`                                       |
| `list-table-metrics`      | `GET`  | `/api/maintenance/table/ops/metrics/tables`    | `identifiers`, `partitionPath`                                                                     |
| `list-job-metrics`        | `GET`  | `/api/maintenance/table/ops/metrics/jobs`      | `identifiers`                                                                                      |

Each route calls the existing optimizer command implementation. The IRC hook and scheduler
pipelines (`PolicyExpandPipeline` / `MaintenanceSparkSubmitPipeline`) do not call this group.

---

## 8. Configuration

### 8.1 Enablement keys (`gravitino.conf`)

| Key                                       | Default | Description                                                                                               |
| ----------------------------------------- | ------- | --------------------------------------------------------------------------------------------------------- |
| `gravitino.server.rest.extensionPackages` | none    | Must include the TMS Feature package (illustrative: `org.apache.gravitino.maintenance.web.rest.feature`). |
| `gravitino.auxService.names`              | none    | Must include `iceberg-rest` when using IRC. TMS itself is **not** started this way.                       |

### 8.2 Iceberg REST → TMS in-process event keys

Illustrative keys (exact names may be finalized in implementation).

| Key (illustrative)                                  | Default | Description                                                                                |
| --------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------ |
| `gravitino.iceberg-rest.tableMaintenance.inProcess` | `false` | When `true`, IRC invokes the **main-server-registered event callback / SPI** after commit. |

Because IRC may use an isolated classloader, the callback must be registered by the TMS plugin (for
example on `GravitinoEnv`), not a direct cast to TMS implementation classes. IRC and the main
server must share **one JVM**.

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.auxService.names = iceberg-rest
gravitino.iceberg-rest.tableMaintenance.inProcess = true
gravitino.maintenance.scheduler.enabled = true
gravitino.maintenance.scheduler.threads = 4
```

HTTP `tableMaintenance.uri` / Kafka produce-consume keys are **not** in scope (Non-Goal #5).

### 8.3 Task types and minimum interval (global default + table override)

TMS recognizes four maintenance **task types** (aligned with product Compact policy surface):

| Task type          | Typical job / policy                             | Code default `minIntervalMs` |
| ------------------ | ------------------------------------------------ | ---------------------------- |
| `compaction`       | rewrite data files / `system_iceberg_compaction` | `3600000` (1 hour)           |
| `snapshot-expiry`  | expire snapshots                                 | `86400000` (1 day)           |
| `manifest-rewrite` | rewrite manifests                                | `86400000` (1 day)           |
| `orphan-cleanup`   | orphan file cleanup                              | `604800000` (7 days)         |

**Commit path:** only `compaction`, `manifest-rewrite`, and `snapshot-expiry` may use `onCommit`,
and only in that order among attached types (§5.7.1). **`orphan-cleanup` is crontab-only.**

**Snapshot expiry batching:** When a snapshot-expiry policy is attached at **catalog** or **schema**
scope, TMS may submit one Spark job that runs `expire_snapshots` for multiple tables under that
attachment. The default is **10 tables per job**; remaining tables are submitted in follow-on jobs.
Table-attached snapshot-expiry policies still use **one table per job** (commit-driven path).

**Resolution order** (first hit wins), same idea as Amoro table props + AMS defaults:

```text
1. Table property override (if set)
2. Global gravitino.conf key (if set)
3. Code default in the table above
```

**Global keys** (`gravitino.conf`, prefix `gravitino.maintenance.`):

| Key                                   | Description                                                                                  |
| ------------------------------------- | -------------------------------------------------------------------------------------------- |
| `task.compaction.minIntervalMs`       | Default min interval for compaction jobs                                                     |
| `task.snapshot-expiry.minIntervalMs`  | Default min interval for snapshot expiry                                                     |
| `task.snapshot-expiry.batchSize`      | Max tables per snapshot-expiry job when policy is catalog- or schema-attached (default `10`) |
| `task.manifest-rewrite.minIntervalMs` | Default min interval for manifest rewrite                                                    |
| `task.orphan-cleanup.minIntervalMs`   | Default min interval for orphan cleanup                                                      |

**Table-level overrides** (Iceberg / Gravitino table properties):

| Property                                     | Overrides                                    |
| -------------------------------------------- | -------------------------------------------- |
| `maintenance.compaction.minIntervalMs`       | Compaction min interval for this table       |
| `maintenance.snapshot-expiry.minIntervalMs`  | Snapshot expiry min interval for this table  |
| `maintenance.manifest-rewrite.minIntervalMs` | Manifest rewrite min interval for this table |
| `maintenance.orphan-cleanup.minIntervalMs`   | Orphan cleanup min interval for this table   |

On each **① expand**, for every candidate work unit the pipeline checks `MAX(finished_at)` on
`table_maintenance_job` for that `(table_id, policy_id)` against the resolved `minIntervalMs` for
that task type (§5.5.3). An in-flight row (`finished_at IS NULL`) also skips INSERT ② (and ②
re-checks before `runJob`). Policy content still owns **trigger thresholds** (e.g. MSE); the
interval only limits how often expand may enqueue another unit once ① runs (scheduler pick or ops
API).

Example table override:

```sql
ALTER TABLE rest_catalog.db.orders SET TBLPROPERTIES (
  'maintenance.compaction.minIntervalMs' = '7200000'
);
```

### 8.4 db-scheduler keys (`gravitino.conf`)

| Key                                                           | Default                                   | Description                                                        |
| ------------------------------------------------------------- | ----------------------------------------- | ------------------------------------------------------------------ |
| `gravitino.maintenance.scheduler.enabled`                     | `true` on MySQL/PostgreSQL; `false` on H2 | Enables embedded db-scheduler. Auto-false when entity store is H2. |
| `gravitino.maintenance.scheduler.threads`                     | `4`                                       | db-scheduler worker threads (size for concurrent short evaluates). |
| `gravitino.maintenance.scheduler.pollingIntervalMs`           | `10000`                                   | How often due tasks are polled.                                    |
| `gravitino.maintenance.scheduler.heartbeatIntervalMs`         | `60000`                                   | Heartbeat while a short evaluate callback is running.              |
| `gravitino.maintenance.scheduler.missedHeartbeatsLimit`       | `6`                                       | Missed heartbeats before a pick is considered dead.                |
| `gravitino.maintenance.scheduler.alwaysPersistTimestampInUTC` | `true` on MySQL                           | Required for MySQL timestamp handling per db-scheduler docs.       |

Dependency (illustrative, version pinned at implementation time):

```kotlin
implementation("com.github.kagkarlsson:db-scheduler:<version>")
```

`scheduled_tasks` DDL is added to the entity-store migration for MySQL and PostgreSQL backends only.

---

## 9. Work Plan and Checklist

### 9.1 Suggested Work Plan

This design delivers the in-process plugin, IRC commit **bump** of policy-expand ①, two-tier
`scheduled_tasks` (① retain / ② DELETE after pick), per-run `table_maintenance_job` records,
`tms` principal + SecretManager auth / credential vending, policy `jobOptions`, and expand →
spark-submit pipeline.

| Phase | Work item                                | Notes                                                                                                                          |
| ----- | ---------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------ |
| 1     | Load the in-process plugin               | `TableMaintenanceRESTFeature`; start/stop db-scheduler; `TmsPrincipalBootstrapListener`.                                       |
| 2     | Internal expand + spark-submit pipelines | `PolicyExpandPipeline` + `MaintenanceSparkSubmitPipeline`; unit tests.                                                         |
| 3     | db-scheduler two task kinds              | `tms-policy-expand` + `tms-spark` handlers; `scheduled_tasks` migration; heartbeats (§5.5).                                    |
| 4     | In-process IRC hook (bump ①)             | Bump expand (§5.4); policy create inserts ①; expand writes ② (§6.1).                                                           |
| 5     | Hardening                                | Service metrics, graceful shutdown, H2 path tests, user docs.                                                                  |
| 6     | Optimizer CLI replacement APIs           | Ops resources in §7. Same commands as `gravitino-optimizer`.                                                                   |
| 7     | TMS principal + SecretManager auth       | `tms` user/role (§5.6); credential vending + none/basic/oauth/kerberos (§5.9); policy `schedule` (§5.7) + `jobOptions` (§5.8). |

#### Phase 1 checklist

- [ ] Add `TableMaintenanceRESTFeature` (Jersey 2 `Feature`) that registers the in-process callback.
      Ops resources are added in phase 6. Do not add a health or commit-event resource.
- [ ] Register via `gravitino.server.rest.extensionPackages` (illustrative package
      `org.apache.gravitino.maintenance.web.rest.feature`).
- [ ] Package plugin jars with the main Gravitino server distribution (on the main server classpath).
- [ ] Document `extensionPackages` enablement.
- [ ] Add a unit test that the feature registers the callback and exposes no commit or health
      resource.
- [ ] `TmsPrincipalBootstrapListener` (`EventListenerPlugin`): skip `user_meta` when authorization is
      disabled; when enabled, register on `eventListenerManager`, reconcile all metalakes on plugin
      `start()`, and reconcile on `CreateMetalakeEvent` (`ASYNC_ISOLATED`) (§5.6).

#### Phase 2 checklist

- [ ] Implement `GravitinoTableImportService` (`TableDispatcher.loadTable` → `table_meta`; §5.5.4).
- [ ] Backend-aware owner resolution after import; never default owner to `tms`.
- [ ] Implement `PolicyExpandPipeline` (attachments → expand gates → INSERT ungated `tms-spark` ②; retain ①).
- [ ] Implement `MaintenanceSparkSubmitPipeline` calling `Recommender.submitForStrategyName`.
- [ ] Enforce per-policy gates on ② via `table_maintenance_job`: in-flight / min-interval; DELETE ②.
- [ ] Resolve Active attached policies via existing Policy / `StrategyProvider` (no new policy store).
- [ ] Add unit tests for skip / noop / submit outcomes.

#### Phase 3 checklist

- [ ] Add `TableMaintenanceScheduler` wrapping db-scheduler (`tms-policy-expand` + `tms-spark`).
- [ ] Add entity-store migration for `scheduled_tasks` (MySQL / PostgreSQL).
- [ ] Wire `DataSource` from the relational entity store; honor §8.4 heartbeat keys.
- [ ] On H2 backends: scheduler disabled; pipelines invoked directly in tests (§5.5.3).
- [ ] Integration test: two nodes; only one pick wins per instance; ② deleted after pick; dead JVM
      releases lease via heartbeat miss.

#### Phase 4 checklist

- [ ] Add `IcebergCommitEventHandler` and main-server-registered in-process callback / SPI (§5.1.1 /
      §8.2).
- [ ] Add EntityStore migration for **`table_maintenance_job`** (§6.2).
- [ ] On policy create/enable: INSERT **`tms-policy-expand`** ① (`policy_id`) (§5.5.2).
- [ ] IRC post-commit builds ordered commit chain (§5.7.1); rejects orphan-cleanup `onCommit`; does
      **not** `runJob` on the commit thread.
- [ ] Tests: attached subset runs in fixed order; missing types skipped; chain waits for prior
      terminal; concurrent commits coalesce.
- [ ] Wire IRC post-commit hook to the in-process callback (`tableMaintenance.inProcess`).
- [ ] Wire IRC **drop** hook to delete outstanding ② + `table_maintenance_job` rows (§6.3).
- [ ] On `runJob` success: `INSERT` `table_maintenance_job`; **DELETE** ②; on terminal: `UPDATE`
      metrics + `finished_at` (§6.2).
- [ ] Integration tests: commit ② key is `{table_id}:{policy_ids…}` after minInterval filter;
      shorter key when some gated; remainder enqueued on terminal; crontab keys unchanged; drop
      cleans ② + job rows.
- [ ] Do **not** ship HTTP `…/events/iceberg-commit` or Kafka ingress.

#### Phase 5 checklist

- [ ] Service metrics: enqueue counts, pick counts, submit counts, dead-execution recoveries, failures.
- [ ] Graceful shutdown: stop db-scheduler before entity store closes.
- [ ] Update user-facing TMS / optimizer docs for scheduler enqueue + per-run `table_maintenance_job`.

#### Phase 6 checklist

- [ ] Add the seven ops resources in §7, each calling the existing optimizer command implementation.
- [ ] Require WRITE on each target table; missing privilege returns 403.
- [ ] `dryRun=true` returns the recommendation or job config and does not submit.
- [ ] Accept statistics and metrics JSON Lines in the body. Do not accept a server `--file-path`.
- [ ] Tests: each CLI `--type` maps to one route; the commit path does not call these routes.

#### Phase 7 checklist

- [ ] Wire **SecretManager** for TMS IRC auth secrets (no `tms_credential` table). Opt-in: plaintext
      / missing secrets still work when SecretManager is not enabled.
- [ ] Policy create/alter rejects credential-shaped keys in `jobOptions` / `rewriteOptions`.
- [ ] Submit path: nearest policy `jobOptions` (table > schema > catalog) overlay template base;
      add credential-vending header when enabled; resolve auth for **none / basic / oauth /
      kerberos** (§5.9); `runJob` as `tms`.
- [ ] Tests: authorization off skips user insert; authorization on grants `USE_CATALOG`,
      `USE_SCHEMA`, `PROBE_TABLE_LIKE`, `MODIFY_TABLE`, `VIEW_POLICY`, `USE_JOB_TEMPLATE`, `RUN_JOB`.
- [ ] Tests: table attachment overrides catalog `jobOptions`; auth secrets never persist in
      `policy_meta`; `runtime_job_template` redacts passwords / tokens / keytabs.

### 9.2 Review Checklist

| Area         | Checklist                                                                                                                                  |
| ------------ | ------------------------------------------------------------------------------------------------------------------------------------------ |
| Deployment   | Enabled via `gravitino.server.rest.extensionPackages`; IRC colocated in the same JVM. Ops APIs on **8090** (§7).                           |
| Classpath    | TMS plugin on main server classpath; **not** an aux isolated listener.                                                                     |
| Triggers     | Commit: ordered attached types (§5.7.1); crontab: per-policy ①; orphan-cleanup crontab-only.                                               |
| Scheduling   | **db-scheduler**: ① long-lived per `policy_id`; ② one-shot DELETE after pick (§5.5, §6.1, §8.4).                                           |
| Job records  | `table_maintenance_job` one row per `job_run_id` (Validation JSON + submit gates).                                                         |
| Import       | Lazy `table_meta` import via `TableDispatcher.loadTable` (§5.5.4); not Iceberg `registerTable`.                                            |
| Ops API      | Seven routes replace `gravitino-optimizer` (§7). Not used by the commit path. Table WRITE required.                                        |
| Pipeline     | ① expand → INSERT ②; ② pick → gates → `Recommender` → Jobs → DELETE ②.                                                                     |
| Validation   | `table_maintenance_job` (`before_metrics` / `after_metrics` JSON + `finished_at`) (§6.2).                                                  |
| Drop         | Drop hook deletes outstanding ② + `table_maintenance_job` rows (§6.3). Rename out of scope.                                                |
| Multi-node   | Due rows: N poll, one pick; ② deleted after pick; Spark double-submit blocked by in-flight row.                                            |
| Policy       | Reuses metalake Policy APIs + `policy_meta`; create inserts ①; `schedule` (§5.7) + `jobOptions` (§5.8).                                    |
| Job boundary | Expand due ≠ Spark wall-clock; Spark in Jobs; Validation on `table_maintenance_job` (§6.2).                                                |
| Principal    | Automated Jobs run as `tms`; `TmsPrincipalBootstrapListener` on plugin start + `CreateMetalakeEvent` when authorization is enabled (§5.6). |
| Credentials  | Auth via SecretManager (none/basic/oauth/kerberos) + credential vending (§5.9); not policy / not `tms_credential`.                         |
| Security     | Ops APIs require table WRITE. TMS role is least-privilege for list + table write + run job. No commit-event or health endpoint.            |
| License      | db-scheduler is **Apache 2.0**; no LGPL scheduling dependency.                                                                             |

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
10. [db-scheduler](https://github.com/kagkarlsson/db-scheduler) — embedded persistent scheduler (Apache 2.0)
