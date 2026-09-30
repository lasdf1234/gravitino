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

# Gravitino 表维护服务（TMS）设计

> 本文为 `design-docs/table-maintenance-service-design.md` 的中文译本。

## 1. 背景

Gravitino 中的表维护服务（Table Maintenance Service，TMS）目前仍是 alpha 能力。
`maintenance/optimizer` 包已包含执行核心：统计采集、规则评估、策略推荐、指标查询与作业提交
（`Updater`、`Recommender`、各类 Provider、`JobSubmitter`）。

该核心尚未作为长期运行的 Gravitino 服务承载。若无服务端组件：

1. Iceberg 提交后缺少稳定的**服务端 commit 事件**。否则各引擎需各自挂接 TMS 监听器，或运维在外部调度维护。
2. 配置、审计与服务级指标难以集中，执行仍是临时、进程本地的。
3. 每次运行各自创建运行时与 Provider 实例，而非共享服务生命周期。
4. 需要 Spark 维护工作时，已提交工作会返回由 Gravitino 作业框架拥有的 `jobId`。该作业状态边界应保持不变。
5. 多节点部署下缺少用于过期 claim 回收的**嵌入式调度器**。

本设计将 TMS 做成主服务 **8090** 上的 **REST 插件**（与 IdP 相同，通过
`gravitino.server.rest.extensionPackages`），以便同机部署的 IRC 在 commit 后**触发**按策略的 expand。
**db-scheduler** 在 `scheduled_tasks` 上持有**两类**任务租约：① **policy-expand**（把 policy 解析成 Spark 工作单元；长期保留）与
② **spark-submit**（一次性；pick 完成后删除）。`table_maintenance_job` 为每次 Spark 运行存一行（Validation JSON + submit 门控）。
optimizer 执行核心在 ② pick 后以进程内方式运行。

---

## 2. 目标

1. **主服务进程内插件**：通过 `gravitino.server.rest.extensionPackages` 加载 Table Maintenance（Jersey 2
   `Feature`，与 IdP 相同），IRC 回调注册在主 JVM。commit 路径**不使用 HTTP**。替代 optimizer CLI
   的运维调用见 **§7** ops API。
2. **IRC 进程内 commit 事件**：经 IRC 成功提交 Iceberg 后，TMS 通过**主服务注册的进程内回调 / SPI**
   收到 commit 事件（IRC 与主服务同 JVM，见 **§5.1.1**）。处理程序解析 Active 策略，并 **bump** 该 `policy_id`
   对应的长期 **`tms-policy-expand`** 行（`execution_time = now`）——不在 commit 线程 expand / submit（§5.4）。
3. **`scheduled_tasks` 上两层 db-scheduler 任务**：
   - **`tms-policy-expand`（①）**：维护策略**创建 / 启用**时写入；实例键含 **`policy_id`**。解析 policy，
     **INSERT** 零或多条 **`tms-spark`**。expand 行**保留**（crontab / 下次到期）；仅在 `policy_meta`
     变更 / 禁用 / 删除时删除或改写（§5.5、§6.1）。
   - **`tms-spark`（②）**：由 expand 写出的一次性 Spark submit 单元。某节点 **pick** 并完成 submit 管线
     （门控 → `runJob` → `table_maintenance_job` INSERT）后，**删除该 `tms-spark` 行**。Spark 在作业框架异步继续。
   所有节点轮询；每个到期实例 **N 抢 1 中**。**不要**持有 pick 直到 Spark 完成。
4. **复用现有 optimizer 执行核心**：调度器任务处理程序以**进程内方法**调用 `maintenance/optimizer` 中已有的
   `Updater` / `Recommender` / 作业提交路径，而非第二套逻辑。
5. **作业框架兼容**：Spark 维护工作继续使用 Gravitino 作业框架。TMS 在 `table_maintenance_job` 中记录每次运行，
   但不拥有作业状态。Automate Jobs **Validation** 的前后指标在同一行（§6.2）。
6. **复用 Govern Policy**：维护策略仍在现有 `policy_meta` 与 metalake Policy API
   （create / alter / enable / disable / associate）。TMS **不**另建策略库或
   `/api/maintenance/table/policies` CRUD。
7. **多节点安全执行**：db-scheduler pick + heartbeat 是每个 `scheduled_tasks` 实例的互斥锁（① 与 ② 皆然）
   （§5.5、§6）。同一 `(table, policy)` 的并发 Spark 提交还由在途 `table_maintenance_job` 行门控（§6.2）。
8. **按运行维护作业表**：Gravitino 为每个 Spark `job_run_id` 持久化一行 `table_maintenance_job`
   （Validation JSON + `finished_at`）。调度任务的入队 / 回收在 `scheduled_tasks`（上述两类）。
9. **Commit / crontab 驱动 expand；expand 驱动 spark 行**：IRC commit 与 crontab 只让 **① 到期**（§5.4、§5.7）。
   Expand 写入 **②**。`minIntervalMs` 是 ② 路径上的**运行时门控**（§5.5、§6.2）。
10. **专用 TMS 执行主体**：所有自动化维护（commit 后事件入队与定时策略到期）均以内置 metalake 用户 **`tms`**
    提交 Jobs，而非创建策略的运维人员（§5.6）。
11. **策略 evaluate 触发方式**：自动化维护在 `policy_version_info.content.schedule` 中配置
    `onCommit` 和/或 `crontab`（§5.7）。同一策略一行即可同时启用两种触发；拆成两条 policy 仅因维护**类型**不同。
12. **策略上的作业模板参数**：非认证的 Spark / job-template 参数存放在维护策略内容
    （`jobOptions` / 现有 `rewriteOptions`）。在 catalog / schema / table 挂载同类型策略；
    **最近挂载优先**（table > schema > catalog）（§5.8）。
13. **通过 SecretManager 管理认证凭据**：运行 Spark Jobs 所需的密码、令牌、访问密钥**不**存放在
    `policy_meta` 或 TMS 自有凭据表。敏感值通过 Gravitino **SecretManager**（URN / provider）引用，
    与服务器配置的可插拔 secret 方案一致。可选启用：未启用 secret 方案时，明文 / 缺失 secret 仍可用（§5.9）。

---

## 3. 非目标

1. **独立维护守护进程**：无单独进程或 `gravitino-iceberg-rest-server.sh` 式入口。
2. **专用 aux HTTP 监听**：无 `GravitinoAuxiliaryService`、无隔离的 `gravitino.maintenance.classpath`、
   无 TMS 专用端口（如 **9301**）。TMS 不是 `iceberg-rest` / `lance-rest` 那种专用监听器。
3. **重写 Provider SPI**：不替换事件管线使用的 `StatisticsUpdater`、`StatisticsCalculator`、
   `StatisticsProvider`、`StrategyProvider`、`TableMetadataProvider`、`JobSubmitter` 契约。
4. **引擎侧 commit 上报路径**：绕过 Gravitino Iceberg REST 的引擎不在事件驱动路径范围内。
5. **Commit 路径 HTTP 或 Kafka**：无 `POST …/events/iceberg-commit`、无健康检查资源、无 Kafka 生产/消费路径。
   commit 处理**仅进程内**（§5.1.1）。替代 optimizer CLI 的 API 在 **§7**，不是 commit 入口。
   远程 IRC / 跨 JVM 投递不在范围（如需可后续跟进）。
6. **持有 db-scheduler pick 直到 Spark 完成**：expand 与 spark-submit 回调必须在入队 / submit（或 skip）后返回。
   长 Spark 生命周期由 `table_maintenance_job` 与作业框架门控；② 行在 submit 后**删除**，不靠长期 `picked`。
7. **自动化 TMS 的按人用户模板**：手动 Automate Jobs UI 日后可存按用户默认值；自动化事件/定时运行始终使用
   **`tms`** 主体与策略 `jobOptions`（§5.6–§5.8）。

## 4. 方案调研

### 4.1 选项 A：仅保持进程本地执行

继续在临时本地进程中跑全部 optimizer 工作，无 TMS 服务端点。

**优点：** 无新监听；实现量最小。

**缺点：** 无 IRC 事件目标；无集中的事件驱动 expand → spark-submit 服务。

**决策：** 否决。

### 4.2 选项 B：主服务进程内插件（选定）

通过 `gravitino.server.rest.extensionPackages` 注册 Jersey 2 `Feature`，在主服务进程内运行。
commit 路径**不使用 HTTP**。每次 Iceberg commit 后，**同机** IRC 钩子调用主服务注册的**进程内**回调，
**bump** 长期 **`tms-policy-expand`** 任务。**所有** TMS 节点运行 db-scheduler；一个节点 pick 每个到期实例
（expand ① 或 spark-submit ②）。

**优点：** 无额外进程或端口；commit 路径无远程事件跳转；通过调度器 pick 在副本间分发工作；
复用 Policy + Jobs；匹配插件打包；租约 heartbeat 内置于 db-scheduler。

**决策：** **选定**。

### 4.3 选项 C：独立长期运行的 Table Maintenance Service 进程

**优点：** 完整 JVM 隔离。

**缺点：** 额外可部署物；重复主 webserver 插件已覆盖的服务器生命周期模式。

**决策：** 否决。优先主服务进程内插件。

### 4.4 选项 E：专用 aux Jetty 监听（:9301）

实现 `GravitinoAuxiliaryService`，`shortName() = "maintenance"`，暴露专用 Jetty 监听（默认 **9301**），
TMS 不在主 8090 JAX-RS 应用上。

**优点：** 类路径隔离类似 `iceberg-rest` / `lance-rest`。

**缺点：** 额外端口与 aux 启用；与已通过 `extensionPackages` 扩展 **8090** 的插件模式不一致。

**决策：** 否决。优先选项 B。

### 4.5 两层调度任务 + 按运行作业记录（db-scheduler — 选定）

TMS 需要在 `scheduled_tasks` 上对**两类**工作做集群安全调度，外加持久的按运行作业记录：

1. **Policy-expand** — 把维护策略解析成 Spark 工作单元（每个 `policy_id` 长期一行）。
2. **Spark-submit** — 真正 `runJob` 的一次性单元（多行；**pick 后删除**）。
3. **`table_maintenance_job`** — 在途 Spark / `minIntervalMs` / Validation 跨过 spark-submit pick 仍有效。

这与 Spark 作业执行（已由 Gravitino Jobs 拥有）**不是**同一问题。单独的臃肿 evaluate 状态表（带 `evaluate_pending` +
`IDLE`/`RUNNING` + 应用 heartbeat）重复了 db-scheduler 已为短任务提供的能力。持有 `picked` 直到
Spark 完成会把调度器线程绑在长作业上，已否决（非目标 #6）。

#### 行业与项目内替代方案

| ----------------------- | --------------------------------------------------------------- | ---------------------------------- | ---------------------------------------------------- | --------------------------------------------------- |
| ----------------------- | --------------------------------------------------------------- | ---------------------------------- | ---------------------------------------------------- | --------------------------------------------------- |
|                         | **[db-scheduler](https://github.com/kagkarlsson/db-scheduler)** | [JobRunr](https://www.jobrunr.io/) | [ShedLock](https://github.com/lukas-krecan/ShedLock) | [Quartz](https://www.quartz-scheduler.org/) JDBC 集群 |
| 许可证                     | Apache 2.0                                                      | LGPL v3（+ 商业）                      | Apache 2.0                                           | Apache 2.0                                          |
| 嵌入主服务                   | 是                                                               | 是                                  | 是                                                    | 是                                                   |
| 集群 CAS / 单飞             | 是（`scheduled_tasks` 乐观锁 / `SKIP LOCKED`）                        | 是                                  | 仅锁                                                   | 是（`QRTZ_*` 行锁）                                      |
| 短租约 heartbeat           | 是（`last_heartbeat`）                                             | 是                                  | 不适用                                                  | 是                                                   |
| 适合按 (table,policy) 到期任务 | 是                                                               | 是                                  | 否（仅锁）                                                | 是（更重）                                               |
| H2 单元测试路径               | 降级：禁用调度器；测试中直接跑管线（§5.5.3）                                       | H2 支持更好                            | 是                                                    | 仅测试用 RAMJobStore                                    |
| 额外运维组件                  | 无                                                               | 可选 dashboard 服务                    | 无                                                    | 无                                                   |
| 决策                      | **选定**                                                          | 否决 — 许可证 + 与 Gravitino Jobs 重叠     | 否决 — 非调度器                                            | 否决 — 约 11 张表，过重                                     |

#### 为何 db-scheduler + 精简作业表

1. **Apache License 2.0** — 适合 ASF 项目；JobRunr 为 LGPL v3。
2. **可嵌入且轻量** — 租约用一张 `scheduled_tasks` 表；随 `TableMaintenanceRESTFeature` 启停。
3. **内置 heartbeat** — 短 evaluate 回调运行期间，db-scheduler 刷新 `last_heartbeat`；死 JVM 释放 pick，
   无需在 TMS 状态表上手写租约列。
4. **职责清晰** — `tms-policy-expand` = 解析 policy → 入队 spark 单元；`tms-spark` = submit 后 **DELETE**；
   `table_maintenance_job` = Validation + submit 门控；Gravitino Jobs 跑 Spark。
5. **无臃肿 TMS 状态机** — 无 `evaluate_pending` / `IDLE`/`RUNNING` 与 `picked` 的双份实现。

**决策：** **选定** — db-scheduler 两类任务名；`table_maintenance_job` 用于按运行记录与 submit 门控。

### 4.6 自动化 Job 配置存放位置

#### 4.6.1 作业模板参数（`jobOptions`）

自动化维护运行的非认证 Spark / job-template 参数（例如 executor 内存、shuffle 分区数、Iceberg
`rewriteOptions`）。

|     | 按 catalog / schema / table 的额外表 | 策略内容 `jobOptions`（选定）                             |
| --- | ------------------------------- | ------------------------------------------------- |
| 优点  | 显式                              | 复用 `policy_meta` / `policy_relation_meta`；最近挂载已定义 |
| 缺点  | 与策略并行的挂载 + 优先级 + UI；易漂移         | 认证材料见 §4.6.2，不放在此处                                |
| 决策  | 否决                              | **选定**（§5.8）                                      |

#### 4.6.2 认证信息

TMS Jobs 的 IRC 客户端认证与 credential-vending 所需 secret（密码、OAuth client credential、keytab 等）。

|     | 策略 `jobOptions` 中的认证键 | 专用 `tms_credential` 表               | SecretManager + URN（选定）                          |
| --- | --------------------- | ----------------------------------- | ------------------------------------------------ |
| 优点  | 与 Spark 选项同一张 map     | 显式 TMS 覆盖层                          | 可插拔 provider（file / Vault / KMS）；可选；与服务器 conf 共享 |
| 缺点  | 策略广泛可读；secret 泄漏      | SecretManager 旁第二 keystore；无 KMS 路径 | 需为 TMS 主体 bootstrap secret                       |
| 决策  | 否决                    | 否决                                  | **选定**（§5.9）                                     |

---

## 5. 方案

### 5.1 架构

TMS 在 `scheduled_tasks`（db-scheduler）上使用**两类**行。写入一行**不等于**每个节点都执行：
**每个**节点轮询；每个到期实例 **N 抢 1 中**。

| 种类                      | `task_name`（示例）     | 何时创建                         | 实例键                                                                | pick 之后                                                           |
| ----------------------- | ------------------- | ---------------------------- | ------------------------------------------------------------------ | ----------------------------------------------------------------- |
| ① Policy → Spark expand | `tms-policy-expand` | 策略**创建 / 启用**（变更时 reconcile） | `{policy_id}`                                                      | 解析 policy → **INSERT** 零或多条 ②；**保留** ①（设下次 `execution_time`）      |
| ② Spark submit 单元       | `tms-spark`         | 由 ① expand 时写入               | `{policy_id}:{table_id}` 或 `{policy_id}:batch:{batch_id}`（每工作单元唯一） | 门控 → `runJob` → `INSERT table_maintenance_job` → **DELETE** 该 ② 行 |

**生命周期概要：**

```text
创建 / 启用维护策略（policy_id = P）
  → INSERT scheduled_tasks ① tms-policy-expand / {policy_id}
     execution_time = 下次 crontab（若仅 onCommit，可先放到远未来直至首次 commit）

onCommit / crontab 到期
  → bump ① execution_time = now（commit）或保留 crontab 到期时间

① 被 pick（一个节点）
  → 读 policy_meta + 挂载；列出 / 过滤表（§5.5.4）
  → 对每个应运行的工作单元：INSERT ② tms-spark（execution_time = now）
  → 按 crontab 设 ① 下次 execution_time（或等待下次 commit bump）
  → **不**删除 ①（除非策略禁用 / 删除 / 替换）

② 被 pick（每行一个节点；多条 ② 可在不同节点并行）
  → 门控（在途 / minInterval）→ Recommender → 以 tms 的 runJob
  → INSERT table_maintenance_job（job_run_id + 键；finished_at NULL）
  → DELETE 该 ② scheduled_tasks 行
  → 返回；Spark 在作业框架继续
  → 终态：UPDATE table_maintenance_job 的 before/after/finished_at
```

```text
Spark / Flink / Trino
        │  Iceberg REST commit
        v
Gravitino IRC (:9001)
        │
        └─ post-commit → IcebergCommitEventHandler (§5.4)
                ├─ 解析该表 Active（onCommit）策略
                └─ bump tms-policy-expand ①
                      task_instance = {policy_id}
                      execution_time = now
                      （可选 task_data：已提交 table_id）

Node A / Node B / Node C  — 各轮询 db-scheduler（§5.5）；N 抢 1 中
        │
        ├─ pick 到期的 ① tms-policy-expand
        │     ├─ expand policy → INSERT N × ② tms-spark（execution_time = now）
        │     └─ 保留 ①；设下次 crontab 到期（若有）；释放 pick
        │
        ├─ pick 到期的 ② tms-spark
        │     ├─ ensureTableImported（§5.5.4）
        │     ├─ 门控：在途？/ minIntervalMs？→ 否则 DELETE ② 并返回
        │     ├─ Recommender → 以 `tms` 的 runJob → job_run_id
        │     ├─ INSERT table_maintenance_job（§6.2）
        │     ├─ DELETE 该 ② scheduled_tasks 行
        │     └─ 返回（**不**等待 Spark）
        v
                 Gravitino Job 框架（异步）
                 改表前采样 before_metrics；终态 UPDATE 指标（§6.2）

        ┌──────────────────────────────────────────────────────────────┐
        │  scheduled_tasks                                              │
        │  • ① tms-policy-expand — 按 policy_id 长期保留                │
        │  • ② tms-spark — 一次性；pick 路径成功后 DELETE               │
        │  • 到期 ⇒ 所有节点轮询；每个实例仅一个 pick                   │
        └──────────────────────────────────────────────────────────────┘
```

| 表                                      | 角色                                                                                            |
| -------------------------------------- | --------------------------------------------------------------------------------------------- |
| `policy_meta` / `policy_relation_meta` | **expand 什么**、`schedule` 触发（§5.7）与非认证 `jobOptions`（§5.8）                                      |
| `scheduled_tasks`                      | ① expand + ② spark-submit 租约（§5.5、§6.1）                                                       |
| `table_maintenance_job`                | 按运行 Validation JSON + `finished_at`；submit 门控（§6.2）                                           |
| SecretManager / SecretProvider         | 经 URN 的 TMS Spark / Iceberg **认证**材料（§5.9）；非 TMS 自有表                                          |
| `user_meta`                            | 启用授权时的内置 metalake 用户 `tms`（§5.6）                                                              |
| `job_run_meta`                         | Spark 作业运行**记录**（状态 + `runtime_job_template` 快照）；非默认配置                                        |

#### 5.1.1 进程内 commit 事件

Commit 事件**仅进程内**投递。Iceberg commit 成功后，**IRC post-commit 钩子**调用**主服务注册的回调 / SPI**
（例如在 `GravitinoEnv` 上）。该回调解析带 `onCommit` 的 Active 策略，并 **bump** 每条匹配的
**`tms-policy-expand`** 行（`execution_time = now`，可选在 `task_data` 中带已提交表提示）。
**不**在 commit 路径插入 `tms-spark`，也**不**调用 `runJob`。expand 在**任意**节点 pick 到期 ① 时运行（§5.5）。
TMS **不**为每次 commit 持久化单独行；已提交的 `snapshot_id` 仍在 Iceberg 表元数据中（可选在采样时记入 `table_maintenance_job.before_metrics` — §6.2）。

| 要求          | 详情                                                                                                                                            |
| ----------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| 部署          | IRC（`iceberg-rest`）与主 Gravitino 服务器共享**一个 JVM**。                                                                                              |
| 传输          | 仅进程内回调 / SPI —— **无** HTTP `POST …/events/iceberg-commit`，**无** Kafka。                                                                        |
| 载荷          | 规范化 `table_identifier`（`catalog.schema.table`）与已提交 `snapshot_id`。策略选择使用 Active 策略 + `onCommit`。                                               |
| 入队          | Upsert / bump `tms-policy-expand`，`task_instance = {policy_id}`，`execution_time = now`。                                                       |
| 执行          | 所有节点轮询；**一个** pick 跑 expand ① → 写 ②；随后 pick 跑 ② submit 再 **DELETE** ②（§5.5）。                                                                  |
| 调度          | db-scheduler 嵌入 TMS 插件；与 MySQL / PostgreSQL entity store 使用同一 JDBC DataSource。                                                                |

### 5.2 内部结构

| 组件                                  | 职责                                                                                                                                           |
| ----------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `TableMaintenanceRESTFeature`       | Jersey 2 `Feature`；启停 db-scheduler；注册进程内回调与 ops（§7）；bootstrap `tms` 用户（§5.6）。                                                                |
| `TableMaintenanceScheduler`         | 封装 db-scheduler；注册 **`tms-policy-expand`** 与 **`tms-spark`** 处理程序；策略创建/变更时 reconcile ①（§5.5）。                                                |
| `IcebergCommitEventHandler`         | IRC commit 回调；为 Active `onCommit` 策略 bump **`tms-policy-expand`**（§5.4）。                                                                     |
| `PolicyExpandPipeline`              | 在 ① pick 内运行：解析挂载 → 列表 → INSERT **`tms-spark`**；保留 ①（§5.5）。                                                                                  |
| `MaintenanceSparkSubmitPipeline`    | 在 ② pick 内运行：`ensureTableImported` → 门控 → `Recommender` → 以 `tms` 的 `runJob` → `table_maintenance_job` → **DELETE** ②（§5.5）。                 |
| `GravitinoTableImportService`       | 经 `TableDispatcher.loadTable` 懒 import 进 `table_meta`（§5.5.4）；按 backend 解析 owner。                                                            |
| `TmsPrincipalBootstrapListener`     | 监听 `CreateMetalakeEvent` 的 `EventListenerPlugin`；启用授权时确保 metalake 用户 `tms` + 内置角色（§5.6）。                                                     |
| `TmsAuthConfigResolver`             | 解析 IRC 认证 + credential-vending Spark conf；经 SecretManager 加载 secret（§5.9）。                                                                   |
| `TableMaintenanceJobStore`          | 读写 `table_maintenance_job` 按运行行；submit 门控 + Validation JSON（§6.2–§6.3）。                                                                      |
| `IcebergTableLifecycleHook`         | 进程内 IRC **drop** 钩子：删除相关 ② + `table_maintenance_job`；① 除非策略移除否则不动（§6.3）。                                                                     |
| 现有 optimizer 类                      | `Updater`、`Recommender`、providers、`JobSubmitter` — spark-submit 路径契约不变。                                                                      |
| db-scheduler `scheduled_tasks`      | ① 长期 expand + ② 一次性 spark-submit。不能替代 `table_maintenance_job` 或 `job_run_meta`。                                                              |

### 5.3 用户流程

1. 运维启用 TMS REST 插件（`extensionPackages`）、**同 JVM** 的 `iceberg-rest`、进程内 commit 事件（§5.1.1 / §8.2）
   与嵌入式调度器（§8.4）。若启用授权，TMS bootstrap metalake 用户 `tms` 并授予权限（§5.6）。
2. 运维创建 / 启用维护策略（含非认证 `jobOptions`），并通过 metalake Policy API 挂载到 catalog / schema / table。创建/启用时 TMS **INSERT**
   **`tms-policy-expand`** ①（该 `policy_id`）（§5.5、§6.1）。例如：

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

3. 引擎经 Gravitino Iceberg REST 写入。commit 成功后，**IRC 钩子**为 Active `onCommit` 策略 bump ①（§5.4）。
4. 某节点 pick ① → expand 写入 N × **`tms-spark`** ②。各节点再 pick 每条 ②：门控 → `runJob` → `INSERT` 作业行 → **DELETE** 该 ②（§5.5、§6）。
   每个实例**只有一个**节点执行。
5. 运维在 Gravitino **Jobs** UI / API 观察运行（含来自 `table_maintenance_job` 的 Validation）。自动化 Job 的 `audit.creator` 为 **`tms`**。
   §7 ops API 可 bump ① 或在测试钩子下入队 ②。

### 5.4 Commit 路径 — 仅 bump expand

```text
IRC commit 成功（同 JVM）
  └─ IRC post-commit 钩子 (§5.1.1)
        │
        └─ 进程内回调 / SPI
              │
              v
        IcebergCommitEventHandler
              │
              ├─ 解析该表 Active onCommit 策略
              └─ 对每个 policy_id P：
                    bump tms-policy-expand ①
                    task_instance = {metalake_id}:{P}
                    execution_time = now
                    task_data 可选：已提交 catalog.schema.table / table_id
```

IRC 路径**不**插入 `tms-spark`，**不**跑 expand / submit。并发 commit 通过 bump **同一** ① 实例合并。

### 5.5 执行路径 — expand pick + spark-submit pick

```text
db-scheduler（每个 TMS 节点轮询；每个到期实例仅一个 pick 胜出）

① tms-policy-expand 到期：
        ├─ pick ①（执行期间 heartbeat）
        ├─ PolicyExpandPipeline：
        │     ├─ 加载 policy_meta / content.schedule / 挂载
        │     ├─ 解析目标表（commit 提示 → 单表；crontab → 挂载范围内列表）
        │     ├─ 对候选 ensureTableImported（§5.5.4）
        │     ├─ 对每个工作单元（表或 snapshot-expiry batch）：INSERT tms-spark ②
        │     │     task_instance = {policy_id}:{table_id}
        │     │     execution_time = now
        │     ├─ 按 crontab 设 ① 下次 execution_time（若有）；否则等下次 commit bump
        │     └─ 返回（① **不**删除）
        └─ 死 JVM → 错过 heartbeat → ① 可再次运行

② tms-spark 到期：
        ├─ pick ②
        ├─ MaintenanceSparkSubmitPipeline：
        │     ├─ ensureTableImported（§5.5.4）
        │     ├─ 若在途（finished_at IS NULL）→ DELETE ②；返回
        │     ├─ 若仍在 minIntervalMs 内 → DELETE ②；返回
        │     ├─ Recommender → 叠加 jobOptions / SecretManager；以 tms 的 runJob
        │     ├─ INSERT table_maintenance_job（job_run_id + 键；finished_at NULL）（§6.2）
        │     ├─ DELETE 该 ② scheduled_tasks 行
        │     └─ 返回（Spark 异步）
        └─ 回调中途死 JVM → 错过 heartbeat → ② 可能再次被 pick
           （幂等门控 + 唯一工作单元键避免双 submit）
```

**Pick 语义：** 写入 `scheduled_tasks` 只设定**何时**可跑。节点**不会**都执行同一实例。

**为何删 ②、留 ①：** ① 是按 `policy_id` 的持久「按日程 / commit 解析该 policy」租约。② 是临时工作单元；
一旦已 submit（或被门控跳过），调度行不得再跑 —— pick 路径结束后 **DELETE**。Spark 生命周期由
`table_maintenance_job` + `job_run_meta` 跟踪，不靠保留 ②。

**为何不在 ② pick 内等待 Spark：** 短回调；`finished_at IS NULL` 阻止同一 `(table, policy)` 再次 submit。

#### 5.5.1 调度状态存放位置

| 内容                                            | 存放位置                                    | 说明                                               |
| --------------------------------------------- | --------------------------------------- | ------------------------------------------------ |
| Policy expand 到期 / pick / heartbeat           | `scheduled_tasks` ① `tms-policy-expand` | 每个 `policy_id` 一条长期实例                            |
| Spark submit 单元到期 / pick                      | `scheduled_tasks` ② `tms-spark`         | 多行；pick 路径后 **DELETE**                           |
| 策略类型、阈值、jobOptions、挂载                         | `policy_meta` / `policy_relation_meta`  | **expand 什么** + 非认证 job 参数                       |
| Spark / Iceberg 认证键                           | SecretManager（URN）                      | TMS 主体；不在策略中                                     |
| 按运行 Validation + submit 门控                    | `table_maintenance_job`                 | 在途行 + 前后 JSON（§6.2）                              |
| Job 运行快照                                      | `job_run_meta.runtime_job_template`     | **该次运行**所用；非默认配置                                 |

#### 5.5.2 db-scheduler 任务

| 任务名                   | 实例键                                                       | 何时创建 / 到期                                    | 动作                                                    |
| --------------------- | --------------------------------------------------------- | -------------------------------------------- | ----------------------------------------------------- |
| `tms-policy-expand`   | `{policy_id}`                                             | 策略创建/启用；crontab 或 commit bump 到期             | Expand → INSERT ②；保留 ①                                |
| `tms-spark`           | `{policy_id}:{table_id}` 或 `{policy_id}:batch:{batch_id}` | 由 ① 写入；通常立即到期（`execution_time = now`）        | Submit Spark → `table_maintenance_job` → **DELETE** ② |

实例键只使用代理 id（`policy_id`、`table_id`）。在 entity store 中将其视为**全局唯一**，因此 `task_instance` **不需要** `metalake_id`（需要 metalake 时从 `policy_meta` / `table_meta` 反查即可）。

**策略生命周期 → ①：** 创建/启用 → INSERT ①；变更 schedule → 更新 ① `execution_time` / `task_data`；禁用/删除 → DELETE ① 并 DELETE 该 `policy_id` 下未完成的 ②。

写 ② 行前须经 §5.5.4 懒 import 得到 `table_id`。

#### 5.5.3 H2 与测试后端

| Entity-store 后端                 | 调度器行为                                                                                                       |
| ------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| MySQL / PostgreSQL（生产）          | db-scheduler **启用**；`scheduled_tasks` 与 entity store 一起迁移                                                   |
| H2（单元 / 本地测试）                   | `gravitino.maintenance.scheduler.enabled = false`；测试在假入队后**直接**调用管线                                         |

**门控顺序**（在 **② `tms-spark`** pick 内）：

1. `ensureTableImported`（§5.5.4）——启用 import 时须在 submit 门控前成功。
2. 若该 `(metalake_id, table_id, policy_id)` 存在在途行（`finished_at IS NULL`）：**DELETE** 该 ② 行并返回。
3. 否则若该键的 `MAX(finished_at)` 仍在解析的 `minIntervalMs` 内（表属性 → 全局 conf → 代码默认；§8.3）：**DELETE** 该 ② 行并返回。
4. 策略触发（`Recommender`）。
5. 叠加最近策略 `jobOptions`（§5.8）与 SecretManager 认证 / credential-vending conf（§5.9）；以主体 `tms` 的 `runJob` → `job_run_id`。
6. **立刻** `INSERT` `table_maintenance_job`（`job_run_id` + 三键，`finished_at = NULL`）（§6.2）。
7. **DELETE** 该 ② `scheduled_tasks` 行。**不**等待 Spark。`before_metrics` 须在**改表前采样**，可在 INSERT 或终态与 `after` + `finished_at` 一并落库。

**① expand pick** 不用 minInterval / 在途门控来跳过写 ②；这些门控在 ② 上执行。import 失败的表可不建 ②。

#### 5.5.4 懒加载 Gravitino 元数据 import（`table_meta`）

TMS 需要稳定的 `table_id` 作为 `table_maintenance_job` 与 `scheduled_tasks` 的键。该 id 在 Gravitino
**`table_meta`** 中，而非仅在 Iceberg catalog backend。

这是 **Gravitino import**，不是 Iceberg REST 的 `registerTable`。复用 core
`TableDispatcher.loadTable(NameIdentifier)` —— 从 catalog backend 加载，若尚未 import 则写入
**`table_meta`**（并先 import 父 schema 到 `schema_meta`）。

| 组件         | 说明                                                                                               |
| ---------- | ------------------------------------------------------------------------------------------------ |
| API        | 对 `metalake.catalog.schema.table` 调用 `TableDispatcher.loadTable`（与 IRC `importTableEntity` 同路径）。 |
| **不是**     | Iceberg REST `registerTable`、新建 TMS 表、或直接 `INSERT` `table_meta`。                                 |
| 时机（commit） | 可选：bump ① 前 import 已提交表，或在 ① expand 时按 commit 提示 import。                                         |
| 时机（expand） | 展开 catalog / schema 挂载时：**从 catalog backend list tables**，对每张候选表 import 后再 INSERT ②。             |
| 幂等         | `table_meta` 已有该表时，`loadTable` 不再重复 import。                                                      |
| 失败         | 记录日志并**跳过**该表的调度 / submit；无 `table_id` 时不写 `table_maintenance_job`。                              |

**Import 后的 owner**（可选，与 `table_meta` 行写入分开）：

1. 经 **catalog backend** 解析 owner（如 Iceberg 表属性 `owner`），不用 `tms` 主体。
2. 若解析到且用户在 metalake `user_meta` 中存在，调用 `OwnerDispatcher.setOwner`。
3. 缺失或未知用户则**不**设 Gravitino owner —— **不要**默认 `tms`。
4. 表已 import 则不覆盖已有 owner。

**为何 `table_maintenance_job` 不存 `schema_id`：** 行粒度是 **表 + 策略**。`table_id` 经 `table_meta`
命名空间（`metalake.catalog.schema`）已隐含 schema。仅 schema 粒度的 TMS 状态本设计不存在，故不需要 `schema_id`。

### 5.6 TMS 执行主体（`tms`）

自动化**事件**（commit 入队）与**定时**（策略到期）维护均以内置 metalake 用户 **`tms`** 提交 Spark Jobs。
策略作者不是 Job 创建者。这使审计、权限与凭据落在服务身份上。

#### 用户何时创建

| 服务器模式                                                           | Bootstrap                                                                                   |
| --------------------------------------------------------------- | ------------------------------------------------------------------------------------------- |
| `gravitino.authorization.enable = false`（典型 `simple` / none 认证） | **不**插入 `user_meta`。无 RBAC 身份。Jobs 仍将 `audit.creator = tms` 作为字面主体名。                        |
| `gravitino.authorization.enable = true`                         | TMS 插件启动时 reconcile **所有已有** metalake；每个新建 metalake 在 `CreateMetalakeEvent` 时 reconcile。幂等。 |

用户名固定（`tms`）。运维不得将其用作交互式登录。

#### Bootstrap 机制（启用授权时）

使用 **`EventListenerPlugin`**，**不是** IRC hook，也**不**改 metalake 创建 API 或 `MetalakeHookDispatcher`。
与 core 中 `BuiltInJobTemplateEventListener` 同一模式。

| 组件            | 说明                                                                                                                         |
| ------------- | -------------------------------------------------------------------------------------------------------------------------- |
| Listener 类    | `TmsPrincipalBootstrapListener` 实现 `EventListenerPlugin`。                                                                  |
| 注册            | `TableMaintenanceRESTFeature` 在 TMS 插件启动时通过 `GravitinoEnv.eventListenerManager().addEventListener("tms-principal", …)` 注册。 |
| 插件 `start()`  | 列出所有在用 metalake，逐个 reconcile `tms` 用户 + `tms_maintenance` 角色 + 授权（覆盖已有 metalake，以及集群后来才开授权的情况）。                            |
| `onPostEvent` | 收到 `CreateMetalakeEvent` 时，仅 reconcile **新建** metalake。                                                                    |
| `mode()`      | `ASYNC_ISOLATED` — 不阻塞 metalake 创建（与内置 job-template listener 相同）。                                                          |
| Reconcile     | 幂等 create-or-skip：在 `user_meta` 创建用户 `tms`、角色 `tms_maintenance`、以及下文 §5.6 的 metalake 级授权。                                  |

**Bootstrap 不在此范围：** IRC commit/drop hook（§5.1.1、§6.3）、`MetalakeHookDispatcher`（创建者成员关系已由 core 处理）、扩展 `POST /api/metalakes`。

#### 内置角色与权限（启用授权时）

TMS 创建内置角色（示例名 `tms_maintenance`），在 **metalake** 上授予用户 `tms`，使权限适用于其下所有 catalog / schema / table。

产品需求 → Gravitino 权限映射：

| 产品表述            | metalake 上的权限                     | 原因                                                   |
| --------------- | --------------------------------- | ---------------------------------------------------- |
| List catalogs   | `USE_CATALOG`                     | 无单独 `LIST_CATALOG`；列出/使用 catalog 用 `USE_CATALOG`。    |
| List schemas    | `USE_SCHEMA`                      | 列出/使用 schema。                                        |
| List tables     | `USE_SCHEMA` + `PROBE_TABLE_LIKE` | 探测/列出表类对象，不隐含 SELECT 数据。                             |
| 写所有表            | `MODIFY_TABLE`                    | Iceberg rewrite / expire / orphan cleanup 修改表数据与元数据。 |

另授予以便 TMS 读策略并提交 Jobs：

| 权限                 | 原因                       |
| ------------------ | ------------------------ |
| `VIEW_POLICY`      | 读取挂载的维护策略与 `jobOptions`。 |
| `USE_JOB_TEMPLATE` | 使用内置维护 job 模板。           |
| `RUN_JOB`          | 提交 Spark 维护 Jobs。        |

**不要**授予 `MANAGE_USERS`、`CREATE_CATALOG` 或 `MANAGE_GRANTS`。

若集群已运行 TMS 后 later 启用授权，下次插件启动会 bootstrap 缺失的 `tms` 用户与授权。

### 5.7 策略 evaluate 触发（`onCommit` + `crontab`）

自动化 TMS **以 policy 为门控**：表上（直接或经 schema / catalog 继承）若无 Active、已启用的维护策略，
commit 事件不会触发维护，也不会调度 crontab evaluate。Policy 除定义阈值与作业参数外，还定义**何时 evaluate**。

**产品模型：** 两种自动化 evaluate 触发（可组合在**同一**策略上 —— **一条** `policy_meta` 记录、
一个 `content.schedule` 对象；**不是**因为同时启用 onCommit 与 crontab 就要建两条 policy）：

| 触发方式         | 含义                                 | 典型场景                                  |
| ------------ | ---------------------------------- | ------------------------------------- |
| `onCommit`   | IRC commit 后入队 evaluate（立即）        | 写多读多的表 compaction                     |
| `crontab`    | 按 crontab 表达式周期性 evaluate          | snapshot-expiry、orphan cleanup、低写入表   |

仅当维护**类型**不同（如 compaction + snapshot-expiry）时才用**两条** policy，不是因为两种触发方式。

这与 `minIntervalMs`（§8.3）**不同**：`onCommit` / `crontab` 决定**何时 evaluate**；`minIntervalMs` 限制上次成功
submit 后多久可再次 submit（查该 `(table_id, policy_id)` 的 `MAX(finished_at)`）。

**存储：** 触发配置在 **`policy_version_info.content`**，不在 `policy_meta` 列、也不在 `scheduled_tasks`。
前端展示与编辑的 JSON 与 TMS 调度器读取的相同。

示例 `content.schedule`（字段名可与 Policy API / UI 最终对齐）：

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

| 字段                    | 前端                           | TMS 运行时                                                                 |
| --------------------- | ---------------------------- | ----------------------------------------------------------------------- |
| `schedule.onCommit`   | 「commit 后运行」开关               | IRC 钩子 bump `tms-policy-expand` ①（`execution_time = now`）（§5.4）         |
| `schedule.crontab`    | Crontab 选择器                  | expand 后刷新 ① 的下次 `scheduled_tasks.execution_time`                       |
| `schedule.timezone`   | crontab 时区                   | 解析 crontab 计算下次到期时间                                                     |

**规则：**

- 自动化维护应至少设置 `onCommit` 或 `crontab` 之一；可同时设置（同一 `task_instance = {metalake_id}:{table_id}:{policy_id}`）。
- 仅 `crontab` — 常见于少 commit 的 snapshot-expiry / orphan-cleanup。
- 仅 `onCommit` — 常见于流式 compaction；未达 `Recommender` 阈值仍 skip submit。
- 运维 API（§7）为人工触发，不使用 `schedule`。

**状态存放位置：**

```text
policy_version_info.content.schedule  →  UI 展示的触发配置；触发的真实来源
scheduled_tasks ① tms-policy-expand   →  下次 expand（crontab 或 commit bump）；长期保留
scheduled_tasks ② tms-spark           →  一次性 spark 单元；pick 路径后 DELETE
table_maintenance_job + minIntervalMs →  在途 job + submit 冷却（不是 crontab）
```

**Crontab 时间线（示例：`0 2 * * *`）：**

```text
[策略创建/启用] → INSERT ① tms-policy-expand；execution_time = 下次 02:00
02:00         → N 节点轮询；一个 pick 赢下 ①
              → expand INSERT N × ② tms-spark（execution_time = now）
              → 保留 ①；设下次 crontab 到期
~02:00+       → 各节点 pick 每条 ②（可跨单元 / 节点并行）
              → 门控 → runJob → INSERT table_maintenance_job → DELETE 该 ②
              → Spark 异步；终态 UPDATE table_maintenance_job 指标
```

① 上的 `execution_time = 02:00` 是 **expand 到期时间**。② 在 expand 写出后通常立刻到期。
② **不是**再盖一个墙钟 02:00:00，且 pick 后从 `scheduled_tasks` **删除**，不像 ① 那样保留。

### 5.8 策略上的作业模板参数（`policy_meta`）

`job_run_meta.runtime_job_template` 是**运行快照**（该 Job 实际所用）。自动化维护的默认参数**不**存那里。

非认证 job-template 参数（Spark 资源与策略选项，如 `spark.executor.memory`、`spark.sql.shuffle.partitions`、
Iceberg rewrite 选项）存放在**维护策略内容**，已持久化在 `policy_meta`。Compaction 已将 `rewriteOptions` 转发为
`job.options.*`；其他内置类型（`system_iceberg_snapshot_expiration`、orphan cleanup、manifest rewrite）在 content 中使用同一 `jobOptions` map。

**优先级**（先命中者生效），与策略挂载一致：

```text
表上挂载的该类型策略
  > schema 上挂载的该类型策略
  > catalog 上挂载的该类型策略
  > job 模板基线配置
```

在 catalog 挂载一种类型的策略作为集群默认。仅在参数不同的 schema 或 table 再挂载**同类型**另一策略。TMS **不**再建按
catalog / schema / table 的第二张表 —— 会重复 `policy_relation_meta` 并漂移。

Submit 时：

```text
job 模板基线 configs
  叠加最近挂载策略 jobOptions（仅非认证）
  叠加 SecretManager 解析的认证 + credential-vending 键（§5.9）
  → 以 tms 的 JobSubmitter.runJob(..., jobConfig)
```

策略 create / alter **拒绝** `jobOptions` / `rewriteOptions` 中形似凭据的键（与属性掩码同名规则：`password`、`secret`、`token`、`access-key` 等）。
这些键属于 SecretManager（§5.9），需要时以 URN 引用。

### 5.9 Credential vending 与 IRC 认证（SecretManager）

自动化 TMS Jobs 以 Spark catalog 形式访问 Gravitino **Iceberg REST**。两个正交关注点：

1. **Credential vending** — Spark 如何获取表数据的临时存储凭据。
2. **IRC 认证** — Spark 如何认证 Iceberg REST 端点（`none` / `basic` / `oauth` / `kerberos`）。

敏感值（密码、client secret、keytab 内容、若需要的静态云密钥）**不得**放在 `policy_meta`。通过 Gravitino
**SecretManager** / **SecretProvider**（URN 引用）存储与解析，与 Jerry 对服务器 JDBC 密码要求的可插拔路径相同。
方案**可选**：未配置 SecretManager 时，非 secret 认证属性与明文测试值仍可用于本地运行。

下列键写入 Job 的 `spark_conf` map（Iceberg REST catalog 属性）。精确前缀（`spark.sql.catalog.<name>.…`）由
job 模板 / submitter 根据 `jobConfig` 中的 catalog 名应用。

#### Credential vending

当维护 Jobs 应使用**代发**存储凭据（生产推荐）时，在 **Spark / Job** 侧为每种认证模式配置：

| 属性                                   | 必填           | 说明                                                                            |
| ------------------------------------ | ------------ | ----------------------------------------------------------------------------- |
| `header.X-Iceberg-Access-Delegation` | 是（vending 时） | 须为 `vended-credentials`，以便 IRC 在 `loadTable` 时返回临时存储凭据。                       |
| `uri`                                | 是            | Iceberg REST 基 URI（如 `http://host:9001/iceberg`）。                             |
| `type`                               | 是            | `rest`。                                                                       |
| `warehouse` / `prefix`               | 按需           | 部署使用时的 catalog warehouse 或 REST prefix。                                       |
| `io-impl`                            | 通常必填         | 匹配 warehouse scheme 的 Iceberg FileIO（如 `org.apache.iceberg.aws.s3.S3FileIO`）。 |

启用 vending 时**不要**把长期 `s3.access-key-id` / `s3.secret-access-key`（或等价项）放进 TMS Job `spark_conf` ——
客户端须消费 IRC 代发的块。静态密钥与 vending 头混用会导致引擎忽略临时凭据。

**Catalog 侧**（运维在 Gravitino catalog / IRC 配置，不在策略 `jobOptions`）：

| 属性                                                          | 必填                | 说明                                                                                                                 |
| ----------------------------------------------------------- | ----------------- | ------------------------------------------------------------------------------------------------------------------ |
| `data-access` 或 IRC `header.X-Iceberg-Access-Delegation` 默认 | 推荐                | 向客户端宣告 `vended-credentials`。                                                                                       |
| `credential-providers`                                      | token vending 时必填 | 如 `s3-token`、`oss-token`、`adls-token`、`gcs-token`（见 [Credential vending](../docs/security/credential-vending.md)）。 |
| 各 provider 键（`s3-role-arn`、`s3-region`、…）                   | 按 provider 必填     | 服务端用于签发临时凭据的键。此处 secret 也经 SecretManager / catalog secret 绑定，不在 TMS 策略内容。                                          |

以下认证模式小节仅在 credential-vending 基线上增加 IRC **客户端认证**属性。

#### 认证：none

无 IRC 客户端认证（实验室典型 `simple` / 开放 IRC）。

| 属性                                                  | 必填          | SecretManager? | 说明                                                               |
| --------------------------------------------------- | ----------- | -------------- | ---------------------------------------------------------------- |
| *（省略 `rest.auth.type`）* 或 `rest.auth.type` = `none` | 否           | 否              | 无用户名 / 密码 / 令牌。                                                  |
| §5.9 Credential vending 中的属性                        | 若使用 vending | 否（仅 header）    | 存储代发时仍设 `header.X-Iceberg-Access-Delegation=vended-credentials`。 |

IRC 认证本身无需 SecretManager 条目。

#### 认证：basic

针对 Gravitino IRC 的 HTTP Basic（本地用户 / basic authenticator）。TMS Jobs 以维护主体认证（启用授权时通常为用户 `tms`）。

| 属性                           | 必填          | SecretManager? | 说明                                                         |
| ---------------------------- | ----------- | -------------- | ---------------------------------------------------------- |
| `rest.auth.type`             | 是           | 否              | `basic`。                                                   |
| `rest.auth.basic.username`   | 是           | 否              | 自动化 TMS 运行：`tms`（或配置的 TMS 用户名）。                            |
| `rest.auth.basic.password`   | 是           | **是**          | 该用户密码。存为 SecretManager secret；Job 配置仅在 submit 时持 URN 或解析值。 |
| §5.9 Credential vending 中的属性 | 若使用 vending | —              | 同父节。                                                       |

#### 认证：oauth

针对 Gravitino IRC 的 OAuth2 client credentials 或 bearer token。

| 属性                           | 必填                                | SecretManager? | 说明                                                     |
| ---------------------------- | --------------------------------- | -------------- | ------------------------------------------------------ |
| `rest.auth.type`             | 是                                 | 否              | `oauth2`。                                              |
| `token`                      | token **或** client-credential 集之一 | **是**          | Bearer 访问令牌路径（短生命周期；常在 submit 时签发而非存储）。                |
| `oauth2-server-uri`          | client-credential 路径              | 否              | 令牌端点 URI。                                              |
| `credential`                 | client-credential 路径              | **是**          | OAuth client id 与 secret，通常 `client_id:client_secret`。 |
| `scope`                      | 推荐                                | 否              | OAuth scope（Iceberg 可默认 `catalog`）。                    |
| §5.9 Credential vending 中的属性 | 若使用 vending                       | —              | 同父节。                                                   |

`credential` 优先 client-credential + SecretManager，避免 TMS 在策略或模板中嵌入长期 bearer token。

#### 认证：kerberos

Gravitino authenticators 含 `kerberos` 时的 Kerberos / SPNEGO。Spark / Job 运行时须能为 TMS 服务主体获取 TGT。

| 属性                                                                           | 必填                                               | SecretManager?              | 说明                                                         |
| ---------------------------------------------------------------------------- | ------------------------------------------------ | --------------------------- | ---------------------------------------------------------- |
| `rest.auth.type`                                                             | 是（Iceberg REST 客户端暴露时）/ Gravitino 客户端 `authType` | 否                           | `kerberos`（或维护 Job 的 Iceberg / Gravitino 客户端使用的部署特定键）。     |
| Kerberos principal                                                           | 是                                                | 否                           | TMS Jobs 使用的服务主体（如 `tms/_HOST@REALM`）。                     |
| Keytab 路径或 keytab 材料                                                         | 是                                                | keytab 字节/路径 secret 时 **是** | Keytab 不得放在 `policy_meta`。经 SecretManager 或策略内容外预置的主机路径引用。 |
| `java.security.krb5.conf` / Hadoop `hadoop.security.authentication=kerberos` | 按集群需要                                            | 否                           | Spark driver/executor 的集群 Kerberos 接线。                     |
| §5.9 Credential vending 中的属性                                                 | 若使用 vending                                      | —                           | 同父节。Kerberos 认证 IRC；启用时存储访问仍用代发凭据。                         |

#### Submit 时解析

```text
1. 从模板 ⊕ 最近策略加载非认证 spark_conf / jobOptions
2. 从 TMS / metalake 设置选择 IRC 认证模式（none | basic | oauth | kerberos）
3. 启用时添加 credential-vending 键（header.X-Iceberg-Access-Delegation=…）
4. 解析密码 / oauth credential / keytab 的 SecretManager URN
5. 日志与持久化 runtime_job_template 快照中脱敏 secret
6. 以主体 tms 的 runJob
```

无 `tms_credential` 表。从本地 file SecretProvider 切换到 Vault / 云 KMS 仅是 provider 配置变更。

---

## 6. 多节点协调

在**多个** Gravitino / TMS 节点上，commit 可能在 IRC 节点 bump ①，但**每个**节点都运行 db-scheduler。
写入 `scheduled_tasks` **不会**让所有节点都执行；当 `execution_time` 到期时，节点**竞争**，**只有一个**
pick 给定实例（CAS + heartbeat）。

**入队 ①：** 策略创建/启用 INSERT；commit / crontab bump `execution_time`（§5.4、§5.7）。

**入队 ②：** 仅由成功的 ① expand pick 写入（§5.5）。

**Expand 互斥：** ① `tms-policy-expand` 上的 pick（§6.1）。

**Spark-submit 互斥：** 每条 ② `tms-spark` 上的 pick；pick 路径后 **DELETE** 该行。同一 `(table, policy)`
另有在途 `table_maintenance_job` + `minIntervalMs`（§6.2）。

**死 worker：** 错过 heartbeat 解锁 / 重新入队。已提交的 Spark job **不**取消；`finished_at IS NULL` 仍挡双 submit。

| 表                                      | 角色                                                                                           |
| -------------------------------------- | -------------------------------------------------------------------------------------------- |
| `policy_meta` / `policy_relation_meta` | **expand 什么**、`schedule`（§5.7）与非认证 `jobOptions`（§5.8）                                        |
| `scheduled_tasks`                      | ① 长期 expand + ② 一次性 spark-submit（pick 后 DELETE）                                              |
| `table_maintenance_job`                | 按运行 Validation JSON + submit 门控（§6.2）                                                        |
| SecretManager / SecretProvider         | `tms` 的 IRC 认证 secret（§5.9）；无 `tms_credential` 表                                             |
| `user_meta`                            | 启用授权时的内置 `tms` 用户（§5.6）                                                                      |

认证材料用 SecretManager。Job-template **参数**留在策略挂载上。

### 6.1 两层租约（`scheduled_tasks`）

```text
策略创建 → INSERT ①（policy_id）
Commit / crontab → bump ① 到期
Node A / B / C 轮询
        │
        ├─ pick ① → INSERT 多条 ② → 保留 ① → unpick
        ├─ pick ②ₐ → submit → DELETE ②ₐ
        ├─ pick ②ᵦ → submit → DELETE ②ᵦ   （可在另一节点）
        └─ picker 死亡 → 错过 heartbeat → 实例可再次运行
```

**不是**「写入 `scheduled_tasks` ⇒ 每个节点都执行」。**是**「到期 ⇒ 每个节点都可尝试；恰好一个 claim 跑该实例」。

示意 `scheduled_tasks` 用法（db-scheduler 拥有；MySQL 形态）：

```sql
-- 由 db-scheduler 拥有；见上游 DDL
-- PRIMARY KEY (task_name, task_instance)
-- ① task_name = 'tms-policy-expand', task_instance = '{policy_id}'
-- ② task_name = 'tms-spark', task_instance = '{policy_id}:{table_id}' or '{policy_id}:batch:{batch_id}'
-- execution_time, picked, picked_by, last_heartbeat, version, task_data, ...
-- ② pick 路径成功结束后（submit 或门控跳过）：DELETE 该行。
-- ① 仅在策略禁用 / 删除 / 替换时删除（§5.5.2）。
```

### 6.2 按运行维护作业表（`table_maintenance_job`）

**表名：** `table_maintenance_job`

每个 Spark `job_run_id` 一行。同一张表同时服务 Automate **Jobs → Validation**（前后 JSON）与 submit 门控
（在途行 + `minIntervalMs`）。`minIntervalMs` 以本行 `finished_at` 作为任务结束时间，不用 `job_run_meta.job_finished_at`。

| 列                | 类型                         | 说明                                                                    |
| ---------------- | -------------------------- | --------------------------------------------------------------------- |
| `job_run_id`     | `BIGINT UNSIGNED NOT NULL` | `job_run_meta.job_run_id`；主键                                          |
| `metalake_id`    | `BIGINT UNSIGNED NOT NULL` | Metalake id                                                           |
| `table_id`       | `BIGINT UNSIGNED NOT NULL` | `table_meta` 代理 id（§5.5.4 import 后）                                   |
| `policy_id`      | `BIGINT UNSIGNED NOT NULL` | `policy_meta.policy_id`                                               |
| `before_metrics` | `MEDIUMTEXT NULL`          | 改表**前**采样的 JSON；可延迟到终态才落库                                             |
| `after_metrics`  | `MEDIUMTEXT NULL`          | Job 终态后的 JSON；待处理时为 null                                              |
| `finished_at`    | `BIGINT UNSIGNED NULL`     | 任务结束时间（epoch 毫秒）；**null = 在途**；驱动 `minIntervalMs`                     |

**主键：**（`job_run_id`）。不存 `table_identifier` 或 `schema_id` —— `table_id` 已足够，并与表级
`policy_relation_meta.metadata_object_id` 对齐。

无 `evaluate_pending`、无 `IDLE`/`RUNNING`、无应用租约 heartbeat —— 这些是 `scheduled_tasks` 的事。

| 存储                      | 角色                                     | 适合 Jobs Validation?            |
| ----------------------- | -------------------------------------- | ------------------------------ |
| `statistic_meta`        | 每对象 + 名称的最新统计                          | 否 — 仅当前值                       |
| `table_metrics`         | 按 `metric_ts` 追加时序                     | 否 — 非按 `job_run_id`            |
| `job_metrics`           | Job 范围 optimizer 时序                    | 否 — 非表前后 UI                    |
| `table_maintenance_job` | 每 job 冻结前后 JSON                        | **是**                          |

**Submit 门控**（给定 `(metalake_id, table_id, policy_id)`），在 **`runJob` 之前**应用：

1. **在途：** `SELECT 1 … WHERE finished_at IS NULL LIMIT 1` —— 若存在则跳过 submit。
2. **最小间隔：** `SELECT MAX(finished_at) …` 与解析的 `minIntervalMs`（§8.3）比较。

**生命周期：**

1. 门控 + `Recommender` 通过后：`runJob` → 得到 `job_run_id`。
2. **立刻** `INSERT`（`job_run_id`、`metalake_id`、`table_id`、`policy_id`），`finished_at = NULL`
   （在途占坑）。此时 `before_metrics` / `after_metrics` 可为 null。
3. Spark 异步跑。在任何改表前**采样** `before_metrics`（TMS submit 路径或 Spark 任务开头）。JSON 可等到步骤 4 再落库。
4. Job 终态（回调 / 监听器）：`UPDATE` `before_metrics`（若尚未写入）、`after_metrics` 与 `finished_at`。

**不要**在 rewrite / expire 已经改表后再去采样「before」指标。

示意 MySQL DDL：

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

**JSON 格式：** UTF-8 对象，指标键扁平存放，值为 number / string / boolean。可选顶层字段：

- `task_type` — 如 `compaction`、`snapshot-expiry`（建议写在 `before_metrics`）
- `passed` — 整体验证结果（建议仅写在 `after_metrics`）

按任务类型的 `before_metrics` / `after_metrics` 示例键：

| 任务类型               | JSON 示例键                                                          | 自动通过/失败                     |
| ------------------ | ----------------------------------------------------------------- | --------------------------- |
| `compaction`       | `num_files`、`avg_file_size_bytes`、`snapshot_count`                | 推荐（`passed`）                |
| `snapshot-expiry`  | `snapshot_count`、可选 `deleted_manifest_files_count`                | 推荐（`passed`）                |
| `manifest-rewrite` | `manifest_file_count`（若采集）                                        | 可选 / 跳过                     |
| `orphan-cleanup`   | 删除摘要计数；跳过 Files 前后对比                                              | 跳过表指标对比                     |

`after_metrics` 示例：

```json
{
  "task_type": "compaction",
  "num_files": 42,
  "avg_file_size_bytes": 134217728,
  "snapshot_count": 8,
  "passed": true
}
```

### 6.3 表删除生命周期

Iceberg **表 drop** 成功后，IRC 调用进程内 `IcebergTableLifecycleHook`：

1. 将已 drop 的 `catalog.schema.table` 解析为 `table_id`（若 `table_meta` 中仍存在）。
2. `DELETE` `task_instance` 包含该 `table_id` 的未完成 **`tms-spark`** ② 行。
3. 对 `(metalake_id, table_id)` `DELETE` `table_maintenance_job`。
4. **不**删除 **`tms-policy-expand`** ①（策略级）；该 policy 下其余表仍可在下次到期 expand。在途 Spark job **不**由此钩子取消。

**表重命名不在范围。**

---

## 7. Optimizer CLI 替代 API

Commit 路径保持进程内，**不**调用这些 API。它们为运维与脚本替代 `gravitino-optimizer` CLI（`--type ...`）。
同一插件在主 webserver（**8090**）上提供。

- 前缀：`/api/maintenance/table/ops/...`。不在 Compact 策略 UI 展示。
- 调用方须对每个目标表拥有 **WRITE**。缺权限 → **403**。
- `--conf-path` 仍为服务器配置。`update-statistics` 与 `append-metrics` 在 body 发送 JSON Lines。
  API 不接受服务器 `--file-path`。
- `dryRun=true` 返回推荐或 job 配置，不 submit。

| CLI `--type`              | 方法     | 路径                                             | Body 或 query                                                                                       |
| ------------------------- | ------ | ---------------------------------------------- | -------------------------------------------------------------------------------------------------- |
| `submit-strategy-jobs`    | `POST` | `/api/maintenance/table/ops/strategy-jobs`     | `identifiers`, `strategyName`, `dryRun`, `limit`                                                   |
| `submit-update-stats-job` | `POST` | `/api/maintenance/table/ops/update-stats-jobs` | `identifiers`, `dryRun`, `updateMode` (`stats` / `metrics` / `all`), `updaterOptions`, `sparkConf` |
| `update-statistics`       | `POST` | `/api/maintenance/table/ops/statistics`        | `calculatorName`, `identifiers`, `statisticsPayload` (JSON Lines)                                  |
| `append-metrics`          | `POST` | `/api/maintenance/table/ops/metrics`           | `calculatorName`, `identifiers`, `statisticsPayload` (JSON Lines)                                  |
| `monitor-metrics`         | `POST` | `/api/maintenance/table/ops/metrics/monitor`   | `identifiers`, `actionTime`, `rangeSeconds`, `partitionPath`                                       |
| `list-table-metrics`      | `GET`  | `/api/maintenance/table/ops/metrics/tables`    | `identifiers`, `partitionPath`                                                                     |
| `list-job-metrics`        | `GET`  | `/api/maintenance/table/ops/metrics/jobs`      | `identifiers`                                                                                      |

每条路由调用现有 optimizer 命令实现。IRC 钩子与 `MaintenanceSparkSubmitPipeline` **不**调用本组。

---

## 8. 配置

### 8.1 启用键（`gravitino.conf`）

| 键                                         | 默认       | 说明                                                                                                        |
| ----------------------------------------- | -------- | --------------------------------------------------------------------------------------------------------- |
| `gravitino.server.rest.extensionPackages` | none     | 须包含 TMS Feature 包（示例：`org.apache.gravitino.maintenance.web.rest.feature`）。                                |
| `gravitino.auxService.names`              | none     | 使用 IRC 时须包含 `iceberg-rest`。TMS 本身**不**以此方式启动。                                                             |

### 8.2 Iceberg REST → TMS 进程内事件键

示例键（实现时可能最终确定名称）。

| 键（示例）                                               | 默认      | 说明                                                                                         |
| --------------------------------------------------- | ------- | ------------------------------------------------------------------------------------------ |
| `gravitino.iceberg-rest.tableMaintenance.inProcess` | `false` | 为 `true` 时，IRC 在 commit 后调用**主服务注册的事件回调 / SPI**。                                           |

因 IRC 可能用隔离 classloader，回调须由 TMS 插件注册（如在 `GravitinoEnv`），不能直接 cast 到 TMS 实现类。
IRC 与主服务器须共享**同一 JVM**。

```properties
gravitino.server.rest.extensionPackages = org.apache.gravitino.maintenance.web.rest.feature
gravitino.auxService.names = iceberg-rest
gravitino.iceberg-rest.tableMaintenance.inProcess = true
gravitino.maintenance.scheduler.enabled = true
gravitino.maintenance.scheduler.threads = 4
```

HTTP `tableMaintenance.uri` / Kafka 生产消费键**不在**范围（非目标 #5）。

### 8.3 任务类型与最小间隔（全局默认 + 表覆盖）

TMS 识别四种维护**任务类型**（与产品 Compact 策略面对齐）：

| 任务类型               | 典型 job / 策略                                      | 代码默认 `minIntervalMs`         |
| ------------------ | ------------------------------------------------ | ---------------------------- |
| `compaction`       | rewrite 数据文件 / `system_iceberg_compaction`       | `3600000`（1 小时）              |
| `snapshot-expiry`  | 过期快照                                             | `86400000`（1 天）              |
| `manifest-rewrite` | rewrite manifest                                 | `86400000`（1 天）              |
| `orphan-cleanup`   | 孤儿文件清理                                           | `604800000`（7 天）             |

**Snapshot expiry 批量：** 当 snapshot-expiry 策略挂在 **catalog** 或 **schema** 上时，TMS 可将该
范围内的多张表合并提交到**同一个 Spark job**（driver 内依次 `expire_snapshots`）。默认每 job **10 张表**，
超出部分拆到后续 job。挂在**表**上的 snapshot-expiry 仍为**一表一 job**（commit 驱动路径）。

**解析顺序**（先命中者生效），同 Amoro 表属性 + AMS 默认思路：

```text
1. 表属性覆盖（若设置）
2. 全局 gravitino.conf 键（若设置）
3. 上表代码默认
```

**全局键**（`gravitino.conf`，前缀 `gravitino.maintenance.`）：

| 键                                     | 说明                                                     |
| ------------------------------------- | ------------------------------------------------------ |
| `task.compaction.minIntervalMs`       | compaction job 默认最小间隔                                  |
| `task.snapshot-expiry.minIntervalMs`  | snapshot expiry 默认最小间隔                                 |
| `task.snapshot-expiry.batchSize`      | catalog / schema 级 snapshot-expiry 每 job 最多表数（默认 `10`） |
| `task.manifest-rewrite.minIntervalMs` | manifest rewrite 默认最小间隔                                |
| `task.orphan-cleanup.minIntervalMs`   | orphan cleanup 默认最小间隔                                  |

**表级覆盖**（Iceberg / Gravitino 表属性）：

| 属性                                           | 覆盖                                           |
| -------------------------------------------- | -------------------------------------------- |
| `maintenance.compaction.minIntervalMs`       | 该表 compaction 最小间隔                           |
| `maintenance.snapshot-expiry.minIntervalMs`  | 该表 snapshot expiry 最小间隔                      |
| `maintenance.manifest-rewrite.minIntervalMs` | 该表 manifest rewrite 最小间隔                     |
| `maintenance.orphan-cleanup.minIntervalMs`   | 该表 orphan cleanup 最小间隔                       |

管线在每次调度驱动调用时，用解析的 `minIntervalMs` 检查该 `(table_id, policy_id)` 的 `MAX(finished_at)`（§5.5）。
在途行（`finished_at IS NULL`）阻止并发 submit。策略内容仍拥有**触发阈值**（如 MSE）；间隔仅在 evaluate 运行
（调度器 pick 或 ops API）时限制成功 submit 的重复频率。

表示例覆盖：

```sql
ALTER TABLE rest_catalog.db.orders SET TBLPROPERTIES (
  'maintenance.compaction.minIntervalMs' = '7200000'
);
```

### 8.4 db-scheduler 键（`gravitino.conf`）

| 键                                                             | 默认                                        | 说明                                                                 |
| ------------------------------------------------------------- | ----------------------------------------- | ------------------------------------------------------------------ |
| `gravitino.maintenance.scheduler.enabled`                     | MySQL/PostgreSQL 上 `true`；H2 上 `false`    | 启用嵌入式 db-scheduler。entity store 为 H2 时自动 false。                    |
| `gravitino.maintenance.scheduler.threads`                     | `4`                                       | db-scheduler 工作线程（并发短 evaluate 定容）。                                |
| `gravitino.maintenance.scheduler.pollingIntervalMs`           | `10000`                                   | 轮询到期任务的频率。                                                         |
| `gravitino.maintenance.scheduler.heartbeatIntervalMs`         | `60000`                                   | 短 evaluate 回调运行时的 heartbeat。                                       |
| `gravitino.maintenance.scheduler.missedHeartbeatsLimit`       | `6`                                       | pick 视为死亡前错过的 heartbeat 数。                                         |
| `gravitino.maintenance.scheduler.alwaysPersistTimestampInUTC` | MySQL 上 `true`                            | 按 db-scheduler 文档 MySQL 时间戳处理需要。                                   |

依赖（示例，实现时固定版本）：

```kotlin
implementation("com.github.kagkarlsson:db-scheduler:<version>")
```

`scheduled_tasks` DDL 仅加入 MySQL 与 PostgreSQL 后端的 entity-store 迁移。

---

## 9. 工作计划与检查清单

### 9.1 建议工作计划

本设计交付进程内插件、IRC commit **bump** policy-expand ①、两层 `scheduled_tasks`（① 保留 / ② pick 后 DELETE）、
按运行 `table_maintenance_job`、`tms` 主体 + SecretManager、策略 `jobOptions` 与 expand → spark-submit 管线。

| 阶段    | 工作项                                 | 说明                                                                                                              |
| ----- | ----------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| 1     | 加载进程内插件                             | `TableMaintenanceRESTFeature`；启停 db-scheduler；`TmsPrincipalBootstrapListener`。                                  |
| 2     | 内部 expand + spark-submit 管线         | `PolicyExpandPipeline` + `MaintenanceSparkSubmitPipeline`；单元测试。                                                 |
| 3     | db-scheduler 两类任务                   | `tms-policy-expand` + `tms-spark`；`scheduled_tasks` 迁移；heartbeat（§5.5）。                                         |
| 4     | 进程内 IRC 钩子（入队路径）                    | 调度实例（§5.4）；`table_maintenance_job` 按运行行（§6.2）。                                                                  |
| 5     | 加固                                  | 服务指标、优雅关闭、H2 路径测试、用户文档。                                                                                         |
| 6     | Optimizer CLI 替代 API                | §7 ops 资源。与 `gravitino-optimizer` 相同命令。                                                                         |
| 7     | TMS 主体 + SecretManager 认证           | `tms` 用户/角色（§5.6）；credential vending + none/basic/oauth/kerberos（§5.9）；策略 `schedule`（§5.7）+ `jobOptions`（§5.8）。 |

#### 阶段 1 检查清单

- [ ] 添加 `TableMaintenanceRESTFeature`（Jersey 2 `Feature`）注册进程内回调。ops 资源在阶段 6 添加。不要加 health 或 commit-event 资源。
- [ ] 经 `gravitino.server.rest.extensionPackages` 注册（示例包 `org.apache.gravitino.maintenance.web.rest.feature`）。
- [ ] 将插件 jar 与主 Gravitino 服务器发行版打包（在主服务器 classpath）。
- [ ] 记录 `extensionPackages` 启用方式。
- [ ] 单元测试：Feature 注册回调且不暴露 commit 或 health 资源。
- [ ] `TmsPrincipalBootstrapListener`（`EventListenerPlugin`）：禁用授权时跳过 `user_meta`；启用时注册到
      `eventListenerManager`，插件 `start()` reconcile 所有 metalake，并在 `CreateMetalakeEvent` 时 reconcile
      （`ASYNC_ISOLATED`）（§5.6）。

#### 阶段 2 检查清单

- [ ] 实现 `GravitinoTableImportService`（`TableDispatcher.loadTable` → `table_meta`；§5.5.4）。
- [ ] import 后按 backend 解析 owner；永不将 owner 默认设为 `tms`。
- [ ] 实现 `PolicyExpandPipeline`（挂载 → INSERT `tms-spark` ②；保留 ①）。
- [ ] 实现 `MaintenanceSparkSubmitPipeline` 调用 `Recommender.submitForStrategyName`。
- [ ] 在 ② 上经 `table_maintenance_job` 强制门控：在途 / 最小间隔；完成后 DELETE ②。
- [ ] 经现有 Policy / `StrategyProvider` 解析 Active 挂载策略（无新策略库）。
- [ ] 为 skip / noop / submit 结果添加单元测试。

#### 阶段 3 检查清单

- [ ] 添加 `TableMaintenanceScheduler` 封装 db-scheduler（`tms-policy-expand` + `tms-spark`）。
- [ ] 为 `scheduled_tasks` 添加 entity-store 迁移（MySQL / PostgreSQL）。
- [ ] 从关系 entity store 接线 `DataSource`；遵守 §8.4 heartbeat 键。
- [ ] H2 后端：调度器禁用；测试中直接调用管线（§5.5.3）。
- [ ] 集成测试：两节点；仅一个 pick 胜出；死 JVM 经错过 heartbeat 释放租约。

#### 阶段 4 检查清单

- [ ] 添加 `IcebergCommitEventHandler` 与主服务注册进程内回调 / SPI（§5.1.1 / §8.2）。
- [ ] 为 **`table_maintenance_job`** 添加 EntityStore 迁移（§6.2）。
- [ ] 策略创建/启用时 INSERT **`tms-policy-expand`** ①（`policy_id`）（§5.5.2）。
- [ ] IRC post-commit 为 Active `onCommit` 策略 bump ①；**不**在 commit 线程插入 ②。不要加单独的 TMS 事件表。
- [ ] IRC post-commit 钩子接到进程内回调（`tableMaintenance.inProcess`）。
- [ ] IRC **drop** 钩子删除调度器实例 + `table_maintenance_job` 行（§6.3）。
- [ ] `runJob` 成功后：`INSERT` `table_maintenance_job`（`job_run_id` + 键，`finished_at` null）；
      终态：`UPDATE` `before_metrics` + `after_metrics` + `finished_at`（§6.2）。
- [ ] 集成测试：到期任务单 pick（N 抢 1）；在途 / minInterval 跳过 submit；drop 清理调度器 + 作业行。
- [ ] **不要**交付 HTTP `…/events/iceberg-commit` 或 Kafka 入口。

#### 阶段 5 检查清单

- [ ] 服务指标：入队数、pick 数、submit 数、dead-execution 恢复、失败。
- [ ] 优雅关闭：entity store 关闭前停止 db-scheduler。
- [ ] 更新面向用户的 TMS / optimizer 文档：调度器入队 + 按运行 `table_maintenance_job`。

#### 阶段 6 检查清单

- [ ] 添加 §7 七个 ops 资源，各调用现有 optimizer 命令实现。
- [ ] 要求每个目标表 WRITE；缺权限返回 403。
- [ ] `dryRun=true` 返回推荐或 job 配置，不 submit。
- [ ] body 接受 statistics 与 metrics JSON Lines。不接受服务器 `--file-path`。
- [ ] 测试：每个 CLI `--type` 映射一条路由；commit 路径不调用这些路由。

#### 阶段 7 检查清单

- [ ] 为 TMS IRC 认证 secret 接线 **SecretManager**（无 `tms_credential` 表）。可选：未启用 SecretManager 时明文 / 缺失 secret 仍可用。
- [ ] 策略 create/alter 拒绝 `jobOptions` / `rewriteOptions` 中形似凭据的键。
- [ ] Submit 路径：最近策略 `jobOptions`（table > schema > catalog）叠加模板基线；启用时加 credential-vending header；
      解析 **none / basic / oauth / kerberos** 认证（§5.9）；以 `tms` 的 `runJob`。
- [ ] 测试：关闭授权跳过用户插入；开启授权授予 `USE_CATALOG`、`USE_SCHEMA`、`PROBE_TABLE_LIKE`、`MODIFY_TABLE`、
      `VIEW_POLICY`、`USE_JOB_TEMPLATE`、`RUN_JOB`。
- [ ] 测试：表挂载覆盖 catalog `jobOptions`；认证 secret 永不持久化到 `policy_meta`；`runtime_job_template` 脱敏密码 / 令牌 / keytab。

### 9.2 评审检查清单

| 领域            | 检查清单                                                                                                             |
| ------------- | ---------------------------------------------------------------------------------------------------------------- |
| 部署            | 经 `gravitino.server.rest.extensionPackages` 启用；IRC 同 JVM 同机。Ops API 在 **8090**（§7）。                              |
| Classpath     | TMS 插件在主服务器 classpath；**不是** aux 隔离监听。                                                                           |
| 触发            | Commit / crontab bump **① expand**；expand 写 **② spark**；`minIntervalMs` 仅在 ②。                                    |
| 调度            | **db-scheduler**：① 按 `policy_id` 长期保留；② 一次性 pick 后 DELETE（§5.5、§6.1、§8.4）。                                       |
| 作业记录          | `table_maintenance_job` 每 `job_run_id` 一行（Validation JSON + submit 门控）。                                          |
| Import        | 经 `TableDispatcher.loadTable` 懒 import `table_meta`（§5.5.4）；非 Iceberg `registerTable`。                           |
| Ops API       | 七条路由替代 `gravitino-optimizer`（§7）。commit 路径不用。须表 WRITE。                                                           |
| 管线            | ① expand → INSERT ②；② pick → 门控 → `Recommender` → Jobs → DELETE ②。                                               |
| Validation    | `table_maintenance_job`（`before_metrics` / `after_metrics` JSON + `finished_at`）（§6.2）。                          |
| Drop          | Drop 钩子删除调度器实例 + `table_maintenance_job` 行（§6.3）。重命名不在范围。                                                        |
| 多节点           | 到期 `scheduled_tasks`：N 轮询、一个 pick；在途行阻止 Spark 双 submit。                                                          |
| 策略            | 复用 metalake Policy API + `policy_meta`；`schedule`（§5.7）+ `jobOptions`；最近挂载优先（§5.8）。                              |
| Job 边界        | evaluate 到期时间 ≠ Spark 墙钟启动；Validation 指标在 `table_maintenance_job`（§6.2）。                                         |
| 主体            | 自动化 Jobs 以 `tms` 运行；启用授权时由 `TmsPrincipalBootstrapListener` 在插件启动 + `CreateMetalakeEvent` 时 bootstrap（§5.6）。      |
| 凭据            | 认证经 SecretManager（none/basic/oauth/kerberos）+ credential vending（§5.9）；不在策略 / 不在 `tms_credential`。               |
| 安全            | Ops API 须表 WRITE。TMS 角色为 list + 表写 + run job 最小权限。无 commit-event 或 health 端点。                                    |
| 许可证           | db-scheduler 为 **Apache 2.0**；无 LGPL 调度依赖。                                                                       |

---

## 10. 参考文献

1. [Gravitino Iceberg REST service](../docs/iceberg-rest-service.md)
2. [Gravitino Lance REST service](../docs/lance-rest-service.md)
3. [Manage policies in Gravitino](../docs/manage-policies-in-gravitino.md)
4. [Iceberg compaction policy](../docs/iceberg-compaction-policy.md)
5. [Table Maintenance optimizer overview](../docs/table-maintenance-service/optimizer.md)
6. [Design of SCIM 2.0 User and Group Provisioning in Gravitino](./gravitino-scim-provisioning.md)
7. [Amoro AIP-3 – Event-Triggered Optimization of Iceberg Tables](https://cwiki.apache.org/confluence/display/AMORO/AIP-3%3A+Event-Triggered+Optimization+of+Iceberg+Tables+in+Amoro)
8. [OpenHouse architecture (Jobs Scheduler / CronJob data services)](https://github.com/linkedin/openhouse/blob/main/ARCHITECTURE.md)
9. [Apache Iceberg REST Catalog OpenAPI](https://github.com/apache/iceberg/blob/main/open-api/rest-catalog-open-api.yaml)
10. [db-scheduler](https://github.com/kagkarlsson/db-scheduler) — 嵌入式持久调度器（Apache 2.0）
