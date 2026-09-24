# deepspace — 深空测控指令裁决服务

纯后端 API（Go 1.23 + 标准库 `net/http` + PostgreSQL）。在中继并发领取上行指令、
失联者带着旧确认在租约过期后迟到的场景下，保证：

- **不重复结算**：一条指令至多一个终态；终态永不被覆盖。
- **不抢占新持有者结果**：旧代次令牌迟到时返回 `409`，状态不变。
- **链式等待与阻断**：后继必须等前驱 `delivered` 后才可领取；前驱 `failed` 会原子阻断所有未领取后继。
- **重启后继续裁决**：指令、租约代次、终态全部持久化在 PostgreSQL。

## 运行

需要 Docker Compose（内置 PostgreSQL）。

```bash
# 宿主机发布端口由 API_PORT 控制（默认 8080）
API_PORT=8080 docker compose up --build
```

`up` 会依次启动 `db`、`api` 与 **`verify` 一次性验收服务**。`verify` 跑完即退出，
退出码为 0 表示验收通过。单独重跑：

```bash
docker compose run --rm verify
```

### 本地开发

```bash
# 需要一个可达的 PostgreSQL
export DATABASE_URL="postgres://user:pass@localhost:5432/deepspace?sslmode=disable"
go run ./cmd/api            # 端口取 API_PORT，默认 8080

# 集成测试（自动创建隔离的临时数据库）
export DEEPSPACE_TEST_ADMIN_URL="postgres://postgres@localhost:5432/postgres?sslmode=disable"
go test -race ./...
```

服务首次连接数据库时会自动执行幂等迁移（带咨询锁，多实例同时启动也安全）。

## API 契约

所有请求/响应均为 `application/json`。错误响应使用**稳定信封**，禁止客户端依赖
自然语言文案：

```json
{ "error": { "code": "lease_expired", "message": "lease token is expired or superseded by a newer generation" } }
```

### `POST /commands` — 创建指令

按创建顺序写入（自增 ID 即顺序）。`payload` 必填且不能为 `null`，可以是任意 JSON 值。

请求：`{"payload": {"seq": 1, "target": "mars-relay-7"}}`

可选 `predecessor_id` 声明至多一个前驱：

```json
{"payload": {"seq": 2}, "predecessor_id": 1}
```

- 前驱必须是已经存在的较早正整数编号；引用不存在编号返回 `422 unknown_predecessor`。
- 类型错误、0、负数或小数返回 `422 invalid_predecessor`；`null`/缺省表示无前驱。
- 前驱尚未 `delivered` 时，后继保持 `pending` 但不会被领取，也不会预发租约。
- 前驱已经 `failed`/`blocked` 时，新后继以 `201` 创建为只读 `blocked` 终态。

- `201` 返回指令对象
- `400 invalid_json`：请求体不是合法 JSON（或含多个 JSON 值）
- `422 missing_payload`：缺少 `payload` 或为 `null`

### `POST /claims` — 原子领取最早可用指令

请求：`{"lease_duration_ms": 1000}`

- `lease_duration_ms` 必须是 **100–5000** 的整数，否则 `422 invalid_lease_duration`；
  字段缺失为 `422 missing_lease_duration`。
- 领取规则：状态为 `pending`、整条前驱链均已 `delivered`、且未持有有效（未过期）租约的指令中，
  取 **ID 最小**的一条。前驱链未全部送达的指令只等待，不签发租约。
- 并发安全：底层使用 `SELECT ... ORDER BY id FOR UPDATE SKIP LOCKED`，
  同一时刻并发的任意多个领取者中**恰有一个**能拿到同一条指令。
- `200`：

  ```json
  {
    "command_id": 1,
    "generation": 1,
    "lease_token": "8d221c1e…(256bit 随机数的十六进制，不透明)",
    "lease_expires_at": "2026-09-18T00:00:01Z",
    "lease_duration_ms": 1000
  }
  ```

  每次领取都会推进该指令的 `lease_generation` 并签发**全新随机令牌**，
  新令牌必然不同于任何历史令牌（同时由数据库唯一约束兜底）。
- `204`：当前没有可领取的指令（无响应体）。

### `POST /commands/{id}/ack` — 凭租约确认终态

请求：`{"lease_token": "…", "result": "delivered"}`

- `result` 仅允许 `"delivered"` 或 `"failed"`，否则 `422 invalid_result`；
  令牌缺失为 `422 missing_lease_token`。
- 只有**未过期且属于当前代次**的令牌可以结算。裁决顺序（行锁内原子完成）：
  1. 指令为 blocked → `409 command_blocked`，delivered/failed → `409 already_settled`；
  2. 令牌属于旧代次 → `409 lease_expired`（失联者迟到）；
  3. 令牌已过期 → `409 lease_expired`；
  4. 令牌从未对该指令签发 → `409 invalid_lease_token`。
- 成功 `200` 返回指令详情，`status` 变为 `delivered` 或 `failed`。
- 结果为 `failed` 时，同一事务会把所有尚未领取（`lease_generation = 0`）的直接/间接后继
  原子置为 `blocked`；`blocked_by` 记录失败来源，`blocked_at` 记录阻断时间。
  blocked 指令不生成租约、不插入 settlement，也不能被领取。
- 对 blocked 指令的确认返回 `409 command_blocked`。
- 任意失败路径都**不会改变状态**。
- 未知编号：`404 unknown_command`（非数字 ID 同样按 404 处理）。

### `GET /commands/{id}` — 观察领取代次

`200` 返回指令详情：

```json
{
  "id": 1,
  "payload": {"seq": 1},
  "status": "failed",
  "predecessor_id": null,
  "blocked_by": null,
  "blocked_at": null,
  "lease_generation": 3,
  "current_lease_expires_at": "2026-09-18T00:00:03Z",
  "created_at": "…",
  "updated_at": "…",
  "leases": [
    { "generation": 1, "claimed_at": "…", "expires_at": "…" },
    { "generation": 2, "claimed_at": "…", "expires_at": "…" },
    { "generation": 3, "claimed_at": "…", "expires_at": "…" }
  ],
  "settlement": { "generation": 3, "result": "failed", "settled_at": "…" }
}
```

- `leases` 给出**每一代领取记录**，可观察代次推进；GET 永不返回令牌原文。
- `predecessor_id`：前驱编号，无前驱时为 `null`；
- `blocked_by` / `blocked_at`：仅 `blocked` 终态非空，记录阻断来源与时间；
- `settlement`：`delivered/failed` 的租约结算；`blocked` 仍为 `null`。
- 未结算且未阻断时 `settlement` 为 `null`。未知编号 `404 unknown_command`。

`GET /commands` 按创建顺序返回指令列表。`GET /healthz` 为存活探针。

### 状态码汇总

| 场景 | 状态码 | error.code |
| --- | --- | --- |
| 无可用指令可领取 | `204` | —（无体） |
| 未知指令编号 | `404` | `unknown_command` |
| 未知路由 / 方法不允许 | `404` / `405` | `route_not_found` / `method_not_allowed` |
| 请求体非法 JSON | `400` | `invalid_json` |
| 租期越界（<100 或 >5000ms）/ 缺失 | `422` | `invalid_lease_duration` / `missing_lease_duration` |
| 结果非法 / 令牌缺失 / 载荷缺失 | `422` | `invalid_result` / `missing_lease_token` / `missing_payload` |
| 前驱编号非法 / 前驱不存在 | `422` | `invalid_predecessor` / `unknown_predecessor` |
| 指令已被失败前驱阻断 | `409` | `command_blocked` |
| 旧代次或已过期令牌迟到 | `409` | `lease_expired` |
| 令牌从未对该指令签发 | `409` | `invalid_lease_token` |
| 重复结算 / 覆盖终态 | `409` | `already_settled` |

## 数据模型

迁移 SQL 内嵌于 `internal/store/migrations_sql/`，启动时自动应用：

- `commands`：指令本体、当前 `status`
  （`pending|delivered|failed|blocked`）、可选 `predecessor_id`、链归属
  `chain_id`、blocked 来源 `blocked_by/blocked_at`、当前租约代次、当前令牌与到期时间。
- `leases`：每一代租约（`(command_id, generation)` 唯一，`lease_token` 全局唯一），
  构成可观察的领取历史。
- `settlements`：以 `command_id` 为主键——即使应用层存在疏漏，数据库层面也**物理禁止
  插入第二条终态**，是“不重复结算”的最后防线。

领取在单事务内完成（行锁 + 代次推进 + 租约写入），确认在单事务内完成
（链咨询锁 + 行锁 + 终态裁决 + `settlements` 插入 + 状态翻转 + 后继阻断传播），
创建后继同样持有链咨询锁，因此创建与前驱失败结算交错时由数据库串行裁决，
任何崩溃/重启都不会留下半完成的裁决或“已失败却仍可领取”的后继。

## 验收交错（`cmd/verify`）

`verify` 是一次性服务，断言完整生命周期：

1. 输入校验契约（422/404/400/204 与稳定错误码）；
2. 创建唯一指令，**16 个请求并发抢领**，断言恰有 1 个 `200`、其余全为 `204`；
3. 等待第一代租约到期后重新领取，断言代次推进、新令牌不同；
4. 旧令牌迟到确认 → `409 lease_expired` 且状态仍为 `pending`；
5. 再等第二代到期、领取第三代，用第二代旧令牌逆序确认 → `409`，状态不变；
6. 当前持有者置 `failed`；旧/新/持有者令牌再次确认全部 `409`，终态不被覆盖；
7. 直连 PostgreSQL 带外核对：`settlements` 恰 1 行、`leases` 恰 3 代、状态唯一；
8. **重启 API 进程**（同一数据库、新进程重新迁移连接），核对终态 `failed/gen=3`
   保持不变，且重启后重复确认仍为 `409`、终态指令仍不可领取；
9. 创建前驱链，验证未送达前不得预发租约、逐级 delivered 解锁、failed 原子阻断后继链，
   并验证 blocked 不产生租约或结算、失败后新建后继立即 blocked。

退出码 0 即全部通过。

## 项目结构

```
cmd/api/main.go                 API 入口（迁移、连接重试、优雅关停）
cmd/verify/main.go              一次性验收服务
internal/httpapi/               路由、处理器、稳定 JSON 错误信封
internal/store/                 PostgreSQL 持久化与租约裁决
internal/store/migrations_sql/  内嵌迁移 SQL
internal/testdb/                测试用临时数据库辅助
```
