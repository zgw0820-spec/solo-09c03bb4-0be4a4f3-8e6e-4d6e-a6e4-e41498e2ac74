# 聚合口摘链演练（FastAPI 本地版）

在本地模拟“从聚合口摘掉一根成员链路”的值班操作：两条 **FIFO** 成员链路（A/B）
的送达延时与确认延时都可调，按流发送带编号的报文，申请摘链时设置等待时限和
缓冲上限，然后观察对端收包顺序、每包实际走的链路，以及本次摘链的最终结果。

演练支持两种时钟，**启动时选择、运行中不可切换**（切换需重新启动或 `reset` 后重启）：

- **实时时钟（默认，`realtime`）**：延时按墙上时间流逝，行为与原版完全一致。
- **虚拟时钟（`virtual`，快进模式）**：模拟时钟**只在调用快进 API 时推进**，
  不推进则一切静止；一次推进按离散事件跑完截至目标模拟时刻的全部收包、确认和
  摘链超时。分钟级延时无需真的等待，适合值班员连续试多组链路延时与摘链时限。

## 规则（与演练行为一一对应）

- **两条链路都是严格 FIFO**：同一条链路同一时刻只处理一个报文，先送达再等确认，
  然后才取下一个。因此同一条流按接纳顺序送达，不丢不重。
- **对端确认**：摘链申请创建时，把成员链路上**当时已排队/在途**的受影响流报文
  记为一个快照。对端把快照中每个报文都收齐并回确认后，才把受影响流切到另一条链路。
- **等待期间**：待迁流的新包进入缓冲；缓冲总数达到上限就返回 `429` 拒收，**其他流
  照常发送**。被拒的编号没有被接纳，摘链结束后可以重发。
- **超时**：到达等待时限仍未收齐确认，本次摘链失败（`timed_out`），流不迁移，
  缓冲的报文按接纳顺序回到**原链路**。之后才到的确认会被记录，但**不会改变**本次结果。
  **最后一个快照包的确认恰好落在截止时刻也算超时**：同刻事件先处理摘链超时，
  该确认记为“迟到确认”（`late_confirmations_ignored`）。
- **一次只处理一个摘链申请**：已有 pending 申请时，第二个申请返回 `409`。
- 摘链成功后，成员链路从聚合组中摘除（`removed: true`），不能再在它上面建流；
  此时两条链路只剩一条，无法再摘，直到 `POST /api/reset` 复原。
- **快进确定性**：所有事件带模拟时刻并按 `(时刻, 优先级, 入堆序号)` 排序
  （摘链超时优先级高于同刻的收包/确认）。因此在操作顺序与操作时刻完全相同时，
  把一次推进拆成多段，收包顺序、每包模拟收包时刻和摘链结果与一次推进完全一致。
  模拟时钟只在推进时走，**跨过空闲时段不产生任何处理量**，处理耗时只与链路延时有关。
- `POST /api/reset` 清空本轮演练的流、报文、摘链申请与事件，**模拟时钟归零**。

## 启动

```bash
# 默认实时时钟（与旧版行为一致），端口 8000
docker compose up --build -d
# 打开 http://localhost:8000   （端口可用 DRILL_PORT=9000 覆盖）

# 额外起一个虚拟时钟（快进）服务，端口 8001，互不干扰
docker compose --profile virtual up --build -d drill-virtual
# 打开 http://localhost:8001
```

容器只有一个 worker（状态在内存里），健康检查打 `/healthz`。初始延时可通过
`.env`（参考 `.env.example`）或启动后 `PATCH /api/links/{A|B}` 调整。时钟模式由
环境变量 `DRILL_CLOCK_MODE` 决定（`realtime` 默认，或 `virtual` / `fastforward`）。

不想用 Docker 时可以直接跑：

```bash
python3 -m pip install -r requirements.txt
# 实时
python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers 1
# 快进
DRILL_CLOCK_MODE=virtual \
  python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8001 --workers 1
```

## 快进 API（仅虚拟时钟模式）

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/api/clock` | 当前时钟模式、模拟时刻（毫秒 + ISO）、待处理事件数 |
| POST | `/api/clock/advance` | 入参 `{"ms": 1000}`，推进指定毫秒并返回模拟时刻与本轮处理的事件数 |

- 时钟**只在** `advance` 时走；`ms=0` 合法，只返回当前时刻。单次上限 1 小时
  （3,600,000ms），负数/超限返回 `422`。
- 实时模式下调用 `advance` 返回 `409`，原有的发包、摘链、查询 API 在两种模式下
  路径和语义完全相同。
- 推进是同步的：返回时，所有 `due <= 目标时刻` 的送达、确认、摘链超时（以及摘链
  结束触发的缓冲包转发）都已处理完毕；转发产生的后续事件若也在目标时刻前，会在
  同一次推进内继续处理。

### 可直接调用的快进演练请求

以下命令针对虚拟时钟服务 `http://localhost:8001`，全程没有真实等待，时间单位毫秒。

```bash
BASE=http://localhost:8001

# 0) 复位（清空本轮演练，模拟时钟归零）
curl -s -X POST $BASE/api/reset

# 1) 设定链路延时（与实时模式同一套 API）
curl -s -X PATCH $BASE/api/links/A \
  -H 'Content-Type: application/json' -d '{"delivery_delay_ms":300,"ack_delay_ms":200}'
curl -s -X PATCH $BASE/api/links/B \
  -H 'Content-Type: application/json' -d '{"delivery_delay_ms":150,"ack_delay_ms":80}'

# 2) 建流、发包、申请摘链（与实时模式完全相同）
curl -s -X POST $BASE/api/flows -H 'Content-Type: application/json' -d '{"flow_id":"voice","link":"A"}'
curl -s -X POST $BASE/api/flows/voice/packets -H 'Content-Type: application/json' -d '{"seq":1}'
curl -s -X POST $BASE/api/flows/voice/packets -H 'Content-Type: application/json' -d '{"seq":2}'
RM=$(curl -s -X POST $BASE/api/removals -H 'Content-Type: application/json' \
  -d '{"member_link":"A","wait_ms":5000,"buffer_limit":5}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["removal_id"])')
curl -s -X POST $BASE/api/flows/voice/packets -H 'Content-Type: application/json' -d '{"seq":3}'

# 3) 快进 1000ms：A 上两包在 300/800ms 送达、500/1000ms 确认，摘链在 1000ms 成功
curl -s -X POST $BASE/api/clock/advance -H 'Content-Type: application/json' -d '{"ms":1000}'
# 4) 再快进 150ms：缓冲的 voice#3 在摘链后转投 B，1150ms 送达
curl -s -X POST $BASE/api/clock/advance -H 'Content-Type: application/json' -d '{"ms":150}'

# 5) 查看确定性结果：收包时刻 300/800/1150ms，链路 A/A/B
curl -s $BASE/api/flows/voice/received | python3 -m json.tool
curl -s $BASE/api/removals/$RM | python3 -m json.tool
```

**同刻超时边界**：把 `wait_ms` 设成与最后一个确认相同的时刻（A 单包
`300+200=500ms`，即 `"wait_ms":500`），推进 500ms 后结果为 `timed_out`，
缓冲包走原链路 A，该确认记为迟到（`late_confirmations_ignored=1`）。

**分段与一次推进等价**：同一组操作，把 `advance 1000` 拆成 10 次 `advance 100`，
收包顺序、每包 `received_at_ms` 和摘链结果与一次推进完全一致（演练脚本场景 5 会自动核对）。

## 可直接调用的演练请求（实时时钟）

以下命令假设实时服务在 `http://localhost:8000`，按顺序执行即可复现一次成功摘链。
时间单位均为毫秒。快进模式的对应请求见上文“快进 API”一节。

```bash
BASE=http://localhost:8000

# 0) 复位（清空流、申请和事件，模拟时钟归零）
curl -s -X POST $BASE/api/reset

# 1) 把链路延时调小，方便观察（可选）
curl -s -X PATCH $BASE/api/links/A \
  -H 'Content-Type: application/json' \
  -d '{"delivery_delay_ms": 300, "ack_delay_ms": 200}'
curl -s -X PATCH $BASE/api/links/B \
  -H 'Content-Type: application/json' \
  -d '{"delivery_delay_ms": 150, "ack_delay_ms": 80}'

# 2) 建两条流：voice 走 A（待摘），video 走 B
curl -s -X POST $BASE/api/flows \
  -H 'Content-Type: application/json' -d '{"flow_id":"voice","link":"A"}'
curl -s -X POST $BASE/api/flows \
  -H 'Content-Type: application/json' -d '{"flow_id":"video","link":"B"}'

# 3) voice 先发两包，让它们排在 A 上
curl -s -X POST $BASE/api/flows/voice/packets \
  -H 'Content-Type: application/json' -d '{"seq":1,"payload":"v1"}'
curl -s -X POST $BASE/api/flows/voice/packets \
  -H 'Content-Type: application/json' -d '{"seq":2,"payload":"v2"}'

# 4) 申请摘 A：最多等 5s，缓冲上限 5 包；记下 removal_id
RM=$(curl -s -X POST $BASE/api/removals \
  -H 'Content-Type: application/json' \
  -d '{"member_link":"A","wait_ms":5000,"buffer_limit":5}' \
  | python3 -c 'import json,sys; print(json.load(sys.stdin)["removal_id"])')
echo "removal_id=$RM"

# 5) 等待期间给 voice 再发一包 -> 202，进入缓冲
curl -s -X POST $BASE/api/flows/voice/packets \
  -H 'Content-Type: application/json' -d '{"seq":3}'

# 6) 其他流不受影响
curl -s -X POST $BASE/api/flows/video/packets \
  -H 'Content-Type: application/json' -d '{"seq":1}'

# 7) 再申请一次摘链 -> 409（一次只处理一个）
curl -s -X POST $BASE/api/removals \
  -H 'Content-Type: application/json' \
  -d '{"member_link":"B","wait_ms":1000,"buffer_limit":1}'

# 8) 等对端确认（A 上两包依次 300+200ms）后查看结果：
#    status=succeeded, moved_flow_ids=["voice"], flushed_to="B", snapshot_acked=2
sleep 1.2
curl -s $BASE/api/removals/$RM | python3 -m json.tool

# 9) 对端收包顺序：1,2 走 A，3 走 B，按 1,2,3 送达
sleep 0.3
curl -s $BASE/api/flows/voice/received | python3 -m json.tool
```

超时场景：把第 4 步的 `wait_ms` 调成比 `delivery+ack` 小（例如 `50`），再发第 5 步。
结果会是 `timed_out`、`moved_flow_ids: []`、`flushed_to: "A"`，之后迟到的确认
只体现在 `late_confirmations_ignored`。若 `wait_ms` 与最后一个确认时刻**完全相同**，
同样判超时且该确认记为迟到（虚拟时钟下为确定性行为，实时时钟下以同刻优先规则处理）。

缓冲上限场景：把 `buffer_limit` 设成 `1`，等待期间发两包，第二包返回 `429`。
被拒的 seq 未接纳，摘链完成后可以重发。

### 一键跑完整演练脚本

```bash
# 实时模式（本机已有服务时）
python3 drills/run_drill.py --base-url http://localhost:8000
# 或让 Compose 起一个一次性客户端（跑完即退出）
docker compose --profile drill run --rm drill-client
# 只跑某个场景：--only success|timeout|buffer|after_removal

# 快进模式（虚拟时钟服务，本机）
python3 drills/run_drill_virtual.py --base-url http://localhost:8001
# 或 Compose 一次性客户端
docker compose --profile virtual-drill run --rm drill-virtual-client
# 只跑某个场景：--only clock|success|timeout|buffer|idle|equivalent
```

两个脚本都只依赖标准库，全部检查通过时返回 0，否则返回 1 并列出失败项。
快进脚本覆盖：时钟只在推进时走、摘链成功与缓冲转发、同刻确认判超时且记迟到、
缓冲拒收、空闲时段零处理量、分段推进与一次推进结果完全一致。

## 接口一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/healthz` | 健康检查 |
| GET | `/api/clock` | 时钟模式、当前模拟时刻（`sim_time_ms`/`sim_time`）、待处理事件数 |
| POST | `/api/clock/advance` | 快进 `{ms}`，仅虚拟时钟可用；返回推进量、模拟时刻与处理事件数 |
| GET | `/api/links` | 两条链路的延时、队列深度、收发计数、是否已摘除、当前阶段到期时刻 |
| PATCH | `/api/links/{A\|B}` | 调整 `delivery_delay_ms` / `ack_delay_ms` |
| GET | `/api/flows` | 所有流及当前承载链路、已接纳/已送达计数 |
| POST | `/api/flows` | 建流 `{flow_id?, link}` |
| POST | `/api/flows/{id}/packets` | 发包 `{seq, payload?}`；正常入队 201，进缓冲 202，缓冲满 429，重复 seq 409 |
| GET | `/api/flows/{id}/received` | 对端收包顺序（序号、报文 ID、链路、ISO 与毫秒模拟时间） |
| GET | `/api/packets/{packet_id}` | 单个报文的状态与走过的链路 |
| POST | `/api/removals` | 申请摘链 `{member_link, wait_ms, buffer_limit}`；需等待 202，立即完成 200 |
| GET | `/api/removals` | 所有申请（倒序摘要） |
| GET | `/api/removals/{id}` | 单个申请详情：快照、确认进度、缓冲、拒收、迟到确认、事件时间线 |
| GET | `/api/state` | 时钟 + 链路 + 流 + 当前申请 + 最近事件 |
| POST | `/api/reset` | 清空并重建两条链路，模拟时钟归零 |

所有时间输出同时带 ISO 字符串与 `*_ms` 毫秒字段（如 `received_at_ms`、
`accepted_at_ms`、`deadline_ms`、`finished_at_ms`），虚拟时钟的毫秒值从 0 起算，
实时时钟的毫秒值从该引擎本次启动/复位起算。

交互界面在 `/`，包含时钟状态与快进按钮（仅虚拟模式显示）、链路延时调节、
建流/发包、摘链申请、对端收包顺序、事件日志，实时模式下每 0.5s 自动刷新。

## 目录

```
app/main.py             FastAPI 路由与静态页挂载（含 /api/clock 快进 API）
app/engine.py           演练核心：双时钟（实时/虚拟离散事件）、FIFO 链路、摘链状态机
app/config.py           从环境变量读取初始链路延时与 DRILL_CLOCK_MODE
app/static/index.html   值班界面（无外部依赖，含快进控件）
drills/run_drill.py          实时模式端到端演练脚本（仅标准库）
drills/run_drill_virtual.py  快进模式端到端演练脚本（仅标准库）
tests/test_drill.py     pytest 测试（实时回归 + 虚拟时钟/快进/等价性）
Dockerfile / docker-compose.yml
```

## 测试

```bash
python3 -m pip install -r requirements-dev.txt
python3 -m pytest -q
```

## 已知边界（本地演练的取舍）

- 状态全部在内存，重启容器即清空；只适合演练，不作持久化。
- 实时模式下每个成员链路用一条 `asyncio` 任务串行处理模拟 FIFO；虚拟模式改为
  离散事件堆，两种模式共用同一套 FIFO/摘链规则代码。没有真实网络丢包/乱序。
- “对端确认”由服务内部模拟：快照报文全部被确认即视为对端收齐；没有独立的对端进程。
- 时钟模式在引擎创建时固定，运行中不能切换；需要切换时用 `DRILL_CLOCK_MODE`
  重新启动（或直接使用 compose 里的第二个服务）。
- 实时模式的超时仍是一次性定时器，与链路延时同量级时墙上时钟的边界先后属于正常
  现象；虚拟模式不存在该问题，同刻事件按固定优先级（超时先于确认）确定性处理。
