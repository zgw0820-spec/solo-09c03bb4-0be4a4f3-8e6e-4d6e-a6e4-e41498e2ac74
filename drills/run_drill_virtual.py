#!/usr/bin/env python3
"""Fast-forward (virtual clock) end-to-end drill.

Mirrors drills/run_drill.py but drives the service exclusively through
POST /api/clock/advance, so a minute-level drill finishes instantly and no
wall-clock sleeping is involved. Only the standard library is needed:

    python3 drills/run_drill_virtual.py --base-url http://localhost:8001

Scenarios:
  success     摘链成功：分段快进后对端确认收齐，流迁到 B，缓冲包经 B 送达。
  timeout     最后一个确认恰好落在截止时刻：先判超时，确认记为迟到，
              缓冲包走原链路 A。
  buffer      缓冲上限为 1：第二个待迁包被拒收（429），其他流照发。
  idle        模拟时钟只在推进时走；跨过空闲时段不产生任何处理量，
              处理时长只与链路延时有关。
  equivalent  同样的操作序列分别用“分段推进”和“一次推进”跑完，
              收包顺序、模拟收包时刻与摘链结果必须完全一致。
"""

from __future__ import annotations

import argparse
import json
import sys
import urllib.error
import urllib.request

FAILURES: list[str] = []


def call(base: str, method: str, path: str, body=None, expect: int | None = None):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(base + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as response:
            status = response.status
            payload = json.loads(response.read() or b"null")
    except urllib.error.HTTPError as exc:
        status = exc.code
        raw = exc.read()
        payload = json.loads(raw) if raw else None
    if expect is not None and status != expect:
        raise AssertionError(f"{method} {path} -> {status}, expected {expect}: {payload}")
    return status, payload


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"  [{mark}] {label}" + (f" — {detail}" if detail else ""))
    if not condition:
        FAILURES.append(label)


def advance(base: str, ms: int) -> dict:
    _, body = call(base, "POST", "/api/clock/advance", {"ms": ms}, expect=200)
    return body


def clock_ms(base: str) -> int:
    return call(base, "GET", "/api/clock", expect=200)[1]["sim_time_ms"]


def received(base: str, flow: str) -> list[dict]:
    _, data = call(base, "GET", f"/api/flows/{flow}/received", expect=200)
    return data["delivery_order"]


def reset(base: str) -> None:
    _, body = call(base, "POST", "/api/reset", expect=200)
    check("复位后模拟时钟归零", body["clock"]["sim_time_ms"] == 0)
    check("复位后为虚拟时钟模式", body["clock"]["clock_mode"] == "virtual")
    call(base, "PATCH", "/api/links/A", {"delivery_delay_ms": 300, "ack_delay_ms": 200}, expect=200)
    call(base, "PATCH", "/api/links/B", {"delivery_delay_ms": 150, "ack_delay_ms": 80}, expect=200)


def advance_until(base: str, predicate, max_ms: int = 60_000, step_ms: int = 100) -> dict | None:
    deadline = clock_ms(base) + max_ms
    while clock_ms(base) < deadline:
        advance(base, step_ms)
        if predicate():
            return predicate()
    return None


def removal(base: str, removal_id: str) -> dict:
    return call(base, "GET", f"/api/removals/{removal_id}", expect=200)[1]


def scenario_clock_only_moves_on_advance(base: str) -> None:
    print("\n== 场景 0：模拟时钟只在推进时走 ==")
    before = clock_ms(base)
    call(base, "POST", "/api/flows", {"flow_id": "tick", "link": "B"}, expect=201)
    call(base, "POST", "/api/flows/tick/packets", {"seq": 1}, expect=201)
    # No advance call in between: the simulated clock must not move and the
    # packet must still be queued.
    check("不推进则模拟时刻不变", clock_ms(base) == before)
    order = received(base, "tick")
    check("不推进则没有任何收包", order == [])


def scenario_success(base: str) -> None:
    print("\n== 场景 1：快进摘链成功 ==")
    call(base, "POST", "/api/flows", {"flow_id": "voice", "link": "A"}, expect=201)
    call(base, "POST", "/api/flows", {"flow_id": "video", "link": "B"}, expect=201)
    for seq in (1, 2):
        call(base, "POST", "/api/flows/voice/packets", {"seq": seq}, expect=201)

    _, rm = call(
        base, "POST", "/api/removals",
        {"member_link": "A", "wait_ms": 5000, "buffer_limit": 5}, expect=202,
    )
    rid = rm["removal_id"]
    check("摘链申请记录截止时刻", rm["deadline_ms"] == 5000 and rm["created_at_ms"] == 0)
    _, buffered = call(base, "POST", "/api/flows/voice/packets", {"seq": 3}, expect=202)
    check("等待期间 voice#3 进入缓冲", buffered["buffered"] is True)
    call(base, "POST", "/api/flows/video/packets", {"seq": 1}, expect=201)

    # Second concurrent request is refused.
    busy = call(
        base, "POST", "/api/removals",
        {"member_link": "B", "wait_ms": 1000, "buffer_limit": 1}, expect=409,
    )
    check("并发的第二个摘链申请被拒绝", busy[0] == 409)

    detail = advance_until(base, lambda: (lambda d: d if d["status"] != "pending" else None)(removal(base, rid)))
    check("快进后摘链成功（对端确认）", detail is not None and detail["status"] == "succeeded")
    if detail:
        check("已迁移流包含 voice", detail["moved_flow_ids"] == ["voice"])
        check("缓冲包转投链路 B", detail["flushed_to"] == "B" and len(detail["flushed_packet_ids"]) == 1)
        # A: pkt1 deliver 300 / ack 500, pkt2 deliver 800 / ack 1000 -> success at 1000ms.
        check("摘链完成的模拟时刻为 1000ms", detail["finished_at_ms"] == 1000)

    order = advance_until(base, lambda: (lambda o: o if len(o) == 3 else None)(received(base, "voice")))
    check("voice 三个包均已送达", order is not None)
    if order:
        check("voice 按接纳顺序送达 1,2,3", [x["seq"] for x in order] == [1, 2, 3])
        check("voice#1/#2 走 A，#3 走 B", [x["link"] for x in order] == ["A", "A", "B"])
        # pkt3 is flushed at t=1000 onto B: 1000+150 delivery -> 1150.
        check(
            "模拟收包时刻确定（300/800/1150ms）",
            [x["received_at_ms"] for x in order] == [300, 800, 1150],
            str([x["received_at_ms"] for x in order]),
        )
    _, flow = call(base, "GET", "/api/flows/voice", expect=200)
    check("voice 当前承载链路为 B", flow["link"] == "B")


def scenario_timeout_exact_deadline(base: str) -> None:
    print("\n== 场景 2：最后确认恰在截止时刻 -> 超时且确认迟到 ==")
    call(base, "POST", "/api/flows", {"flow_id": "telemetry", "link": "A"}, expect=201)
    call(base, "POST", "/api/flows/telemetry/packets", {"seq": 1}, expect=201)

    # A: delivery 300 + ack 200 -> the only snapshot packet is acked at t=500.
    # wait_ms=500 puts the deadline on exactly the same simulated instant.
    _, rm = call(
        base, "POST", "/api/removals",
        {"member_link": "A", "wait_ms": 500, "buffer_limit": 4}, expect=202,
    )
    rid = rm["removal_id"]
    call(base, "POST", "/api/flows/telemetry/packets", {"seq": 2}, expect=202)
    call(base, "POST", "/api/flows/telemetry/packets", {"seq": 3}, expect=202)

    clock = advance(base, 500)
    detail = removal(base, rid)
    check("一次推进直接跑到截止时刻", clock["sim_time_ms"] == 500)
    check("截止时刻确认仍判超时", detail["status"] == "timed_out" and detail["reason"] == "wait_timeout")
    check("超时后未迁移任何流", detail["moved_flow_ids"] == [])
    check("缓冲的两个包回到原链路 A", detail["flushed_to"] == "A" and len(detail["flushed_packet_ids"]) == 2)
    check("同刻确认记为迟到", detail["late_confirmations_ignored"] == 1, str(detail["late_confirmations_ignored"]))

    # The two buffered packets were re-queued on A at t=500. FIFO on A:
    # pkt2 delivered at 800, its ack frees the link at 1000, pkt3 at 1300.
    order = advance_until(base, lambda: (lambda o: o if len(o) == 3 else None)(received(base, "telemetry")))
    check("三个包最终全部送达", order is not None)
    if order:
        check("全部走原链路 A", [x["link"] for x in order] == ["A", "A", "A"])
        check("送达顺序 1,2,3", [x["seq"] for x in order] == [1, 2, 3])
        check(
            "缓冲包从超时时刻按 FIFO 重新排队（300/800/1300ms）",
            [x["received_at_ms"] for x in order] == [300, 800, 1300],
            str([x["received_at_ms"] for x in order]),
        )
    _, flow = call(base, "GET", "/api/flows/telemetry", expect=200)
    check("telemetry 仍承载在 A", flow["link"] == "A")


def scenario_buffer(base: str) -> None:
    print("\n== 场景 3：缓冲满则拒收，其他流照发 ==")
    call(base, "POST", "/api/flows", {"flow_id": "sig", "link": "A"}, expect=201)
    call(base, "POST", "/api/flows", {"flow_id": "bulk", "link": "B"}, expect=201)
    call(base, "POST", "/api/flows/sig/packets", {"seq": 1}, expect=201)

    _, rm = call(
        base, "POST", "/api/removals",
        {"member_link": "A", "wait_ms": 5000, "buffer_limit": 1}, expect=202,
    )
    rid = rm["removal_id"]
    call(base, "POST", "/api/flows/sig/packets", {"seq": 2}, expect=202)
    _, rejected = call(base, "POST", "/api/flows/sig/packets", {"seq": 3}, expect=429)
    check("缓冲已满时 sig#3 被拒收", rejected.get("buffer_limit") == 1)
    call(base, "POST", "/api/flows/bulk/packets", {"seq": 1}, expect=201)
    check("其他流 bulk#1 不受缓冲上限影响", True)

    advance_until(base, lambda: removal(base, rid)["status"] != "pending")
    detail = removal(base, rid)
    check("本次摘链成功", detail["status"] == "succeeded")
    check("拒收计数为 1", detail["rejected_count"] == 1)
    call(base, "POST", "/api/flows/sig/packets", {"seq": 3}, expect=201)
    order = advance_until(base, lambda: (lambda o: o if len(o) == 3 else None)(received(base, "sig")))
    check("重发 sig#3 后按 1,2,3 送达", order is not None and [x["seq"] for x in order] == [1, 2, 3])


def scenario_idle_is_free(base: str) -> None:
    print("\n== 场景 4：跨过空闲时段不产生处理量 ==")
    call(base, "POST", "/api/flows", {"flow_id": "idle", "link": "B"}, expect=201)
    # Skip a long idle window first: no events, almost nothing to process.
    jump = advance(base, 120_000)
    check("空闲快进不处理任何事件", jump["events_processed"] == 0 and jump["sim_time_ms"] == 120_000)
    call(base, "POST", "/api/flows/idle/packets", {"seq": 1}, expect=201)
    # B: delivery 150ms. Idle time must not add to the packet's latency.
    advance(base, 150)
    order = received(base, "idle")
    check("处理量只取决于链路延时（120150ms 送达）", len(order) == 1 and order[0]["received_at_ms"] == 120_150)


def _run_timeline(base: str, steps: list[int]) -> dict:
    """Run one fixed operation sequence, advancing by the given step plan.

    The same plan is applied twice so the total simulated time is the same
    regardless of how the advances are split.
    """
    call(base, "POST", "/api/reset", expect=200)
    call(base, "PATCH", "/api/links/A", {"delivery_delay_ms": 300, "ack_delay_ms": 200}, expect=200)
    call(base, "PATCH", "/api/links/B", {"delivery_delay_ms": 150, "ack_delay_ms": 80}, expect=200)
    call(base, "POST", "/api/flows", {"flow_id": "eq", "link": "A"}, expect=201)
    call(base, "POST", "/api/flows/eq/packets", {"seq": 1}, expect=201)
    call(base, "POST", "/api/flows/eq/packets", {"seq": 2}, expect=201)
    _, rm = call(
        base, "POST", "/api/removals",
        # Strictly wider than the second snapshot ACK (t=1000) so this run
        # exercises the success path; the exact-deadline case has its own test.
        {"member_link": "A", "wait_ms": 1200, "buffer_limit": 5}, expect=202,
    )
    rid = rm["removal_id"]
    call(base, "POST", "/api/flows/eq/packets", {"seq": 3}, expect=202)

    # The first pass covers the removal window (snapshot ACKs at t=1000).
    for step in steps:
        advance(base, step)
    # The second pass gives the flushed packet time to cross link B.
    for step in steps:
        advance(base, step)

    detail = removal(base, rid)
    order = received(base, "eq")
    return {
        "removal": {
            k: detail[k]
            for k in (
                "status", "reason", "snapshot_acked", "moved_flow_ids",
                "flushed_to", "deadline_ms", "created_at_ms", "finished_at_ms",
                "late_confirmations_ignored",
            )
        },
        "received": [
            {"seq": x["seq"], "link": x["link"], "received_at_ms": x["received_at_ms"]}
            for x in order
        ],
        "clock": clock_ms(base),
    }


def scenario_equivalence(base: str) -> None:
    print("\n== 场景 5：分段推进与一次推进结果一致 ==")
    segmented = _run_timeline(base, [100] * 10)
    one_shot = _run_timeline(base, [1000])
    check("摘链结果一致", segmented["removal"] == one_shot["removal"], json.dumps(segmented["removal"]))
    check(
        "收包顺序与模拟收包时刻一致",
        segmented["received"] == one_shot["received"],
        f"{segmented['received']} vs {one_shot['received']}",
    )
    check("最终模拟时刻一致", segmented["clock"] == one_shot["clock"] == 2000)
    check("摘链成功且缓冲包走 B", segmented["removal"]["status"] == "succeeded")
    check(
        "缓冲包模拟送达时刻为 1150ms",
        segmented["received"] == [
            {"seq": 1, "link": "A", "received_at_ms": 300},
            {"seq": 2, "link": "A", "received_at_ms": 800},
            {"seq": 3, "link": "B", "received_at_ms": 1150},
        ],
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://localhost:8001")
    parser.add_argument(
        "--only",
        action="append",
        choices=["clock", "success", "timeout", "buffer", "idle", "equivalent"],
        help="run only the named scenario(s); default: all",
    )
    args = parser.parse_args()
    base = args.base_url.rstrip("/")

    try:
        call(base, "GET", "/healthz", expect=200)
    except Exception as exc:  # noqa: BLE001
        print(f"cannot reach {base}: {exc}", file=sys.stderr)
        return 2

    _, clock = call(base, "GET", "/api/clock", expect=200)
    if clock["clock_mode"] != "virtual":
        print(
            f"service at {base} runs clock_mode={clock['clock_mode']!r}; start it with "
            "DRILL_CLOCK_MODE=virtual (e.g. docker compose --profile virtual up -d drill-virtual)",
            file=sys.stderr,
        )
        return 2

    scenarios = {
        "clock": scenario_clock_only_moves_on_advance,
        "success": scenario_success,
        "timeout": scenario_timeout_exact_deadline,
        "buffer": scenario_buffer,
        "idle": scenario_idle_is_free,
        "equivalent": scenario_equivalence,
    }
    names = args.only or list(scenarios)
    for name in names:
        reset(base)
        scenarios[name](base)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} check(s) FAILED: {FAILURES}")
        return 1
    print(f"all checks passed ({len(names)} scenario(s))")
    return 0


if __name__ == "__main__":
    sys.exit(main())
