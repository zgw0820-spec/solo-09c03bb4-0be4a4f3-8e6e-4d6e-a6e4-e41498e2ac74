from __future__ import annotations

import time

import pytest
from fastapi.testclient import TestClient

from app.config import EngineConfig, LinkConfig
from app.main import create_app


def make_client(a_delivery=60, a_ack=30, b_delivery=30, b_ack=15) -> TestClient:
    config = EngineConfig(
        link_a=LinkConfig("A", a_delivery, a_ack),
        link_b=LinkConfig("B", b_delivery, b_ack),
    )
    return TestClient(create_app(config))


def make_virtual_client(
    a_delivery=300, a_ack=200, b_delivery=150, b_ack=80
) -> TestClient:
    config = EngineConfig(
        link_a=LinkConfig("A", a_delivery, a_ack),
        link_b=LinkConfig("B", b_delivery, b_ack),
        clock_mode="virtual",
    )
    return TestClient(create_app(config))


def wait_for(client: TestClient, predicate, timeout_s: float = 8.0, interval_s: float = 0.02):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval_s)
    return False


def removal(client: TestClient, removal_id: str) -> dict:
    response = client.get(f"/api/removals/{removal_id}")
    assert response.status_code == 200, response.text
    return response.json()


def received(client: TestClient, flow_id: str) -> list[dict]:
    response = client.get(f"/api/flows/{flow_id}/received")
    assert response.status_code == 200, response.text
    return response.json()["delivery_order"]


def create_flow(client: TestClient, flow_id: str, link: str) -> dict:
    response = client.post("/api/flows", json={"flow_id": flow_id, "link": link})
    assert response.status_code == 201, response.text
    return response.json()


def send(client: TestClient, flow_id: str, seq: int, payload=None, expect=201):
    response = client.post(
        f"/api/flows/{flow_id}/packets", json={"seq": seq, "payload": payload}
    )
    assert response.status_code == expect, response.text
    return response.json()


@pytest.fixture()
def client():
    with make_client() as test_client:
        yield test_client


def test_health_and_link_defaults(client):
    assert client.get("/healthz").json() == {"status": "ok"}
    links = {link["id"]: link for link in client.get("/api/links").json()["links"]}
    assert links["A"]["delivery_delay_ms"] == 60
    assert links["B"]["ack_delay_ms"] == 15


def test_fifo_in_order_delivery_and_duplicate_rejection(client):
    create_flow(client, "f1", "A")
    for seq in range(1, 6):
        send(client, "f1", seq)
    # Duplicate seq is not accepted; no extra copy enters the pipe.
    send(client, "f1", 3, expect=409)

    assert wait_for(client, lambda: len(received(client, "f1")) == 5)
    order = [item["seq"] for item in received(client, "f1")]
    assert order == [1, 2, 3, 4, 5]
    assert all(item["link"] == "A" for item in received(client, "f1"))
    assert [item["delivery_index"] for item in received(client, "f1")] == [1, 2, 3, 4, 5]

    flow = client.get("/api/flows/f1").json()
    assert flow["accepted_count"] == 5
    assert flow["count"] == 5


def test_removal_success_moves_flows_and_buffered_packets(client):
    create_flow(client, "f-a", "A")
    create_flow(client, "f-b", "B")
    # Two packets already queued on the member link must be confirmed first.
    send(client, "f-a", 1)
    send(client, "f-a", 2)

    response = client.post(
        "/api/removals",
        json={"member_link": "A", "wait_ms": 5000, "buffer_limit": 5},
    )
    assert response.status_code == 202, response.text
    body = response.json()
    assert body["status"] == "pending"
    assert body["snapshot_packet_count"] == 2
    removal_id = body["removal_id"]

    # Second concurrent request is refused.
    busy = client.post(
        "/api/removals",
        json={"member_link": "B", "wait_ms": 1000, "buffer_limit": 1},
    )
    assert busy.status_code == 409

    # New packets for the affected flow are buffered (202), other flows flow on.
    buffered = send(client, "f-a", 3, expect=202)
    assert buffered["buffered"] is True
    send(client, "f-b", 1)

    late = client.post(
        "/api/removals",
        json={"member_link": "A", "wait_ms": 1000, "buffer_limit": 1},
    )
    assert late.status_code == 409  # still pending

    assert wait_for(
        client, lambda: removal(client, removal_id)["status"] == "succeeded"
    )
    detail = removal(client, removal_id)
    assert detail["result"] == "succeeded"
    assert detail["reason"] == "peer_confirmed"
    assert detail["snapshot_acked"] == 2
    assert detail["moved_flow_ids"] == ["f-a"]
    assert len(detail["flushed_packet_ids"]) == 1
    assert detail["flushed_to"] == "B"

    assert wait_for(client, lambda: len(received(client, "f-a")) == 3)
    order = received(client, "f-a")
    assert [item["seq"] for item in order] == [1, 2, 3]
    assert [item["link"] for item in order] == ["A", "A", "B"]

    assert wait_for(client, lambda: len(received(client, "f-b")) == 1)
    assert received(client, "f-b")[0]["link"] == "B"


def test_buffer_limit_rejects_but_other_flows_continue(client):
    create_flow(client, "f-a", "A")
    create_flow(client, "f-other", "B")
    send(client, "f-a", 1)  # keeps the removal pending for ~90ms

    response = client.post(
        "/api/removals",
        json={"member_link": "A", "wait_ms": 5000, "buffer_limit": 1},
    )
    removal_id = response.json()["removal_id"]

    first = send(client, "f-a", 2, expect=202)
    assert first["buffered"] is True
    rejected = client.post("/api/flows/f-a/packets", json={"seq": 3, "payload": None})
    assert rejected.status_code == 429
    assert rejected.json()["buffer_limit"] == 1
    assert rejected.json()["buffered_count"] == 1

    # Rejected seq was never accepted, so it can be retried after the removal.
    send(client, "f-other", 1)
    assert wait_for(
        client, lambda: removal(client, removal_id)["status"] == "succeeded"
    )
    detail = removal(client, removal_id)
    assert detail["rejected_count"] == 1
    # send_packet ordering: accepted packet 2 is flushed; seq 3 can now be sent
    send(client, "f-a", 3)
    assert wait_for(client, lambda: len(received(client, "f-a")) == 3)
    assert [item["seq"] for item in received(client, "f-a")] == [1, 2, 3]


def test_timeout_keeps_flows_and_flushes_buffer_to_original_link(client):
    create_flow(client, "f-a", "A")
    create_flow(client, "f-b", "B")
    send(client, "f-a", 1)  # takes 90ms on A

    response = client.post(
        "/api/removals",
        json={"member_link": "A", "wait_ms": 25, "buffer_limit": 4},
    )
    removal_id = response.json()["removal_id"]
    send(client, "f-a", 2, expect=202)
    send(client, "f-a", 3, expect=202)

    assert wait_for(
        client, lambda: removal(client, removal_id)["status"] == "timed_out"
    )
    detail = removal(client, removal_id)
    assert detail["result"] == "timed_out"
    assert detail["moved_flow_ids"] == []
    assert len(detail["flushed_packet_ids"]) == 2
    assert detail["flushed_to"] == "A"

    # Late acknowledgement arrives after the timeout and must not change it.
    assert wait_for(client, lambda: len(received(client, "f-a")) == 3)
    detail = removal(client, removal_id)
    assert detail["status"] == "timed_out"
    assert detail["late_confirmations_ignored"] >= 1
    assert detail["moved_flow_ids"] == []

    flow = client.get("/api/flows/f-a").json()
    assert flow["link"] == "A"
    order = received(client, "f-a")
    assert [item["seq"] for item in order] == [1, 2, 3]
    assert [item["link"] for item in order] == ["A", "A", "A"]

    # Duplicate seq after timeout is still rejected: the buffered copy was kept.
    send(client, "f-a", 2, expect=409)

    # A new removal after the timeout is allowed. Whether it is confirmed
    # immediately or has to wait depends on what is still queued on A.
    new = client.post(
        "/api/removals", json={"member_link": "A", "wait_ms": 5000, "buffer_limit": 1}
    )
    assert new.status_code in (200, 202), new.text
    new_id = new.json()["removal_id"]
    assert wait_for(
        client, lambda: removal(client, new_id)["status"] == "succeeded"
    )
    assert client.get("/api/flows/f-a").json()["link"] == "B"


def test_send_to_unknown_flow_and_bad_payloads(client):
    assert client.post("/api/flows/x/packets", json={"seq": 1}).status_code == 404
    assert (
        client.post("/api/flows", json={"flow_id": "f", "link": "C"}).status_code == 422
    )
    create_flow(client, "f", "A")
    assert client.post("/api/flows", json={"flow_id": "f", "link": "A"}).status_code == 409
    assert client.post("/api/flows/f/packets", json={"seq": 0}).status_code == 422
    assert client.patch("/api/links/Z", json={"ack_delay_ms": 5}).status_code == 404


def test_link_delay_update_takes_effect_for_new_packets(client):
    assert client.patch(
        "/api/links/A", json={"delivery_delay_ms": 5, "ack_delay_ms": 5}
    ).status_code == 200
    create_flow(client, "f", "A")
    started = time.monotonic()
    send(client, "f", 1)
    assert wait_for(client, lambda: len(received(client, "f")) == 1, timeout_s=1.0)
    assert time.monotonic() - started < 0.5


def test_flow_cannot_be_created_on_member_link_while_pending(client):
    create_flow(client, "f-a", "A")
    send(client, "f-a", 1)
    client.post("/api/removals", json={"member_link": "A", "wait_ms": 5000, "buffer_limit": 2})
    denied = client.post("/api/flows", json={"flow_id": "new", "link": "A"})
    assert denied.status_code == 409
    allowed = client.post("/api/flows", json={"flow_id": "new-b", "link": "B"})
    assert allowed.status_code == 201


def test_successful_removal_takes_member_link_out_of_bundle(client):
    create_flow(client, "f-a", "A")
    send(client, "f-a", 1)
    response = client.post(
        "/api/removals", json={"member_link": "A", "wait_ms": 5000, "buffer_limit": 2}
    )
    removal_id = response.json()["removal_id"]
    assert wait_for(
        client, lambda: removal(client, removal_id)["status"] == "succeeded"
    )
    links = {link["id"]: link for link in client.get("/api/links").json()["links"]}
    assert links["A"]["removed"] is True
    assert links["B"]["removed"] is False

    assert client.post("/api/flows", json={"flow_id": "x", "link": "A"}).status_code == 409
    again = client.post(
        "/api/removals", json={"member_link": "A", "wait_ms": 1000, "buffer_limit": 2}
    )
    assert again.status_code == 409

    # Removing the surviving member leaves no target, and is refused.
    last = client.post(
        "/api/removals", json={"member_link": "B", "wait_ms": 1000, "buffer_limit": 2}
    )
    assert last.status_code == 409

    # Reset brings both links back.
    assert client.post("/api/reset").status_code == 200
    links = {link["id"]: link for link in client.get("/api/links").json()["links"]}
    assert links["A"]["removed"] is False
    assert client.post("/api/flows", json={"flow_id": "again", "link": "A"}).status_code == 201


def test_reset_clears_flows_and_removals(client):
    create_flow(client, "f", "A")
    send(client, "f", 1)
    client.post("/api/removals", json={"member_link": "A", "wait_ms": 5000, "buffer_limit": 2})
    client.post("/api/reset")
    assert client.get("/api/flows").json()["flows"] == []
    assert client.get("/api/removals").json()["removals"] == []
    state = client.get("/api/state").json()
    assert state["active_removal"] is None
    assert all(link["queue_depth"] == 0 for link in state["links"])


def test_buffer_is_per_request_and_released_at_the_end(client):
    create_flow(client, "f-a", "A")
    send(client, "f-a", 1)
    removal_id = client.post(
        "/api/removals", json={"member_link": "A", "wait_ms": 5000, "buffer_limit": 2}
    ).json()["removal_id"]
    send(client, "f-a", 2, expect=202)
    send(client, "f-a", 3, expect=202)
    assert wait_for(
        client, lambda: removal(client, removal_id)["status"] == "succeeded"
    )
    # Both buffered packets were released after the switch, so the buffer is
    # empty again and new packets queue normally on the new link.
    send(client, "f-a", 4)
    assert wait_for(client, lambda: len(received(client, "f-a")) == 4)
    assert [x["seq"] for x in received(client, "f-a")] == [1, 2, 3, 4]
    assert [x["link"] for x in received(client, "f-a")] == ["A", "B", "B", "B"]


def test_static_ui_is_served(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "聚合口摘链演练" in response.text
    assert "https://" not in response.text  # no external assets needed offline


# ---------------------------------------------------------------- virtual clock


@pytest.fixture()
def virtual_client():
    with make_virtual_client() as test_client:
        yield test_client


def advance(client: TestClient, ms: int) -> dict:
    response = client.post("/api/clock/advance", json={"ms": ms})
    assert response.status_code == 200, response.text
    return response.json()


def test_virtual_clock_only_moves_on_advance(virtual_client):
    client = virtual_client
    clock = client.get("/api/clock").json()
    assert clock["clock_mode"] == "virtual"
    assert clock["sim_time_ms"] == 0

    create_flow(client, "f", "A")

    # An idle window with nothing queued processes no events at all.
    idle = advance(client, 120_000)
    assert idle["events_processed"] == 0
    assert idle["sim_time_ms"] == 120_000

    # Packets queued after the idle jump still pay only the link delay.
    send(client, "f", 1)
    send(client, "f", 2)
    assert client.get("/api/clock").json()["sim_time_ms"] == 120_000
    assert received(client, "f") == []

    body = advance(client, 300)
    assert body["sim_time_ms"] == 120_300
    order = received(client, "f")
    assert [item["seq"] for item in order] == [1]
    assert order[0]["received_at_ms"] == 120_300
    # The second packet is still waiting behind the first on the FIFO link.
    assert len(received(client, "f")) == 1


def test_virtual_fast_forward_removal_success_and_flush(virtual_client):
    client = virtual_client
    create_flow(client, "voice", "A")
    create_flow(client, "video", "B")
    send(client, "voice", 1)
    send(client, "voice", 2)

    rm = client.post(
        "/api/removals",
        json={"member_link": "A", "wait_ms": 5000, "buffer_limit": 5},
    )
    assert rm.status_code == 202
    removal_id = rm.json()["removal_id"]
    assert rm.json()["deadline_ms"] == 5000
    send(client, "voice", 3, expect=202)
    send(client, "video", 1)

    # A: pkt1 deliver 300 / ack 500, pkt2 deliver 800 / ack 1000.
    advance(client, 1000)
    detail = removal(client, removal_id)
    assert detail["status"] == "succeeded"
    assert detail["reason"] == "peer_confirmed"
    assert detail["finished_at_ms"] == 1000
    assert detail["moved_flow_ids"] == ["voice"]
    assert detail["flushed_to"] == "B"
    assert len(detail["flushed_packet_ids"]) == 1

    # pkt3 flushed at t=1000 onto B: delivery 150 -> received at 1150.
    advance(client, 150)
    order = received(client, "voice")
    assert [item["seq"] for item in order] == [1, 2, 3]
    assert [item["link"] for item in order] == ["A", "A", "B"]
    assert [item["received_at_ms"] for item in order] == [300, 800, 1150]


def test_virtual_confirmation_on_exact_deadline_times_out_and_is_late(virtual_client):
    client = virtual_client
    create_flow(client, "f-a", "A")
    send(client, "f-a", 1)  # delivered 300, acked exactly at 500

    rm = client.post(
        "/api/removals",
        json={"member_link": "A", "wait_ms": 500, "buffer_limit": 4},
    )
    removal_id = rm.json()["removal_id"]
    send(client, "f-a", 2, expect=202)
    send(client, "f-a", 3, expect=202)

    body = advance(client, 500)
    assert body["sim_time_ms"] == 500
    detail = removal(client, removal_id)
    assert detail["status"] == "timed_out"
    assert detail["reason"] == "wait_timeout"
    assert detail["moved_flow_ids"] == []
    assert detail["flushed_to"] == "A"
    # Timeout (priority 0) beats the same-instant ACK, which is then recorded
    # as a late confirmation instead of completing the removal.
    assert detail["late_confirmations_ignored"] == 1
    assert detail["snapshot_acked"] == 0
    assert client.get("/api/flows/f-a").json()["link"] == "A"

    # Buffered packets went back to A at t=500. FIFO: pkt2 delivered at 800,
    # then its ack at 1000 frees the link, so pkt3 is delivered at 1300.
    advance(client, 600)
    order = received(client, "f-a")
    assert [item["seq"] for item in order] == [1, 2]
    assert all(item["link"] == "A" for item in order)
    advance(client, 300)
    order = received(client, "f-a")
    assert [item["seq"] for item in order] == [1, 2, 3]
    assert [item["received_at_ms"] for item in order] == [300, 800, 1300]
    # The late confirmation never changes the final result.
    assert removal(client, removal_id)["status"] == "timed_out"


def test_virtual_split_advance_matches_one_shot_advance(virtual_client):
    client = virtual_client

    def run(advances):
        client.post("/api/reset")
        client.patch("/api/links/A", json={"delivery_delay_ms": 300, "ack_delay_ms": 200})
        client.patch("/api/links/B", json={"delivery_delay_ms": 150, "ack_delay_ms": 80})
        create_flow(client, "eq", "A")
        send(client, "eq", 1)
        send(client, "eq", 2)
        rid = client.post(
            "/api/removals",
            json={"member_link": "A", "wait_ms": 1200, "buffer_limit": 5},
        ).json()["removal_id"]
        send(client, "eq", 3, expect=202)
        for ms in advances:
            advance(client, ms)

        detail = removal(client, rid)
        order = received(client, "eq")
        return {
            "status": detail["status"],
            "reason": detail["reason"],
            "finished_at_ms": detail["finished_at_ms"],
            "flushed_to": detail["flushed_to"],
            "late": detail["late_confirmations_ignored"],
            "received": [
                (item["seq"], item["link"], item["received_at_ms"]) for item in order
            ],
            "clock": client.get("/api/clock").json()["sim_time_ms"],
        }

    segmented = run([250, 250, 250, 250])
    one_shot = run([1000])
    assert segmented == one_shot
    assert segmented["status"] == "succeeded"
    assert segmented["received"] == [(1, "A", 300), (2, "A", 800)]
    # pkt3 is flushed at t=1000 onto B once the snapshot is confirmed; it
    # needs another advance beyond the 1000ms window to be delivered.
    assert segmented["clock"] == 1000


def test_virtual_split_advance_matches_one_shot_on_exact_timeout(virtual_client):
    """The deadline-tie rule must be identical no matter how time is split."""
    client = virtual_client

    def run(advances):
        client.post("/api/reset")
        client.patch("/api/links/A", json={"delivery_delay_ms": 300, "ack_delay_ms": 200})
        create_flow(client, "tm", "A")
        send(client, "tm", 1)  # acked exactly at 500
        rid = client.post(
            "/api/removals",
            json={"member_link": "A", "wait_ms": 500, "buffer_limit": 5},
        ).json()["removal_id"]
        send(client, "tm", 2, expect=202)
        for ms in advances:
            advance(client, ms)
        detail = removal(client, rid)
        return {
            "status": detail["status"],
            "reason": detail["reason"],
            "late": detail["late_confirmations_ignored"],
            "flushed_to": detail["flushed_to"],
            "finished_at_ms": detail["finished_at_ms"],
        }

    segmented = run([100, 200, 200])
    one_shot = run([500])
    assert segmented == one_shot
    assert segmented == {
        "status": "timed_out",
        "reason": "wait_timeout",
        "late": 1,
        "flushed_to": "A",
        "finished_at_ms": 500,
    }


def test_virtual_reset_returns_clock_to_zero(virtual_client):
    client = virtual_client
    create_flow(client, "f", "A")
    send(client, "f", 1)
    advance(client, 400)
    assert client.get("/api/clock").json()["sim_time_ms"] == 400

    body = client.post("/api/reset").json()
    assert body["clock"]["sim_time_ms"] == 0
    assert body["clock"]["clock_mode"] == "virtual"
    assert client.get("/api/clock").json()["sim_time_ms"] == 0
    assert client.get("/api/flows").json()["flows"] == []
    assert client.get("/api/removals").json()["removals"] == []
    state = client.get("/api/state").json()
    assert state["clock"]["sim_time_ms"] == 0
    assert all(link["queue_depth"] == 0 for link in state["links"])


def test_virtual_buffer_limit_rejects(virtual_client):
    client = virtual_client
    create_flow(client, "f-a", "A")
    send(client, "f-a", 1)
    client.post(
        "/api/removals",
        json={"member_link": "A", "wait_ms": 5000, "buffer_limit": 1},
    )
    send(client, "f-a", 2, expect=202)
    rejected = client.post("/api/flows/f-a/packets", json={"seq": 3})
    assert rejected.status_code == 429
    assert rejected.json()["buffer_limit"] == 1


def test_realtime_mode_rejects_advance_and_exposes_clock(client):
    clock = client.get("/api/clock").json()
    assert clock["clock_mode"] == "realtime"
    assert clock["advancing"] is False
    denied = client.post("/api/clock/advance", json={"ms": 100})
    assert denied.status_code == 409
    assert denied.json()["clock_mode"] == "realtime"


def test_virtual_advance_validates_payload(virtual_client):
    client = virtual_client
    assert client.post("/api/clock/advance", json={"ms": -1}).status_code == 422
    over = client.post("/api/clock/advance", json={"ms": 3_600_001})
    assert over.status_code == 422
    zero = client.post("/api/clock/advance", json={"ms": 0})
    assert zero.status_code == 200
    assert zero.json()["sim_time_ms"] == 0
