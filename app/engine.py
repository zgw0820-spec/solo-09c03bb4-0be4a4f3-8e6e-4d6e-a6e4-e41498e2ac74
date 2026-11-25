from __future__ import annotations

import asyncio
import heapq
import itertools
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Callable

LINK_IDS = ("A", "B")
CLOCK_MODES = ("realtime", "virtual")

# Virtual clock renders its monotonic milliseconds as ISO timestamps starting
# from a fixed epoch, so drill output still shows recognizable UTC strings.
VIRTUAL_EPOCH = datetime(2025, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
MAX_ADVANCE_MS = 3_600_000  # one simulated hour per fast-forward call


def utcnow() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


class DrillError(Exception):
    """Domain error with an HTTP status and optional structured context."""

    def __init__(self, status_code: int, message: str, context: dict | None = None):
        super().__init__(message)
        self.status_code = status_code
        self.message = message
        self.context = context or {}


@dataclass
class LinkState:
    id: str
    delivery_delay_ms: int
    ack_delay_ms: int
    queue: list["Packet"] = field(default_factory=list)
    worker: asyncio.Task | None = None
    enqueued: int = 0
    delivered: int = 0
    acked: int = 0
    current_packet_id: str | None = None
    current_phase: str = "idle"
    removed: bool = False
    # Simulated time (ms) at which the in-flight packet finishes each phase.
    delivery_due_ms: int | None = None
    ack_due_ms: int | None = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "delivery_delay_ms": self.delivery_delay_ms,
            "ack_delay_ms": self.ack_delay_ms,
            "queue_depth": len(self.queue),
            "enqueued": self.enqueued,
            "delivered": self.delivered,
            "acked": self.acked,
            "current_packet_id": self.current_packet_id,
            "current_phase": self.current_phase,
            "delivery_due_ms": self.delivery_due_ms,
            "ack_due_ms": self.ack_due_ms,
            "removed": self.removed,
        }


@dataclass
class Flow:
    id: str
    link: str
    created_at: str
    created_at_ms: int
    accepted_count: int = 0
    seen_seqs: set[int] = field(default_factory=set)
    received: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "link": self.link,
            "created_at": self.created_at,
            "created_at_ms": self.created_at_ms,
            "accepted_count": self.accepted_count,
            "received_count": len(self.received),
        }


@dataclass
class Packet:
    id: str
    flow_id: str
    seq: int
    payload: Any
    accepted_at: str
    accept_index: int
    accepted_at_ms: int
    status: str = "queued"  # queued | delivering | delivered | acked | buffered
    link: str | None = None
    delivered_at: str | None = None
    delivered_at_ms: int | None = None
    acked_at: str | None = None
    acked_at_ms: int | None = None
    flow_delivery_index: int | None = None
    buffered_for_removal: str | None = None

    def to_dict(self) -> dict:
        return {
            "packet_id": self.id,
            "flow_id": self.flow_id,
            "seq": self.seq,
            "status": self.status,
            "link": self.link,
            "accepted_at": self.accepted_at,
            "accepted_at_ms": self.accepted_at_ms,
            "delivered_at": self.delivered_at,
            "delivered_at_ms": self.delivered_at_ms,
            "acked_at": self.acked_at,
            "acked_at_ms": self.acked_at_ms,
            "flow_delivery_index": self.flow_delivery_index,
            "buffered_for_removal": self.buffered_for_removal,
            "payload": self.payload,
        }


@dataclass
class RemovalRequest:
    id: str
    member_link: str
    target_link: str
    wait_ms: int
    buffer_limit: int
    created_at: str
    created_at_ms: int
    deadline_ms: int
    status: str = "pending"  # pending | succeeded | timed_out
    finished_at: str | None = None
    finished_at_ms: int | None = None
    result: str | None = None
    reason: str | None = None
    affected_flow_ids: list[str] = field(default_factory=list)
    snapshot_packet_ids: list[str] = field(default_factory=list)
    snapshot_acked: int = 0
    buffered_counts: dict[str, int] = field(default_factory=dict)
    buffered_packet_ids: list[str] = field(default_factory=list)
    flushed_packet_ids: list[str] = field(default_factory=list)
    flushed_to: str | None = None
    rejected_count: int = 0
    late_confirmations_ignored: int = 0
    late_confirmation_packet_ids: list[str] = field(default_factory=list)
    moved_flow_ids: list[str] = field(default_factory=list)
    timeout_task: asyncio.Task | None = field(default=None, repr=False)
    timeout_seq: int | None = field(default=None, repr=False)
    events: list[dict] = field(default_factory=list)

    def add_event(self, engine: "DrillEngine", event_type: str, **detail: Any) -> None:
        self.events.append(
            {"at": engine.now_iso, "at_ms": engine.now_ms, "type": event_type, **detail}
        )

    def to_dict(self) -> dict:
        return {
            "removal_id": self.id,
            "status": self.status,
            "result": self.result,
            "reason": self.reason,
            "member_link": self.member_link,
            "target_link": self.target_link,
            "wait_ms": self.wait_ms,
            "buffer_limit": self.buffer_limit,
            "buffer_used": len(self.buffered_packet_ids),
            "created_at": self.created_at,
            "created_at_ms": self.created_at_ms,
            "deadline_ms": self.deadline_ms,
            "finished_at": self.finished_at,
            "finished_at_ms": self.finished_at_ms,
            "affected_flow_ids": self.affected_flow_ids,
            "moved_flow_ids": self.moved_flow_ids,
            "snapshot_packet_ids": self.snapshot_packet_ids,
            "snapshot_packet_count": len(self.snapshot_packet_ids),
            "snapshot_acked": self.snapshot_acked,
            "buffered_packet_ids": self.buffered_packet_ids,
            "buffered_counts": self.buffered_counts,
            "flushed_packet_ids": self.flushed_packet_ids,
            "flushed_to": self.flushed_to,
            "rejected_count": self.rejected_count,
            "late_confirmations_ignored": self.late_confirmations_ignored,
            "late_confirmation_packet_ids": self.late_confirmation_packet_ids,
            "events": self.events,
        }

    def to_summary(self) -> dict:
        data = self.to_dict()
        data.pop("events", None)
        data.pop("late_confirmation_packet_ids", None)
        return data


class DrillEngine:
    """In-memory drill for removing one member link from a two-member bundle.

    Links A and B are strict FIFO pipes. Each packet is delivered after the
    link's delivery delay and acknowledged after the link's ack delay; a link
    processes one packet at a time. The peer's "catch-up confirmation" for a
    removal is derived from the ACK of every packet that was already queued on
    the member link when the removal request was created.

    Two clock modes share one implementation:

    * ``realtime`` (default): phases are driven by asyncio tasks sleeping for
      the configured delays, exactly like a wall-clock simulation.
    * ``virtual``: nothing happens on its own. The clock only advances through
      :meth:`advance_clock`, which runs every delivery/ack/timeout event whose
      scheduled time is within the target simulated time. Events at the same
      simulated time are processed in a fixed deterministic order, so splitting
      an advance into pieces never changes the outcome.
    """

    def __init__(
        self,
        link_a_delivery_ms: int = 300,
        link_a_ack_ms: int = 200,
        link_b_delivery_ms: int = 150,
        link_b_ack_ms: int = 80,
        clock_mode: str = "realtime",
    ):
        if clock_mode not in CLOCK_MODES:
            raise ValueError(f"clock_mode must be one of {CLOCK_MODES}, got {clock_mode!r}")
        self.clock_mode = clock_mode
        self._lock = asyncio.Lock()
        self._defaults = {
            "A": (link_a_delivery_ms, link_a_ack_ms),
            "B": (link_b_delivery_ms, link_b_ack_ms),
        }
        self.links: dict[str, LinkState] = {}
        self.flows: dict[str, Flow] = {}
        self.removals: dict[str, RemovalRequest] = {}
        self._removal_order: list[str] = []
        self.active_removal_id: str | None = None
        self._packets: dict[str, Packet] = {}
        self.events: list[dict] = []

        # Virtual-clock event heap (heapq entries are lists so a cancelled
        # event can be neutralised in place without breaking heap order):
        # [time_ms, priority, seq, callback].
        # Priority makes same-time ordering deterministic:
        #   0 removal timeout (a confirmation landing on the deadline is late)
        #   1 packet delivery / acknowledgement (ties broken by scheduling seq)
        self._event_heap: list[list[Any]] = []
        self._event_seq = itertools.count()
        self._realtime_tasks: set[asyncio.Task] = set()
        self._now_ms: int = 0
        self._realtime_origin = self._wall_monotonic_ms()
        self._reset_state()

    # ------------------------------------------------------------------ clock
    @staticmethod
    def _wall_monotonic_ms() -> float:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        return loop.time() * 1000

    @property
    def now_ms(self) -> int:
        if self.clock_mode == "virtual":
            return self._now_ms
        return int(self._wall_monotonic_ms() - self._realtime_origin)

    @property
    def now_iso(self) -> str:
        if self.clock_mode == "virtual":
            return (
                (VIRTUAL_EPOCH + timedelta(milliseconds=self._now_ms))
                .isoformat(timespec="milliseconds")
                .replace("+00:00", "Z")
            )
        return utcnow()

    def clock_view(self) -> dict:
        return {
            "clock_mode": self.clock_mode,
            "sim_time_ms": self.now_ms,
            "sim_time": self.now_iso,
            "advancing": self.clock_mode == "virtual",
            "scheduled_events": len(self._event_heap),
        }

    def _schedule(
        self,
        delay_ms: int,
        callback: Callable[[], None],
        priority: int = 1,
        name: str | None = None,
    ) -> tuple[int, int, asyncio.Task | None]:
        """Run ``callback`` after ``delay_ms`` simulated milliseconds.

        Returns (scheduled time, heap sequence, realtime task). In realtime
        mode the callback runs inside an asyncio task that re-takes the engine
        lock; in virtual mode it is popped from the heap during
        :meth:`advance_clock`.
        """
        due_ms = self.now_ms + max(0, int(delay_ms))
        if self.clock_mode == "virtual":
            seq = next(self._event_seq)
            heapq.heappush(
                self._event_heap, [due_ms, priority, seq, callback]
            )
            return due_ms, seq, None

        async def _runner() -> None:
            await asyncio.sleep(max(0, int(delay_ms)) / 1000)
            async with self._lock:
                callback()

        task = asyncio.create_task(_runner(), name=name or "scheduled")
        self._realtime_tasks.add(task)
        task.add_done_callback(self._realtime_tasks.discard)
        return due_ms, -1, task

    def _cancel_scheduled(self, due_ms: int | None, seq: int | None) -> None:
        if self.clock_mode != "virtual" or seq is None:
            return
        for entry in self._event_heap:
            if entry[2] == seq and entry[0] == due_ms:
                entry[3] = lambda: None  # neutralise; heap entry stays ordered
                return

    # ------------------------------------------------------------------ setup
    def _reset_state(self) -> None:
        self.links = {
            link_id: LinkState(link_id, *self._defaults[link_id]) for link_id in LINK_IDS
        }
        self.flows = {}
        self.removals = {}
        self._removal_order = []
        self.active_removal_id = None
        self._packets = {}
        self.events = []
        self._event_heap = []
        self._event_seq = itertools.count()
        self._realtime_origin = self._wall_monotonic_ms()
        self._now_ms = 0

    async def start(self) -> None:
        if self.clock_mode != "realtime":
            return
        for link in self.links.values():
            if link.worker is None or link.worker.done():
                link.worker = asyncio.create_task(
                    self._link_worker(link), name=f"link-{link.id}"
                )
                self._realtime_tasks.add(link.worker)

    async def stop(self) -> None:
        tasks: list[asyncio.Task] = []
        for link in self.links.values():
            if link.worker is not None:
                link.worker.cancel()
                tasks.append(link.worker)
                link.worker = None
        for task in list(self._realtime_tasks):
            task.cancel()
            tasks.append(task)
        for task in tasks:
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        self._realtime_tasks.clear()

    def _log(self, event_type: str, **detail: Any) -> None:
        self.events.append(
            {"at": self.now_iso, "at_ms": self.now_ms, "type": event_type, **detail}
        )
        if len(self.events) > 2000:
            del self.events[: len(self.events) - 2000]

    # ------------------------------------------------------------- fast forward
    async def advance_clock(self, ms: int) -> dict:
        if self.clock_mode != "virtual":
            raise DrillError(
                409,
                "clock fast-forward is only available when the drill starts in "
                "virtual clock mode (DRILL_CLOCK_MODE=virtual)",
                {"clock_mode": self.clock_mode},
            )
        if ms < 0:
            raise DrillError(400, "ms must be >= 0")
        if ms > MAX_ADVANCE_MS:
            raise DrillError(
                400,
                f"ms must be <= {MAX_ADVANCE_MS} per advance",
                {"max_ms": MAX_ADVANCE_MS},
            )
        processed = 0
        target_ms = self._now_ms + ms
        while self._event_heap and self._event_heap[0][0] <= target_ms:
            due_ms, _priority, _seq, callback = heapq.heappop(self._event_heap)
            # The clock only moves when explicitly advanced; idle time with no
            # events costs nothing, and every callback sees the event's time.
            self._now_ms = due_ms
            callback()
            processed += 1
        self._now_ms = target_ms
        return {
            "advanced_ms": ms,
            "sim_time_ms": self._now_ms,
            "sim_time": self.now_iso,
            "events_processed": processed,
            "scheduled_events": len(self._event_heap),
            "target_ms": target_ms,
        }

    # ------------------------------------------------------------------ links
    async def _link_worker(self, link: LinkState) -> None:
        """Realtime FIFO worker; only used in realtime clock mode."""
        while True:
            packet = await self._wait_for_packet(link)
            async with self._lock:
                link.current_packet_id = packet.id
                link.current_phase = "delivering"
                packet.status = "delivering"
                delivery_delay = link.delivery_delay_ms
                link.delivery_due_ms = self.now_ms + delivery_delay
            await asyncio.sleep(delivery_delay / 1000)

            async with self._lock:
                if not self._deliver_packet(link, packet):
                    continue
                ack_delay = link.ack_delay_ms
                link.ack_due_ms = self.now_ms + ack_delay
            await asyncio.sleep(ack_delay / 1000)

            async with self._lock:
                self._ack_packet(link, packet)

    async def _wait_for_packet(self, link: LinkState) -> "Packet":
        # LinkState.queue is a plain list so the virtual engine can share it;
        # the realtime worker polls it briefly instead of awaiting an
        # asyncio.Queue.
        while True:
            if link.queue:
                return link.queue.pop(0)
            await asyncio.sleep(0.005)

    def _pump_link(self, link: LinkState) -> None:
        """Schedule the next FIFO phase on an idle link (virtual clock)."""
        if link.removed or link.current_packet_id is not None or not link.queue:
            return
        packet = link.queue.pop(0)
        link.current_packet_id = packet.id
        link.current_phase = "delivering"
        packet.status = "delivering"
        delivery_delay = link.delivery_delay_ms
        link.delivery_due_ms = self.now_ms + delivery_delay
        self._schedule(
            delivery_delay,
            lambda l=link, p=packet: self._on_delivery_due(l, p),
        )

    def _on_delivery_due(self, link: LinkState, packet: "Packet") -> None:
        if not self._deliver_packet(link, packet):
            return
        ack_delay = link.ack_delay_ms
        link.ack_due_ms = self.now_ms + ack_delay
        self._schedule(
            ack_delay,
            lambda l=link, p=packet: self._ack_packet(l, p),
        )

    def _deliver_packet(self, link: LinkState, packet: "Packet") -> bool:
        """Mark a packet delivered. Returns False if the flow vanished."""
        flow = self.flows.get(packet.flow_id)
        if flow is None:
            link.current_packet_id = None
            link.current_phase = "idle"
            link.delivery_due_ms = None
            link.ack_due_ms = None
            if self.clock_mode == "virtual":
                self._pump_link(link)
            return False
        packet.status = "delivered"
        packet.delivered_at = self.now_iso
        packet.delivered_at_ms = self.now_ms
        flow.received.append(
            {
                "delivery_index": len(flow.received) + 1,
                "packet_id": packet.id,
                "seq": packet.seq,
                "link": packet.link,
                "received_at": packet.delivered_at,
                "received_at_ms": packet.delivered_at_ms,
            }
        )
        packet.flow_delivery_index = len(flow.received)
        link.delivered += 1
        link.current_phase = "awaiting_ack"
        return True

    def _ack_packet(self, link: LinkState, packet: "Packet") -> None:
        packet.status = "acked"
        packet.acked_at = self.now_iso
        packet.acked_at_ms = self.now_ms
        link.acked += 1
        link.current_packet_id = None
        link.current_phase = "idle"
        link.delivery_due_ms = None
        link.ack_due_ms = None
        self._on_packet_acked(packet)
        if self.clock_mode == "virtual":
            self._pump_link(link)

    async def update_link(
        self,
        link_id: str,
        delivery_delay_ms: int | None = None,
        ack_delay_ms: int | None = None,
    ) -> dict:
        if link_id not in self.links:
            raise DrillError(404, f"unknown link {link_id!r}", {"links": list(LINK_IDS)})
        async with self._lock:
            link = self.links[link_id]
            if delivery_delay_ms is not None:
                link.delivery_delay_ms = delivery_delay_ms
            if ack_delay_ms is not None:
                link.ack_delay_ms = ack_delay_ms
            self._log(
                "link_updated",
                link=link_id,
                delivery_delay_ms=link.delivery_delay_ms,
                ack_delay_ms=link.ack_delay_ms,
            )
            return link.to_dict()

    # ------------------------------------------------------------------ flows
    async def create_flow(self, flow_id: str | None, link: str) -> dict:
        if link not in LINK_IDS:
            raise DrillError(400, f"link must be one of {LINK_IDS}")
        async with self._lock:
            active = self._active_removal()
            if self.links[link].removed:
                raise DrillError(
                    409,
                    f"link {link} has been removed from the bundle; reset the drill to start over",
                    {"link": link},
                )
            if active is not None and link == active.member_link:
                raise DrillError(
                    409,
                    "cannot create a flow on the member link while it is being removed",
                    {"removal_id": active.id},
                )
            fid = flow_id or f"flow-{uuid.uuid4().hex[:8]}"
            if fid in self.flows:
                raise DrillError(409, f"flow {fid!r} already exists")
            if not isinstance(fid, str) or not fid.strip():
                raise DrillError(400, "flow_id must be a non-empty string")
            flow = Flow(
                id=fid,
                link=link,
                created_at=self.now_iso,
                created_at_ms=self.now_ms,
            )
            self.flows[fid] = flow
            self._log("flow_created", flow_id=fid, link=link)
            return flow.to_dict()

    def _get_flow(self, flow_id: str) -> Flow:
        flow = self.flows.get(flow_id)
        if flow is None:
            raise DrillError(404, f"unknown flow {flow_id!r}")
        return flow

    # ---------------------------------------------------------------- sending
    async def send_packet(self, flow_id: str, seq: int, payload: Any = None) -> dict:
        if seq <= 0:
            raise DrillError(400, "seq must be a positive integer")
        async with self._lock:
            flow = self._get_flow(flow_id)
            if seq in flow.seen_seqs:
                raise DrillError(
                    409,
                    "duplicate seq for this flow; the original packet was kept and this copy was not accepted",
                    {"flow_id": flow_id, "seq": seq},
                )

            active = self._active_removal()
            buffering = (
                active is not None
                and active.status == "pending"
                and flow_id in active.affected_flow_ids
            )

            packet = Packet(
                id=f"pkt-{uuid.uuid4().hex[:12]}",
                flow_id=flow_id,
                seq=seq,
                payload=payload,
                accepted_at=self.now_iso,
                accepted_at_ms=self.now_ms,
                accept_index=flow.accepted_count + 1,
            )

            if buffering:
                assert active is not None
                if len(active.buffered_packet_ids) >= active.buffer_limit:
                    active.rejected_count += 1
                    active.add_event(
                        self,
                        "packet_rejected",
                        flow_id=flow_id,
                        seq=seq,
                        reason="buffer_full",
                        buffered=len(active.buffered_packet_ids),
                        buffer_limit=active.buffer_limit,
                    )
                    raise DrillError(
                        429,
                        "buffer full for the pending link removal; packet rejected",
                        {
                            "removal_id": active.id,
                            "flow_id": flow_id,
                            "seq": seq,
                            "buffer_limit": active.buffer_limit,
                            "buffered_count": len(active.buffered_packet_ids),
                        },
                    )
                packet.status = "buffered"
                packet.buffered_for_removal = active.id
                active.buffered_packet_ids.append(packet.id)
                active.buffered_counts[flow_id] = (
                    active.buffered_counts.get(flow_id, 0) + 1
                )
                active.add_event(
                    self,
                    "packet_buffered",
                    packet_id=packet.id,
                    flow_id=flow_id,
                    seq=seq,
                    buffered=len(active.buffered_packet_ids),
                )
            else:
                packet.status = "queued"
                packet.link = flow.link
                self._enqueue(packet, flow.link)

            flow.seen_seqs.add(seq)
            flow.accepted_count += 1
            self._packets[packet.id] = packet
            self._log(
                "packet_accepted",
                packet_id=packet.id,
                flow_id=flow_id,
                seq=seq,
                link=packet.link,
                state=packet.status,
            )
            return {
                "accepted": True,
                "buffered": packet.status == "buffered",
                "removal_id": active.id if buffering else None,
                "packet": packet.to_dict(),
                "flow": flow.to_dict(),
            }

    def _enqueue(self, packet: Packet, link_id: str) -> None:
        link = self.links[link_id]
        packet.link = link_id
        packet.status = "queued"
        link.enqueued += 1
        link.queue.append(packet)
        if self.clock_mode == "virtual":
            self._pump_link(link)

    # ------------------------------------------------------------ peer / views
    async def received_for_flow(self, flow_id: str) -> dict:
        async with self._lock:
            flow = self._get_flow(flow_id)
            return {
                "flow_id": flow.id,
                "link": flow.link,
                "created_at": flow.created_at,
                "created_at_ms": flow.created_at_ms,
                "accepted_count": flow.accepted_count,
                "delivery_order": list(flow.received),
                "count": len(flow.received),
            }

    async def packet_detail(self, packet_id: str) -> dict:
        async with self._lock:
            packet = self._packets.get(packet_id)
            if packet is None:
                raise DrillError(404, f"unknown packet {packet_id!r}")
            return packet.to_dict()

    # --------------------------------------------------------------- removals
    def _active_removal(self) -> RemovalRequest | None:
        if self.active_removal_id is None:
            return None
        return self.removals.get(self.active_removal_id)

    async def create_removal(
        self, member_link: str, wait_ms: int, buffer_limit: int
    ) -> dict:
        if member_link not in LINK_IDS:
            raise DrillError(400, f"member_link must be one of {LINK_IDS}")
        target_link = "B" if member_link == "A" else "A"
        if wait_ms < 0:
            raise DrillError(400, "wait_ms must be >= 0")
        if buffer_limit < 0:
            raise DrillError(400, "buffer_limit must be >= 0")

        async with self._lock:
            active = self._active_removal()
            if active is not None and active.status == "pending":
                raise DrillError(
                    409,
                    "another link removal is already pending; only one at a time",
                    {"removal_id": active.id},
                )
            if self.links[member_link].removed:
                raise DrillError(
                    409,
                    f"link {member_link} is no longer part of the bundle",
                    {"link": member_link},
                )
            target = self.links[target_link]
            if target.removed:
                raise DrillError(
                    409,
                    f"target link {target_link} has been removed; nothing to move onto",
                    {"target_link": target_link},
                )

            removal = RemovalRequest(
                id=f"rm-{uuid.uuid4().hex[:10]}",
                member_link=member_link,
                target_link=target_link,
                wait_ms=wait_ms,
                buffer_limit=buffer_limit,
                created_at=self.now_iso,
                created_at_ms=self.now_ms,
                deadline_ms=self.now_ms + wait_ms,
            )
            affected = [f for f in self.flows.values() if f.link == member_link]
            removal.affected_flow_ids = [f.id for f in affected]
            # Everything accepted on the member link before this request must be
            # acknowledged by the peer before the flows may move.
            snapshot = [
                p
                for p in self._packets.values()
                if p.flow_id in removal.affected_flow_ids
                and p.status in {"queued", "delivering", "delivered"}
                and p.buffered_for_removal is None
            ]
            snapshot.sort(key=lambda p: (p.flow_id, p.accept_index))
            removal.snapshot_packet_ids = [p.id for p in snapshot]
            removal.snapshot_acked = 0
            self.removals[removal.id] = removal
            self._removal_order.append(removal.id)
            self.active_removal_id = removal.id
            removal.add_event(
                self,
                "removal_created",
                member_link=member_link,
                target_link=target_link,
                wait_ms=wait_ms,
                buffer_limit=buffer_limit,
                affected_flow_ids=list(removal.affected_flow_ids),
                snapshot_packet_ids=list(removal.snapshot_packet_ids),
            )
            self._log(
                "removal_created",
                removal_id=removal.id,
                member_link=member_link,
                snapshot_count=len(snapshot),
            )

            if all(self._packet_acked(pid) for pid in removal.snapshot_packet_ids):
                self._finish_removal(removal, "succeeded", "peer_confirmed")
            else:
                # Priority 0: if the last confirmation lands exactly on the
                # deadline, the timeout is processed first and that confirmation
                # is recorded as late.
                due_ms, seq, task = self._schedule(
                    wait_ms,
                    lambda rid=removal.id: self._removal_timeout(rid),
                    priority=0,
                    name=f"timeout-{removal.id}",
                )
                removal.deadline_ms = due_ms
                if self.clock_mode == "virtual":
                    removal.timeout_seq = seq
                else:
                    removal.timeout_task = task
            return removal.to_dict()

    def _packet_acked(self, packet_id: str) -> bool:
        packet = self._packets.get(packet_id)
        return packet is not None and packet.status == "acked"

    def _on_packet_acked(self, packet: Packet) -> None:
        for removal in self.removals.values():
            if packet.id not in removal.snapshot_packet_ids:
                continue
            if removal.status == "pending":
                removal.snapshot_acked += 1
                if all(self._packet_acked(pid) for pid in removal.snapshot_packet_ids):
                    self._finish_removal(removal, "succeeded", "peer_confirmed")
            elif removal.status == "timed_out":
                removal.late_confirmations_ignored += 1
                removal.late_confirmation_packet_ids.append(packet.id)
                removal.add_event(
                    self,
                    "late_confirmation_ignored",
                    packet_id=packet.id,
                    flow_id=packet.flow_id,
                    seq=packet.seq,
                )

    def _removal_timeout(self, removal_id: str) -> None:
        removal = self.removals.get(removal_id)
        if removal is None or removal.status != "pending":
            return
        self._finish_removal(removal, "timed_out", "wait_timeout")

    def _finish_removal(
        self, removal: RemovalRequest, status: str, reason: str
    ) -> None:
        """Finalize a removal. Must be called while holding the engine lock."""
        if removal.status != "pending":
            return
        removal.status = status
        removal.result = status
        removal.reason = reason
        removal.finished_at = self.now_iso
        removal.finished_at_ms = self.now_ms
        if self.clock_mode == "virtual":
            self._cancel_scheduled(removal.deadline_ms, removal.timeout_seq)
        elif (
            removal.timeout_task is not None
            and removal.timeout_task is not asyncio.current_task()
            and not removal.timeout_task.done()
        ):
            removal.timeout_task.cancel()
        removal.timeout_task = None
        removal.timeout_seq = None
        if self.active_removal_id == removal.id:
            self.active_removal_id = None

        if status == "succeeded":
            for flow_id in removal.affected_flow_ids:
                flow = self.flows.get(flow_id)
                if flow is None:
                    continue
                flow.link = removal.target_link
                removal.moved_flow_ids.append(flow_id)
            self.links[removal.member_link].removed = True
            flush_link = removal.target_link
        else:
            flush_link = removal.member_link

        flushed: list[Packet] = []
        for packet_id in removal.buffered_packet_ids:
            packet = self._packets.get(packet_id)
            if packet is None or packet.status != "buffered":
                continue
            flushed.append(packet)
        flushed.sort(key=lambda p: (p.flow_id, p.accept_index))
        for packet in flushed:
            packet.buffered_for_removal = None
            self._enqueue(packet, flush_link)
            removal.flushed_packet_ids.append(packet.id)

        removal.buffered_packet_ids = []
        removal.flushed_to = flush_link
        removal.add_event(
            self,
            "removal_finished",
            status=status,
            reason=reason,
            moved_flow_ids=list(removal.moved_flow_ids),
            flushed_packet_ids=list(removal.flushed_packet_ids),
            flushed_to=flush_link,
            snapshot_acked=removal.snapshot_acked,
            rejected_count=removal.rejected_count,
        )
        self._log(
            "removal_finished",
            removal_id=removal.id,
            status=status,
            reason=reason,
            moved=len(removal.moved_flow_ids),
            flushed=len(removal.flushed_packet_ids),
            flushed_to=flush_link,
        )

    # -------------------------------------------------------------- snapshots
    async def state(self) -> dict:
        async with self._lock:
            active = self._active_removal()
            return {
                "clock": self.clock_view(),
                "links": [self.links[lid].to_dict() for lid in LINK_IDS],
                "flows": [flow.to_dict() for flow in self.flows.values()],
                "active_removal": active.to_summary() if active else None,
                "removal_count": len(self.removals),
                "events": list(self.events[-100:]),
            }

    async def list_removals(self) -> list[dict]:
        async with self._lock:
            return [
                self.removals[rid].to_summary()
                for rid in reversed(self._removal_order)
            ]

    async def get_removal(self, removal_id: str) -> dict:
        async with self._lock:
            removal = self.removals.get(removal_id)
            if removal is None:
                raise DrillError(404, f"unknown removal {removal_id!r}")
            return removal.to_dict()

    async def reset(self) -> dict:
        await self.stop()
        async with self._lock:
            self._reset_state()
            self._log("system_reset")
        await self.start()
        async with self._lock:
            return {
                "reset": True,
                "clock": self.clock_view(),
                "links": [self.links[lid].to_dict() for lid in LINK_IDS],
            }
