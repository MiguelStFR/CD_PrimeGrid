"""Bully manual com transporte injetável para testes sem rede."""

import asyncio
from collections.abc import Awaitable, Callable

from primegrid.events import event


class Bully:
    def __init__(
        self, node_id: int, peers: list[int],
        send_election: Callable[[int], Awaitable[bool]],
        announce: Callable[[int], Awaitable[None]],
        on_leader: Callable[[int], None], timeout: float = 6,
    ):
        self.node_id = node_id
        self.peers = peers
        self.send_election = send_election
        self.announce = announce
        self.on_leader = on_leader
        self.timeout = timeout
        self.leader_id: int | None = None
        self._announcement = asyncio.Event()
        self._lock = asyncio.Lock()
        self._tasks: set[asyncio.Task] = set()

    def trigger(self) -> None:
        if not self._lock.locked() and not self._tasks:
            task = asyncio.create_task(self.run())
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

    def receive_election(self, candidate: int) -> bool:
        if candidate not in self.peers or candidate >= self.node_id:
            return False
        event(self.node_id, "election_ack", candidate=candidate)
        self.trigger()
        return True

    def receive_coordinator(self, node_id: int) -> bool:
        if node_id != self.node_id and node_id not in self.peers:
            return False
        if node_id < self.node_id:
            self.trigger()
            return False
        self.leader_id = node_id
        self.on_leader(node_id)
        self._announcement.set()
        event(self.node_id, "coordinator", leader_id=node_id)
        return True

    async def run(self) -> None:
        if self._lock.locked():
            return
        async with self._lock:
            while True:
                self._announcement.clear()
                event(self.node_id, "election_started")
                higher = [peer for peer in self.peers if peer > self.node_id]
                replies = await asyncio.gather(*(self.send_election(peer) for peer in higher))
                if not any(replies):
                    self.receive_coordinator(self.node_id)
                    await self.announce(self.node_id)
                    return
                try:
                    await asyncio.wait_for(self._announcement.wait(), self.timeout)
                    return
                except TimeoutError:
                    event(self.node_id, "election_timeout")

    async def close(self) -> None:
        for task in tuple(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
