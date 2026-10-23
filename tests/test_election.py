import asyncio

from primegrid.election import Bully


async def test_bully_elects_highest_active_id_and_rejects_lower_coordinator():
    nodes = {}
    announcements = []

    def make(node_id):
        async def send(peer):
            return peer in nodes and nodes[peer].receive_election(node_id)

        async def announce(leader):
            announcements.append(leader)
            for node in nodes.values():
                node.receive_coordinator(leader)

        return Bully(node_id, [p for p in (1, 2, 3) if p != node_id], send, announce, lambda _: None, timeout=0.1)

    nodes[1], nodes[2] = make(1), make(2)  # ID 3 configurado, porém ausente.
    try:
        await asyncio.wait_for(nodes[1].run(), 1)
        assert nodes[1].leader_id == nodes[2].leader_id == 2
        assert announcements == [2]
        assert not nodes[2].receive_coordinator(1)
        assert not nodes[1].receive_coordinator(999)
    finally:
        await asyncio.gather(*(node.close() for node in nodes.values()))


async def test_ack_without_announcement_repeats_election_after_timeout():
    calls = []

    async def send(peer):
        calls.append(peer)
        return len(calls) == 1

    async def announce(leader):
        assert leader == 1

    election = Bully(1, [2], send, announce, lambda _: None, timeout=0.01)
    await asyncio.wait_for(election.run(), 1)
    assert calls == [2, 2]
    assert election.leader_id == 1


async def test_later_higher_node_takes_over_at_startup():
    seen = []

    async def send(peer):
        return False

    async def announce(leader):
        seen.append(leader)

    election = Bully(2, [1, 3], send, announce, lambda _: None)
    await election.run()
    assert election.leader_id == 2
    assert election.receive_coordinator(3)
    assert election.leader_id == 3
