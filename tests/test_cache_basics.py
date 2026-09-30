import unittest

from tinyzj.chi import ReqOpcode, Resp, SnpOpcode
from tinyzj.zj import Zhujiang


class CacheBasicsTest(unittest.TestCase):

  def test_full_line_store_uses_make_unique_and_discards_old_dirty_data(self):
    system = Zhujiang(buffer_capacity=2, id_capacity=1, retry_enabled=True)
    system.s.dj.write("A", "memory")
    first = system.cc0.store("A", "old dirty")
    system.run_until_idle(200)
    self.assertEqual(ReqOpcode.MAKE_UNIQUE, first.opcode)

    second = system.cc1.store("A", "new dirty")
    system.run_until_idle(200)

    self.assertEqual(ReqOpcode.MAKE_UNIQUE, second.opcode)
    self.assertEqual((Resp.I, "old dirty"), system.cc0.cache["A"])
    self.assertEqual((Resp.UD, "new dirty"), system.cc1.cache["A"])
    self.assertEqual({"n11": Resp.UC}, system.hf.directory["A"])
    self.assertNotIn("A", system.hf.dirty_data)
    self.assertEqual("memory", system.s.dj.data_by_address["A"])
    self.assertTrue(any(message.opcode == SnpOpcode.SNP_MAKE_INVALID
                        for message in system.cc0.received_messages))
    system.check_invariants()

  def test_clean_unique_invalidates_peers_and_keeps_requester_data(self):
    system = Zhujiang(buffer_capacity=2, id_capacity=1, retry_enabled=True)
    system.s.dj.write("A", "value")
    system.cc0.read_shared("A")
    system.run_until_idle(200)
    system.cc1.read_shared("A")
    system.run_until_idle(200)

    request = system.cc0.clean_unique("A")
    system.run_until_idle(200)

    self.assertEqual(ReqOpcode.CLEAN_UNIQUE, request.opcode)
    self.assertEqual((Resp.UC, "value"), system.cc0.cache["A"])
    self.assertEqual((Resp.I, "value"), system.cc1.cache["A"])
    self.assertEqual({"n00": Resp.UC}, system.hf.directory["A"])
    self.assertFalse(system.hf.pending_comp_acks)
    self.assertTrue(any(message.opcode == SnpOpcode.SNP_CLEAN_INVALID
                        for message in system.cc1.received_messages))
    system.check_invariants()

  def test_clean_line_evict_updates_directory_without_writing_data(self):
    for reader, expected_state in (("read_shared", Resp.SC), ("read_unique", Resp.UC)):
      with self.subTest(reader=reader):
        system = Zhujiang()
        system.s.dj.write("A", "value")
        getattr(system.cc0, reader)("A")
        system.run_until_idle()
        self.assertEqual(expected_state, system.cc0.cache["A"][0])
        storage_messages = len(system.s.received_messages)

        request = system.cc0.evict("A")
        system.run_until_idle()

        self.assertEqual(ReqOpcode.EVICT, request.opcode)
        self.assertEqual((Resp.I, "value"), system.cc0.cache["A"])
        self.assertNotIn("A", system.hf.directory)
        self.assertEqual(storage_messages, len(system.s.received_messages))
        system.check_invariants()

  def test_partial_unique_write_merges_against_latest_dirty_line(self):
    system = Zhujiang(buffer_capacity=2, id_capacity=1, retry_enabled=True)
    memory = bytes(range(64))
    latest = bytes(reversed(range(64)))
    update = bytes([0xA5]) * 64
    mask = (1 << 1) | (1 << 32) | (1 << 63)
    expected = bytes(update[index] if mask & (1 << index) else latest[index]
                     for index in range(64))
    system.s.dj.write(0, memory)
    system.cc0.read_unique(0)
    system.run_until_idle(200)
    system.cc0.store_cached(0, latest)

    request = system.cc1.write_unique_partial(0, update, mask)
    system.run_until_idle(500)

    self.assertEqual(ReqOpcode.WRITE_UNIQUE_PTL, request.opcode)
    self.assertEqual(expected, system.s.dj.data_by_address[0])
    self.assertEqual((Resp.I, latest), system.cc0.cache[0])
    self.assertNotIn(0, system.hf.directory)
    self.assertNotIn(0, system.hf.dirty_data)
    self.assertTrue(any(message.opcode == SnpOpcode.SNP_CLEAN_INVALID
                        for message in system.cc0.received_messages))
    system.check_invariants()

  def test_write_clean_keeps_unique_clean_copy_and_home_latest_data(self):
    system = Zhujiang(buffer_capacity=2, id_capacity=1)
    system.s.dj.write("A", "old")
    system.cc0.read_unique("A")
    system.run_until_idle(200)
    system.cc0.store_cached("A", "latest")

    request = system.cc0.write_clean("A")
    system.run_until_idle(200)

    self.assertEqual(ReqOpcode.WRITE_CLEAN_FULL, request.opcode)
    self.assertEqual((Resp.UC, "latest"), system.cc0.cache["A"])
    self.assertEqual({"n00": Resp.UC}, system.hf.directory["A"])
    self.assertEqual("latest", system.hf.dirty_data["A"])
    self.assertEqual("old", system.s.dj.data_by_address["A"])

    read = system.cc1.read_shared("A")
    system.run_until_idle(200)
    self.assertEqual("latest", system.cc1.read_response_for(read).payload)
    self.assertEqual((Resp.SC, "latest"), system.cc0.cache["A"])
    self.assertEqual((Resp.SC, "latest"), system.cc1.cache["A"])
    system.check_invariants()


if __name__ == "__main__":
  unittest.main()
