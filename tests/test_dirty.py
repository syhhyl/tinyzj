import unittest

from tinyzj.chi import DatOpcode, Resp, RespErr
from tinyzj.zj import Zhujiang


class DirtyDataTest(unittest.TestCase):

  def test_home_eviction_retains_dirty_data_until_success(self):
    for fail in (False, True):
      with self.subTest(fail=fail):
        system = Zhujiang(buffer_capacity=2, id_capacity=1)
        system.s.dj.write("A", "old")
        system.cc0.read_unique("A")
        system.run_until_idle()
        system.cc0.store_cached("A", "latest")
        system.cc0.writeback("A")
        system.run_until_idle()
        if fail:
          system.s.error_addresses.add("A")
        eviction = system.hf.evict("A")
        self.assertEqual("latest", system.hf.dirty_data["A"])
        with self.assertRaises(ValueError):
          system.hf.evict("A")
        system.run_until_idle()
        self.assertEqual(RespErr.NDERR if fail else RespErr.OK,
                         system.hf.eviction_results[eviction].resp_err)
        self.assertEqual("old" if fail else "latest", system.s.dj.data_by_address["A"])
        self.assertEqual({"A": "latest"} if fail else {}, system.hf.dirty_data)
        self.assertFalse(system.hf.evictions)
        self.assertFalse(system.hf.pending_requests)
        self.assertFalse(system.hf.address_busy)
        if fail:
          system.s.error_addresses.clear()
          system.hf.evict("A")
          system.run_until_idle()
          self.assertEqual("latest", system.s.dj.data_by_address["A"])
          self.assertFalse(system.hf.dirty_data)

  def test_local_read_after_copyback_waits_and_refills(self):
    system = Zhujiang(buffer_capacity=2, retry_enabled=True)
    system.cc0.read_unique("A")
    system.run_until_idle()
    system.cc0.store_cached("A", "dirty")
    wb = system.cc0.writeback("A")
    request = system.cc0.read_unique("A")
    self.assertIsNotNone(request)
    self.assertIsNone(request.transaction_id)
    with self.assertRaises(ValueError):
      system.cc0.store_cached("A", "too early")
    system.run_until_idle()
    self.assertIsNotNone(system.cc0.write_response_for(wb))
    self.assertEqual("dirty", system.cc0.read_response_for(request).payload)
    self.assertEqual((Resp.UC, "dirty"), system.cc0.cache["A"])

  def test_writeback_and_snoop_races_preserve_latest_value(self):
    for unique in (False, True):
      for writeback_first in (False, True):
        with self.subTest(unique=unique, writeback_first=writeback_first):
          system = Zhujiang(buffer_capacity=2, id_capacity=1, retry_enabled=True)
          system.s.dj.write("A", "old")
          system.cc0.read_unique("A")
          system.run_until_idle()
          system.cc0.store_cached("A", "latest")
          read = system.cc1.read_unique if unique else system.cc1.read_shared
          if writeback_first:
            wb = system.cc0.writeback("A")
            request = read("A")
          else:
            request = read("A")
            for _ in range(3):
              system.step()
            wb = system.cc0.writeback("A")
          system.run_until_idle(500)
          self.assertEqual("latest", system.cc1.read_response_for(request).payload)
          self.assertIsNotNone(system.cc0.write_response_for(wb))
          self.assertEqual((Resp.I, "latest"), system.cc0.cache["A"])
          self.assertEqual("latest", system.hf.dirty_data["A"])
          for table in (system.hf.pending_requests, system.hf.write_dbids,
                        system.hf.address_busy, system.hf.pending_snoops):
            self.assertFalse(table)

  def test_dirty_writeback_completes_at_home_before_memory_update(self):
    system = Zhujiang(id_capacity=1)
    system.s.dj.write("A", "old")
    system.cc0.read_unique("A")
    system.run_until_idle()
    system.cc0.store_cached("A", "latest")
    wb = system.cc0.writeback("A")
    system.run_until_idle()
    self.assertIsNotNone(system.cc0.write_response_for(wb))
    self.assertNotIn("A", system.hf.directory)
    self.assertEqual("old", system.s.dj.data_by_address["A"])
    request = system.cc1.read_shared("A")
    system.run_until_idle()
    self.assertEqual("latest", system.cc1.read_response_for(request).payload)

  def test_dirty_snoop_preserves_latest_data_at_home(self):
    for unique in (False, True):
      with self.subTest(unique=unique):
        system = Zhujiang(buffer_capacity=2, id_capacity=1, retry_enabled=True)
        system.s.dj.write("A", "old")
        system.cc0.read_unique("A")
        system.run_until_idle()
        system.cc0.store_cached("A", "dirty")
        self.assertEqual(Resp.UD, system.cc0.cache["A"][0])
        self.assertEqual("old", system.s.dj.data_by_address["A"])
        request = (system.cc1.read_unique if unique else system.cc1.read_shared)("A")
        system.run_until_idle(200)
        self.assertEqual("dirty", system.cc1.read_response_for(request).payload)
        self.assertEqual("dirty", system.hf.dirty_data["A"])
        self.assertTrue(any(m.opcode == DatOpcode.SNP_RESP_DATA for m in system.hf.received_messages))
        system.cc1.write("A", "new")
        system.run_until_idle(200)
        self.assertEqual("new", system.s.dj.data_by_address["A"])
        self.assertFalse(system.hf.dirty_data)
        self.assertFalse(system.hf.pending_requests)
        self.assertFalse(system.hf.address_busy)

  def test_cached_store_requires_unique_copy(self):
    system = Zhujiang()
    with self.assertRaises(ValueError):
      system.cc0.store_cached("A", "data")
    system.cc0.read_shared("A")
    system.run_until_idle()
    with self.assertRaises(ValueError):
      system.cc0.store_cached("A", "data")
