import unittest

from tinyzj.chi import DatOpcode, Resp
from tinyzj.zj import Zhujiang


class DirtyDataTest(unittest.TestCase):

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
