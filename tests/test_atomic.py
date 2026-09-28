import unittest

from tinyzj.zj import Zhujiang


class AtomicTest(unittest.TestCase):

  def test_two_requesters_swap_one_dirty_line_serially(self):
    system = Zhujiang(buffer_capacity=2, id_capacity=2, retry_enabled=True)
    system.s.dj.write(0, bytes(64))
    system.cc0.read_unique(0)
    system.run_until_idle(500)
    system.cc0.store_cached(0, bytes([3]) * 64)
    first = system.cc0.atomic_swap(0, bytes([1]) * 8)
    second = system.cc1.atomic_swap(0, bytes([2]) * 8)
    system.run_until_idle(1000)
    results = {system.cc0.read_response_for(first).payload,
               system.cc1.read_response_for(second).payload}
    final = system.s.dj.data_by_address[0]
    self.assertIn(bytes([3]) * 8, results)
    self.assertIn(final[:8], (bytes([1]) * 8, bytes([2]) * 8))
    other = bytes([2 if final[0] == 1 else 1]) * 8
    self.assertIn(other, results)
    self.assertEqual(bytes([3]) * 56, final[8:])
    self.assertFalse(system.hf.atomic_results)
    self.assertFalse(system.hf.address_busy)
    for cc in (system.cc0, system.cc1):
      self.assertFalse(cc.pending_write_data)
      self.assertFalse(cc.write_data_sent)
