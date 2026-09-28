import unittest

from tinyzj.chi import ReqOpcode, RespErr
from tinyzj.zj import Zhujiang


class AtomicTest(unittest.TestCase):

  def test_compare_write_error_preserves_home_dirty_value(self):
    system = Zhujiang(error_addresses={0}, id_capacity=1, buffer_capacity=2)
    system.s.error_addresses.clear()
    system.s.dj.write(0, bytes(64))
    system.cc0.read_unique(0)
    system.run_until_idle(500)
    system.cc0.store_cached(0, bytes([3]) * 64)
    system.s.error_addresses.add(0)
    request = system.cc1.atomic_compare(0, bytes([3]) * 8, bytes([9]) * 8)
    system.run_until_idle(500)
    self.assertEqual(RespErr.NDERR, system.cc1.read_response_for(request).resp_err)
    self.assertEqual(bytes([3]) * 64, system.hf.dirty_data[0])
    self.assertEqual(bytes(64), system.s.dj.data_by_address[0])
    self.assertFalse(system.hf.atomic_results)
    self.assertFalse(system.hf.pending_write_errors)

  def test_compare_mismatch_returns_old_without_downstream_write(self):
    system = Zhujiang(id_capacity=1, buffer_capacity=2)
    original = bytes([3]) * 64
    system.s.dj.write(0, original)
    request = system.cc0.atomic_compare(0, bytes(8), bytes([9]) * 8)
    system.run_until_idle(500)
    self.assertEqual(original[:8], system.cc0.read_response_for(request).payload)
    self.assertEqual(original, system.s.dj.data_by_address[0])
    self.assertFalse(any(m.opcode == ReqOpcode.WRITE_NO_SNP_FULL for m in system.s.received_messages))
    self.assertFalse(system.hf.pending_requests)

  def test_competing_compare_has_exactly_one_winner(self):
    for capacity in (1, 2):
      system = Zhujiang(id_capacity=capacity, buffer_capacity=2, retry_enabled=True)
      system.s.dj.write(0, bytes(64))
      system.cc0.read_unique(0)
      system.run_until_idle(500)
      system.cc0.store_cached(0, bytes([3]) * 64)
      requests = [(cc, cc.atomic_compare(0, bytes([3]) * 8, bytes([i]) * 8))
                  for i, cc in enumerate((system.cc0, system.cc1), 1)]
      system.run_until_idle(1000)
      values = [cc.read_response_for(request).payload for cc, request in requests]
      self.assertEqual(1, values.count(bytes([3]) * 8))
      final = system.s.dj.data_by_address[0]
      self.assertIn(final[:8], values)
      self.assertEqual(bytes([3]) * 56, final[8:])
      self.assertFalse(system.hf.address_busy)
      self.assertFalse(system.hf.atomic_results)
      for cc, _ in requests:
        self.assertFalse(cc.pending_write_data)
        self.assertFalse(cc.write_data_sent)

  def test_compare_read_error_and_invalid_input(self):
    system = Zhujiang(error_addresses={0}, id_capacity=1)
    system.s.dj.write(0, bytes(64))
    request = system.cc0.atomic_compare(0, bytes(8), bytes([1]) * 8)
    system.run_until_idle(500)
    self.assertEqual(RespErr.DERR, system.cc0.read_response_for(request).resp_err)
    self.assertEqual(bytes(64), system.s.dj.data_by_address[0])
    self.assertFalse(system.hf.address_busy)
    self.assertFalse(system.cc0.pending_write_data)
    for address, compare in ((1, bytes(8)), (0, bytes(4))):
      with self.assertRaises(ValueError):
        system.cc0.atomic_compare(address, compare, bytes(8))
    self.assertFalse(system.ring.in_flight)

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
