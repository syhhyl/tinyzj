import unittest

from tinyzj.chi import RespErr, SnpOpcode
from tinyzj.zj import Zhujiang


class MaintenanceTest(unittest.TestCase):

  def test_clean_shared_keeps_readable_copy_and_flushes_memory(self):
    system = Zhujiang(id_capacity=1, buffer_capacity=2)
    system.s.dj.write(0, bytes(64))
    system.cc0.read_unique(0)
    system.run_until_idle(500)
    system.cc0.store_cached(0, bytes([7]) * 64)
    request = system.cc1.clean_shared(0)
    system.run_until_idle(500)
    self.assertEqual(RespErr.OK, system.cc1.write_response_for(request).resp_err)
    self.assertEqual(bytes([7]) * 64, system.cc0.cache[0][1])
    self.assertIsNone(system.cc0.read_shared(0))
    self.assertEqual(bytes([7]) * 64, system.s.dj.data_by_address[0])
    self.assertFalse(system.hf.dirty_data)

  def test_clean_invalid_flushes_dirty_and_invalidates(self):
    for error in (False, True):
      system = Zhujiang(id_capacity=1, buffer_capacity=2, retry_enabled=True)
      system.s.dj.write(0, bytes(64))
      system.cc0.read_unique(0)
      system.run_until_idle(500)
      system.cc0.store_cached(0, bytes([9]) * 64)
      if error:
        system.s.error_addresses.add(0)
      request = system.cc1.clean_invalid(0)
      system.run_until_idle(500)
      self.assertEqual(RespErr.NDERR if error else RespErr.OK,
                       system.cc1.write_response_for(request).resp_err)
      self.assertNotIn(0, system.cc0.cache)
      self.assertNotIn(0, system.hf.directory)
      self.assertEqual(bytes(64) if error else bytes([9]) * 64, system.s.dj.data_by_address[0])
      self.assertEqual(error, 0 in system.hf.dirty_data)
      self.assertFalse(system.hf.address_busy)
      self.assertTrue(any(m.opcode == SnpOpcode.SNP_CLEAN_INVALID for m in system.cc0.received_messages))

  def test_clean_invalid_without_cached_line_completes(self):
    system = Zhujiang(id_capacity=1)
    request = system.cc0.clean_invalid("A")
    system.run_until_idle()
    self.assertEqual(RespErr.OK, system.cc0.write_response_for(request).resp_err)
    self.assertFalse(system.s.received_messages)
