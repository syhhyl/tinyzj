import unittest

from tinyzj.chi import RespErr
from tinyzj.zj import Zhujiang


class ExclusiveTest(unittest.TestCase):

  def test_failed_exclusive_read_clears_previous_monitor(self):
    system = Zhujiang(error_addresses={64})
    system.s.dj.write(0, "old")
    system.cc0.read_exclusive_no_snp(0)
    system.run_until_idle()
    system.cc0.read_exclusive_no_snp(64)
    system.run_until_idle()
    request = system.cc0.write_exclusive_no_snp(0, "new")
    system.run_until_idle()
    self.assertEqual(RespErr.OK, system.cc0.write_response_for(request).resp_err)
    self.assertEqual("old", system.s.dj.data_by_address[0])

  def test_exclusive_store_success_and_interference(self):
    for interfere in (False, True):
      system = Zhujiang(id_capacity=1, buffer_capacity=2, retry_enabled=True)
      system.s.dj.write(0, "old")
      read = system.cc0.read_exclusive_no_snp(0)
      system.run_until_idle(500)
      self.assertEqual(RespErr.EXOK, system.cc0.read_response_for(read).resp_err)
      if interfere:
        system.cc1.write_no_snp(0, "other")
        system.run_until_idle(500)
      write = system.cc0.write_exclusive_no_snp(0, "new")
      system.run_until_idle(500)
      self.assertEqual(RespErr.OK if interfere else RespErr.EXOK,
                       system.cc0.write_response_for(write).resp_err)
      self.assertEqual("other" if interfere else "new", system.s.dj.data_by_address[0])
      self.assertFalse(system.hf.exclusive_monitors)
      self.assertFalse(system.hf.address_busy)

  def test_two_monitors_allow_only_one_store(self):
    system = Zhujiang(id_capacity=2, buffer_capacity=2, retry_enabled=True)
    system.s.dj.write(0, "old")
    for cc in (system.cc0, system.cc1):
      cc.read_exclusive_no_snp(0)
    system.run_until_idle(500)
    requests = [(cc, cc.write_exclusive_no_snp(0, cc.node_name)) for cc in (system.cc0, system.cc1)]
    system.run_until_idle(500)
    self.assertEqual([RespErr.EXOK, RespErr.OK], sorted(cc.write_response_for(r).resp_err for cc, r in requests))
