import unittest

from tinyzj.chi import Channel, ReqOpcode, Resp
from tinyzj.zj import Zhujiang


class WriteModeTest(unittest.TestCase):

  def test_no_snoop_write_does_not_invalidate_cached_copy(self):
    system = Zhujiang()
    system.s.dj.write("A", "old")
    system.cc0.read_shared("A")
    system.run_until_idle()
    request = system.cc1.write_no_snp("A", "new")
    system.run_until_idle()
    self.assertEqual(ReqOpcode.WRITE_NO_SNP_FULL, request.opcode)
    self.assertEqual("new", system.s.dj.data_by_address["A"])
    self.assertEqual("old", system.cc0.cache["A"][1])
    self.assertFalse(any(m.channel == Channel.SNP for m in system.cc0.received_messages))
    self.assertIsNotNone(system.cc1.write_response_for(request))

  def test_unique_write_invalidates_cached_copy(self):
    system = Zhujiang()
    system.s.dj.write("A", "old")
    system.cc0.read_shared("A")
    system.run_until_idle()
    request = system.cc1.write_unique("A", "new")
    system.run_until_idle()
    self.assertEqual(ReqOpcode.WRITE_UNIQUE_FULL, request.opcode)
    self.assertEqual((Resp.I, "old"), system.cc0.cache["A"])
    self.assertEqual("new", system.s.dj.data_by_address["A"])
