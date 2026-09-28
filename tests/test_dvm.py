import unittest

from tinyzj.chi import Channel, RespErr, RspOpcode, SnpOpcode
from tinyzj.xj import Message
from tinyzj.zj import Zhujiang


class DVMTest(unittest.TestCase):

  def test_corrupt_dvm_operand_does_not_invalidate_tlb(self):
    system = Zhujiang()
    system.cc1.tlb[1] = 100
    request = system.cc0.dvm_invalidate()
    for _ in range(100):
      for injection in system.ring.in_flight:
        if injection.message.channel == Channel.DAT:
          injection.message.resp_err = RespErr.DERR
      system.step()
      if system.cc0.write_response_for(request):
        break
    self.assertEqual(RespErr.NDERR, system.cc0.write_response_for(request).resp_err)
    self.assertEqual({1: 100}, system.cc1.tlb)
    self.assertFalse(system.hf.pending_snoops)
    system.check_invariants()

  def test_broadcast_and_sync_complete_after_all_targets(self):
    system = Zhujiang(id_capacity=1, buffer_capacity=2, retry_enabled=True)
    for cc in (system.cc0, system.cc1):
      cc.tlb.update({1: 100, 2: 200})
    invalidate = system.cc0.dvm_invalidate(1)
    sync = system.cc0.dvm_sync()
    system.run_until_idle(500)
    for request in (invalidate, sync):
      self.assertIsNotNone(system.cc0.write_response_for(request))
    for cc in (system.cc0, system.cc1):
      self.assertEqual({2: 200}, cc.tlb)
      self.assertFalse(cc.dvm_parts)
    self.assertFalse(system.hf.pending_snoops)
    self.assertFalse(system.hf.address_busy)

  def test_snoop_waits_for_both_parts_in_either_order(self):
    for order in ((0, 1), (1, 0)):
      system = Zhujiang()
      system.cc1.tlb[1] = 100
      for index, part in enumerate(order):
        system.cc1.receive(Message("n01", "n11", transaction_id=77,
                                   channel=Channel.SNP, opcode=SnpOpcode.SNP_DVM_OP,
                                   payload=(part, "invalidate", None)))
        replies = [i for i in system.ring.in_flight if i.message.opcode == RspOpcode.SNP_RESP]
        self.assertEqual(index, len(replies))
      self.assertFalse(system.cc1.tlb)
