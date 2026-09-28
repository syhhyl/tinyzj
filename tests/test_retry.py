import unittest

from tinyzj.chi import Channel, RspOpcode
from tinyzj.xj import Message
from tinyzj.zj import Zhujiang


class RetryTest(unittest.TestCase):

  def test_single_slot_retry_completes_and_cleans_credits(self):
    system = Zhujiang(buffer_capacity=2, max_transactions=1, id_capacity=2,
                      retry_enabled=True)
    requests = []
    for index in range(20):
      cc = (system.cc0, system.cc1)[index % 2]
      requests.append((cc, cc.write(str(index % 3), str(index))))
    system.run_until_idle(2000)
    self.assertTrue(any(m.opcode == RspOpcode.RETRY_ACK
                        for cc in (system.cc0, system.cc1) for m in cc.received_messages))
    for cc, request in requests:
      self.assertIsNotNone(cc.write_response_for(request))
      self.assertTrue(request.allow_retry)
    self.assertFalse(system.hf.credit_waiters)
    self.assertFalse(system.hf.credit_reservations)
    for cc in (system.cc0, system.cc1):
      self.assertFalse(cc.retry_requests)
      self.assertFalse(cc.protocol_credits)
      self.assertFalse(cc.has_pending_requests())

  def test_grant_before_retry_is_saved_and_consumed_once(self):
    system = Zhujiang()
    cc = system.cc0
    request = cc.read("A")
    grant = Message("n01", "n00", channel=Channel.RSP,
                    opcode=RspOpcode.PCRD_GRANT, transaction_id=999)
    retry = Message("n01", "n00", channel=Channel.RSP,
                    opcode=RspOpcode.RETRY_ACK, transaction_id=request.transaction_id)
    cc.receive(grant)
    cc.advance_requests()
    self.assertEqual(1, len(system.ring.in_flight))
    cc.receive(retry)
    cc.advance_requests()
    resent = system.ring.in_flight[-1].message
    self.assertIsNot(resent, request)
    self.assertFalse(resent.allow_retry)
    self.assertTrue(request.allow_retry)
    self.assertEqual(request.transaction_id, resent.transaction_id)
    self.assertFalse(cc.protocol_credits)
    cc.advance_requests()
    self.assertEqual(2, len(system.ring.in_flight))
