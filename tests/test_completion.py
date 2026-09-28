import unittest

from tinyzj.chi import Channel, DatOpcode, RespErr, RspOpcode
from tinyzj.xj import Message, Ring, RingNode
from tinyzj.zj import Socket, Zhujiang


class WriteCompletionTest(unittest.TestCase):

  def test_combined_response_at_requester_and_home(self):
    for target in ("rn", "hn"):
      for error in (RespErr.OK, RespErr.NDERR):
        with self.subTest(target=target, error=error):
          system = Zhujiang(id_capacity=1)
          request = system.cc0.write("A", "value")
          if target == "hn":
            for _ in range(100):
              system.step()
              if system.hf.downstream_requests:
                break
          endpoint = system.cc0 if target == "rn" else system.hf
          source = "n01" if target == "rn" else "n10"
          combined = Message(source, endpoint.node_name, transaction_id=0,
                             dbid=79, channel=Channel.RSP,
                             opcode=RspOpcode.COMP_DBID_RESP, resp_err=error)
          endpoint.receive(combined)
          data = [i.message for i in system.ring.in_flight
                  if i.message.source_name == endpoint.node_name
                  and i.message.opcode == DatOpcode.NON_COPY_BACK_WRITE_DATA]
          self.assertEqual(1, len(data))
          self.assertEqual(79, data[0].transaction_id)
          self.assertEqual("value", data[0].payload)
          if target == "rn":
            self.assertIs(combined, endpoint.write_response_for(request))
            self.assertFalse(endpoint.active_requests)
          else:
            response = next(i.message for i in system.ring.in_flight
                            if i.message.opcode == RspOpcode.COMP)
            self.assertEqual(error, response.resp_err)
            self.assertFalse(endpoint.downstream_requests)

  def test_home_retains_mapping_until_both_downstream_events(self):
    for comp_first in (False, True):
      for error in (RespErr.OK, RespErr.NDERR):
        with self.subTest(comp_first=comp_first, error=error):
          system = Zhujiang(id_capacity=1)
          request = system.cc0.write("A", "value")
          for _ in range(100):
            system.step()
            if system.hf.downstream_requests:
              break
          home = system.hf
          comp = Message("n10", "n01", transaction_id=0,
                         channel=Channel.RSP, opcode=RspOpcode.COMP, resp_err=error)
          dbid = Message("n10", "n01", transaction_id=0, dbid=19,
                         channel=Channel.RSP, opcode=RspOpcode.DBID_RESP)
          first, second = (comp, dbid) if comp_first else (dbid, comp)
          home.receive(first)
          self.assertTrue(home.downstream_requests)
          self.assertTrue(home.address_busy)
          self.assertFalse(any(i.message.opcode == RspOpcode.COMP for i in system.ring.in_flight))
          home.receive(second)
          result = next(i.message for i in system.ring.in_flight if i.message.opcode == RspOpcode.COMP)
          self.assertEqual(request.transaction_id, result.transaction_id)
          self.assertEqual(error, result.resp_err)
          data = [i.message for i in system.ring.in_flight
                  if i.message.source_name == "n01" and i.message.channel == Channel.DAT]
          self.assertEqual(1, len(data))
          self.assertEqual(19, data[0].transaction_id)
          self.assertEqual("value", data[0].payload)
          self.assertFalse(home.downstream_requests)
          self.assertFalse(home.address_busy)
          self.assertFalse(home.downstream_completions)
          self.assertFalse(home.downstream_data_sent)

  def test_requester_waits_for_data_and_comp_in_either_order(self):
    for comp_first in (False, True):
      for error in (RespErr.OK, RespErr.NDERR):
        with self.subTest(comp_first=comp_first, error=error):
          ring = Ring([RingNode("rn", "CC"), RingNode("hn", "HF"), RingNode("sn", "S")])
          cc = Socket(ring, "rn", "hn", id_capacity=1)
          request = cc.write("A", "first value")
          queued = cc.write("B", "second value")
          comp = Message("hn", "rn", transaction_id=0,
                         channel=Channel.RSP, opcode=RspOpcode.COMP, resp_err=error)
          dbid = Message("hn", "rn", transaction_id=0, dbid=73,
                         channel=Channel.RSP, opcode=RspOpcode.DBID_RESP)
          first, second = (comp, dbid) if comp_first else (dbid, comp)
          cc.receive(first)
          cc.advance_requests()
          self.assertIsNone(cc.write_response_for(request))
          self.assertIsNone(queued.transaction_id)
          self.assertIn(0, cc.active_requests)
          self.assertEqual("await_dbid" if comp_first else "await_comp",
                           cc.request_states[request])
          cc.receive(second)
          self.assertIs(comp, cc.write_response_for(request))
          data = [i.message for i in ring.in_flight
                  if i.message.opcode == DatOpcode.NON_COPY_BACK_WRITE_DATA]
          self.assertEqual(1, len(data))
          self.assertEqual(73, data[0].transaction_id)
          self.assertEqual("first value", data[0].payload)
          cc.advance_requests()
          self.assertEqual(0, queued.transaction_id)
          self.assertIsNone(cc.write_response_for(queued))
          self.assertEqual("second value", cc.pending_write_data[0])
          self.assertFalse(cc.write_completions)
          self.assertFalse(cc.write_data_sent)
