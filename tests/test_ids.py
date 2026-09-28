import unittest

from tinyzj.chi import Channel, ReqOpcode, RespErr, RspOpcode
from tinyzj.zj import Zhujiang


class DownstreamIDTest(unittest.TestCase):

  def test_requester_reuse_preserves_response_history_and_waits_for_ack(self):
    system = Zhujiang(buffer_capacity=2, id_capacity=1)
    system.s.dj.write("A", "first")
    system.s.dj.write("B", "second")
    first = system.cc0.read_shared("A")
    second = system.cc0.read("B")
    self.assertEqual(0, first.transaction_id)
    self.assertIsNone(second.transaction_id)
    for _ in range(100):
      if system.cc0.read_response_for(first) is not None:
        break
      system.step()
    self.assertTrue(system.cc0.pending_acks)
    self.assertIsNone(second.transaction_id)
    self.assertIsNone(system.cc0.read_response_for(second))
    system.run_until_idle(200)
    self.assertEqual(0, second.transaction_id)
    self.assertEqual("first", system.cc0.read_response_for(first).payload)
    self.assertEqual("second", system.cc0.read_response_for(second).payload)
    self.assertFalse(system.cc0.has_pending_requests())
    self.assertFalse(system.cc0.pending_acks)

  def test_queued_writes_keep_payloads_across_requester_id_reuse(self):
    system = Zhujiang(buffer_capacity=2, id_capacity=1, error_addresses=["E"])
    requests = [(address, system.cc0.write(address, address + " value"))
                for address in ("A", "B", "E", "C")]
    self.assertEqual(3, len(system.cc0.waiting_requests))
    system.run_until_idle(300)
    self.assertEqual({a: a + " value" for a in ("A", "B", "C")},
                     system.s.dj.data_by_address)
    for address, request in requests:
      self.assertEqual(0, request.transaction_id)
      response = system.cc0.write_response_for(request)
      self.assertIsNotNone(response)
      self.assertEqual(RespErr.NDERR if address == "E" else RespErr.OK, response.resp_err)
    self.assertFalse(system.cc0.waiting_write_data)
    self.assertFalse(system.cc0.pending_write_data)
    self.assertFalse(system.cc0.has_pending_requests())

  def test_reject_invalid_id_capacity(self):
    for capacity in (0, -1, True, 1.5):
      with self.subTest(capacity=capacity), self.assertRaises(ValueError):
        Zhujiang(id_capacity=capacity)

  def test_bounded_ids_backpressure_and_reuse(self):
    for capacity in (1, 2, 3):
      with self.subTest(capacity=capacity):
        system = Zhujiang(buffer_capacity=2, id_capacity=capacity, error_addresses=["E"])
        for cc in (system.cc0, system.cc1):
          cc.read_shared("A")
          system.run_until_idle(200)
        requests = []
        for index in range(24):
          cc = (system.cc0, system.cc1)[index % 2]
          address = ("A", "B", "E")[index % 3]
          operation = ("write", "read_shared", "read_unique", "read")[index % 4]
          args = (address, str(index)) if operation == "write" else (address,)
          request = getattr(cc, operation)(*args)
          if request is not None:
            requests.append((cc, operation, request))
        for _ in range(2000):
          if not system.ring.in_flight:
            break
          system.step()
          for table in (system.hf.pending_requests, system.hf.downstream_requests,
                        system.hf.write_dbids, system.hf.pending_snoops, system.s.pending_writes):
            self.assertLessEqual(len(table), capacity)
            self.assertTrue(all(0 <= key < capacity for key in table))
        self.assertFalse(system.ring.in_flight)
        for cc, operation, request in requests:
          response = (cc.write_response_for if operation == "write" else cc.read_response_for)(request)
          self.assertIsNotNone(response)
          expected = RespErr.OK if request.address != "E" else (
            RespErr.NDERR if operation == "write" else RespErr.DERR)
          self.assertEqual(expected, response.resp_err)
        for table in (system.hf.pending_requests, system.hf.downstream_requests,
                      system.hf.write_dbids, system.hf.pending_snoops,
                      system.hf.address_busy, system.hf.pending_comp_acks,
                      system.hf.pending_write_data, system.s.pending_writes):
          self.assertFalse(table)

  def test_write_dbid_and_snoop_spaces_are_independent(self):
    for separate in (False, True):
      with self.subTest(separate=separate):
        system = Zhujiang(buffer_capacity=2, max_transactions=2)
        system.hf.next_home_id = 100 if separate else 0
        system.hf.next_downstream_id = 1000 if separate else 0
        system.hf.next_dbid = 2000 if separate else 0
        system.hf.next_snoop_id = 3000 if separate else 0
        system.cc0.read_shared("A")
        system.cc1.read_shared("A")
        system.run_until_idle(200)
        first = system.cc0.write("A", "new")
        second = system.cc1.write("B", "other")
        self.assertEqual(first.transaction_id, second.transaction_id)
        system.run_until_idle(200)
        self.assertIsNotNone(system.cc0.write_response_for(first))
        self.assertIsNotNone(system.cc1.write_response_for(second))
        self.assertEqual({"A": "new", "B": "other"}, system.s.dj.data_by_address)
        dbids = [m.dbid for cc in (system.cc0, system.cc1)
                 for m in cc.received_messages if m.opcode == RspOpcode.DBID_RESP]
        start = 2000 if separate else 0
        self.assertEqual([start, start + 1], sorted(dbids))
        snoops = [m for cc in (system.cc0, system.cc1)
                  for m in cc.received_messages if m.channel == Channel.SNP]
        start = 3000 if separate else 0
        self.assertEqual({start, start + 1}, {m.transaction_id for m in snoops})
        self.assertEqual(2, sum(m.transaction_id == start + 1 for m in snoops))
        for table in (system.hf.write_dbids, system.hf.pending_snoops,
                      system.hf.downstream_requests, system.hf.pending_requests,
                      system.hf.pending_write_data, system.hf.address_busy):
          self.assertFalse(table)

  def test_independent_ids_survive_snoops_and_error_completion(self):
    for error in (False, True):
      with self.subTest(error=error):
        system = Zhujiang(buffer_capacity=2, max_transactions=2,
                          error_addresses=["E"] if error else [])
        system.hf.next_home_id = 100
        system.hf.next_downstream_id = 1000
        system.s.next_dbid = 10000
        system.cc0.read_shared("A")
        system.run_until_idle()
        unique = system.cc1.read_unique("A")
        write = system.cc0.write("E", "value")
        system.run_until_idle(200)
        read = system.cc1.read("E")
        system.run_until_idle(200)
        self.assertIsNotNone(system.cc1.read_response_for(unique))
        self.assertEqual(RespErr.NDERR if error else RespErr.OK,
                         system.cc0.write_response_for(write).resp_err)
        response = system.cc1.read_response_for(read)
        self.assertEqual(RespErr.DERR if error else RespErr.OK, response.resp_err)
        self.assertEqual("read data" if error else "value", response.payload)
        downstream = [m for m in system.s.received_messages if m.channel == Channel.REQ]
        self.assertEqual([1000, 1001, 1002, 1003], [m.transaction_id for m in downstream])
        self.assertIn(ReqOpcode.WRITE_NO_SNP_FULL, [m.opcode for m in downstream])
        self.assertEqual({}, system.hf.downstream_requests)
        self.assertEqual({}, system.hf.pending_requests)
        self.assertEqual({}, system.hf.pending_comp_acks)
        self.assertEqual({}, system.hf.address_busy)
        self.assertEqual({}, system.hf.pending_write_data)
        self.assertEqual({}, system.s.pending_writes)
