import unittest

from tinyzj.chi import Channel, ReqOpcode, RespErr, RspOpcode
from tinyzj.zj import Zhujiang


class DownstreamIDTest(unittest.TestCase):

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
