import unittest

from tinyzj.chi import Channel, DatOpcode, ReqOpcode, RespErr, RspOpcode
from tinyzj.xj import Message
from tinyzj.zj import Zhujiang


class AtomicTest(unittest.TestCase):

  def test_atomic_data_completion_before_dbid_retains_operand(self):
    system = Zhujiang(id_capacity=1)
    cc = system.cc0
    request = cc.atomic_load(0, "ADD", bytes([1]) * 8)
    result = Message("n01", "n00", channel=Channel.DAT, opcode=DatOpcode.COMP_DATA,
                     transaction_id=request.transaction_id, payload=bytes(8))
    cc.receive(result)
    self.assertIsNone(cc.read_response_for(request))
    self.assertTrue(cc.active_requests)
    cc.receive(Message("n01", "n00", channel=Channel.RSP, opcode=RspOpcode.DBID_RESP,
                       transaction_id=request.transaction_id, dbid=29))
    self.assertIs(result, cc.read_response_for(request))
    self.assertEqual(bytes([1]) * 8, system.ring.in_flight[-1].message.payload)
    self.assertFalse(cc.active_requests)
    self.assertFalse(cc.atomic_completions)

  def test_atomic_sizes_and_offsets_share_one_line_lock(self):
    for size in (1, 2, 4, 8):
      system = Zhujiang(id_capacity=2, buffer_capacity=2, retry_enabled=True)
      system.s.dj.write(64, bytes(64))
      offset = 64 - size
      first = system.cc0.atomic_load(64 + offset, "ADD", (1).to_bytes(size, "little"))
      second = system.cc1.atomic_compare(64 + offset, (1).to_bytes(size, "little"),
                                         (7).to_bytes(size, "little"))
      system.run_until_idle(1000)
      self.assertEqual(64, first.address)
      self.assertEqual(offset, first.byte_offset)
      self.assertEqual(bytes(size), system.cc0.read_response_for(first).payload)
      # Either legal serialization is possible across the two requesters.
      before = int.from_bytes(system.cc1.read_response_for(second).payload, "little")
      self.assertIn(before, (0, 1))
      expected = 7 if before == 1 else 1
      self.assertEqual(bytes(offset) + expected.to_bytes(size, "little"), system.s.dj.data_by_address[64])

  def test_arithmetic_operations_and_completion_channels(self):
    vectors = (("ADD", 0xffffffffffffffff, 1, 0),
               ("CLR", 0x55, 0x0f, 0x50), ("EOR", 0x55, 0x0f, 0x5a),
               ("SET", 0x50, 0x0f, 0x5f),
               ("SMAX", 0xffffffffffffffff, 1, 1),
               ("SMIN", 0xffffffffffffffff, 1, 0xffffffffffffffff),
               ("UMAX", 0xffffffffffffffff, 1, 0xffffffffffffffff),
               ("UMIN", 0xffffffffffffffff, 1, 1))
    for store in (False, True):
      for operation, initial, operand, expected in vectors:
        with self.subTest(store=store, operation=operation):
          system = Zhujiang(id_capacity=1, buffer_capacity=2)
          old = initial.to_bytes(8, "little")
          system.s.dj.write(0, old + bytes([37]) * 56)
          cc = system.cc0
          request = (cc.atomic_store if store else cc.atomic_load)(0, operation, operand.to_bytes(8, "little"))
          system.run_until_idle(500)
          response = (cc.write_response_for if store else cc.read_response_for)(request)
          self.assertEqual(RespErr.OK, response.resp_err)
          self.assertEqual(None if store else old, response.payload)
          self.assertEqual(expected.to_bytes(8, "little") + bytes([37]) * 56,
                           system.s.dj.data_by_address[0])
          self.assertFalse(cc.pending_write_data)
          self.assertFalse(cc.write_data_sent)
          self.assertFalse(system.hf.atomic_results)

  def test_concurrent_fetch_add_returns_each_predecessor_once(self):
    system = Zhujiang(id_capacity=2, max_transactions=1, buffer_capacity=2,
                      retry_enabled=True, credit_return_delay=3)
    system.s.dj.write(0, bytes(64))
    requests = [(cc, cc.atomic_load(0, "ADD", (1).to_bytes(8, "little")))
                for _ in range(20) for cc in (system.cc0, system.cc1)]
    system.run_until_idle(4000)
    self.assertEqual(list(range(40)), sorted(int.from_bytes(cc.read_response_for(r).payload, "little")
                                           for cc, r in requests))
    self.assertEqual(40, int.from_bytes(system.s.dj.data_by_address[0][:8], "little"))

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
