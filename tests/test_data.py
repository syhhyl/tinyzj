import unittest

from tinyzj.chi import Channel, DatOpcode, RespErr
from tinyzj.data import DataAssembly, data_check, packets
from tinyzj.xj import Message
from tinyzj.zj import Zhujiang


class DataTest(unittest.TestCase):

  def test_odd_byte_parity_detects_corrupted_packet(self):
    self.assertEqual(0b1001, data_check(bytes((0, 1, 2, 3))))
    assembler = DataAssembly(16)
    beats = packets(Message("s", "r", payload=bytes(64), channel=Channel.DAT), 16)
    beats[2].payload = bytes([1]) + beats[2].payload[1:]
    for beat in beats[:-1]:
      self.assertIsNone(assembler.accept(beat))
    self.assertEqual(RespErr.DERR, assembler.accept(beats[-1]).resp_err)

  def test_poison_bit_offsets_and_error_conversion(self):
    for width in (16, 32, 64):
      assembler = DataAssembly(width)
      message = Message("s", "r", payload=bytes(64), channel=Channel.DAT,
                        poison=0b10000101)
      result = None
      for beat in reversed(packets(message, width)):
        result = assembler.accept(beat)
      self.assertEqual(message.poison, result.poison)
      self.assertEqual(RespErr.DERR, result.resp_err)

  def test_poisoned_read_does_not_fill_requester_cache(self):
    system = Zhujiang()
    request = system.cc0.read_shared("A")
    response = Message("n01", "n00", address="A", payload=bytes(64),
                       transaction_id=request.transaction_id, dbid=3,
                       home_nid="n01", channel=Channel.DAT,
                       opcode=DatOpcode.COMP_DATA, poison=1)
    for beat in packets(response, 16):
      system.cc0.receive(beat)
    self.assertNotIn("A", system.cc0.cache)
    self.assertEqual(RespErr.DERR, system.cc0.read_response_for(request).resp_err)

  def test_corrupted_write_packet_does_not_modify_memory(self):
    system = Zhujiang(id_capacity=1, buffer_capacity=2)
    system.s.dj.write("A", bytes(64))
    request = system.cc0.write("A", bytes([1]) * 64)
    corrupted = False
    for _ in range(500):
      for injection in system.ring.in_flight:
        message = injection.message
        if not corrupted and message.source_name == "n00" and message.channel == Channel.DAT:
          message.resp_err = RespErr.DERR
          corrupted = True
      system.step()
      if system.cc0.write_response_for(request) is not None:
        break
    self.assertTrue(corrupted)
    self.assertEqual(RespErr.NDERR, system.cc0.write_response_for(request).resp_err)
    self.assertEqual(bytes(64), system.s.dj.data_by_address["A"])
    self.assertFalse(system.hf.pending_write_errors)

  def test_read_id_and_ack_wait_for_last_packet(self):
    system = Zhujiang(id_capacity=1)
    request = system.cc0.read_shared("A")
    beats = packets(Message("n01", "n00", payload=bytes(64), address="A",
                            transaction_id=request.transaction_id, dbid=91,
                            home_nid="n01", channel=Channel.DAT,
                            opcode=DatOpcode.COMP_DATA), 16)
    for beat in (beats[3], beats[0], beats[2]):
      system.cc0.receive(beat)
      self.assertIsNone(system.cc0.read_response_for(request))
      self.assertIn(request.transaction_id, system.cc0.active_requests)
      self.assertEqual(1, len(system.ring.in_flight))
    system.cc0.receive(beats[1])
    self.assertFalse(system.cc0.active_requests)
    self.assertEqual(91, system.ring.in_flight[-1].message.transaction_id)

  def test_partial_error_leaves_memory_and_resources_intact(self):
    system = Zhujiang(error_addresses={0}, buffer_capacity=2, id_capacity=1)
    original = bytes(range(64))
    system.s.dj.write(0, original)
    request = system.cc0.write_no_snp_partial(0, bytes(64), (1 << 64) - 1)
    system.run_until_idle(500)
    self.assertEqual(RespErr.NDERR, system.cc0.write_response_for(request).resp_err)
    self.assertEqual(original, system.s.dj.data_by_address[0])
    self.assertFalse(system.hf.address_busy)
    self.assertFalse(system.hf.write_dbids)
    self.assertFalse(system.s.pending_writes)

  def test_partial_write_masks_across_packet_boundaries(self):
    for width in (16, 32, 64):
      system = Zhujiang(data_width=width, id_capacity=1, buffer_capacity=2)
      original = bytes(range(64))
      system.s.dj.write(128, original)
      mask = sum(1 << i for i in (0, 15, 16, 31, 32, 63))
      request = system.cc0.write_no_snp_partial(128, bytes([255]) * 64, mask)
      system.run_until_idle(500)
      self.assertIsNotNone(system.cc0.write_response_for(request))
      self.assertEqual(bytes(255 if mask & (1 << i) else i for i in range(64)),
                       system.s.dj.data_by_address[128])
      system.cc0.write_no_snp_partial(128, bytes(64), 0)
      system.run_until_idle(500)
      self.assertEqual(255, system.s.dj.data_by_address[128][63])

  def test_partial_write_rejects_invalid_inputs_before_injection(self):
    system = Zhujiang()
    for address, data, mask in ((1, bytes(64), 1), (0, b"x", 1), (0, bytes(64), -1)):
      with self.assertRaises(ValueError):
        system.cc0.write_no_snp_partial(address, data, mask)
    self.assertFalse(system.ring.in_flight)

  def test_reordered_packets_complete_only_when_all_arrive(self):
    for width in (16, 32, 64):
      assembler = DataAssembly(width)
      message = Message("s", "r", payload=bytes(range(64)), transaction_id=7,
                        channel=Channel.DAT, opcode=DatOpcode.COMP_DATA)
      beats = packets(message, width)
      self.assertEqual(list(range(0, 4, width // 16)), [b.data_id for b in beats])
      beats[0].resp_err = RespErr.DERR
      for beat in reversed(beats[1:]):
        self.assertIsNone(assembler.accept(beat))
      result = assembler.accept(beats[0])
      self.assertEqual(message.payload, result.payload)
      self.assertEqual(RespErr.DERR, result.resp_err)
      self.assertFalse(assembler.pending)

  def test_duplicate_and_inconsistent_packets_do_not_complete(self):
    assembler = DataAssembly(16)
    beats = packets(Message("s", "r", payload=bytes(64), channel=Channel.DAT), 16)
    assembler.accept(beats[0])
    with self.assertRaises(ValueError):
      assembler.accept(beats[0])
    beats[1].dbid = 23
    with self.assertRaises(ValueError):
      assembler.accept(beats[1])
    self.assertEqual(1, len(next(iter(assembler.pending.values()))))

  def test_full_line_paths_with_bounded_resources(self):
    for width in (16, 32, 64):
      system = Zhujiang(data_width=width, buffer_capacity=2, id_capacity=1,
                        retry_enabled=True, credit_return_delay=3)
      data = bytes(range(64))
      system.cc0.write("A", data)
      system.run_until_idle(500)
      self.assertEqual(data, system.s.dj.data_by_address["A"])
      request = system.cc0.read_unique("A")
      system.run_until_idle(500)
      self.assertEqual(data, system.cc0.read_response_for(request).payload)
      system.cc0.store_cached("A", data[::-1])
      request = system.cc1.read_unique("A")
      system.run_until_idle(500)
      self.assertEqual(data[::-1], system.cc1.read_response_for(request).payload)
      system.cc1.store_cached("A", bytes([42]) * 64)
      system.cc1.writeback("A")
      system.run_until_idle(500)
      system.hf.evict("A")
      system.run_until_idle(500)
      self.assertEqual(bytes([42]) * 64, system.s.dj.data_by_address["A"])
      for endpoint in (system.cc0, system.cc1, system.hf, system.s):
        self.assertFalse(endpoint.data_assembly.pending)
