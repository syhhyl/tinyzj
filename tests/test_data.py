import unittest

from tinyzj.chi import Channel, DatOpcode, RespErr
from tinyzj.data import DataAssembly, packets
from tinyzj.xj import Message
from tinyzj.zj import Zhujiang


class DataTest(unittest.TestCase):

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
