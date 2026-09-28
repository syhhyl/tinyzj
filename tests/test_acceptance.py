import random
import unittest

from tinyzj.zj import Zhujiang


class AcceptanceTest(unittest.TestCase):

  def test_replayable_mixed_extension_workload(self):
    for seed in range(6):
      rng = random.Random(seed)
      system = Zhujiang(data_width=(16, 32, 64)[seed % 3], buffer_capacity=2,
                        id_capacity=1 + seed % 2, retry_enabled=True,
                        credit_return_delay=1 + seed % 3)
      expected = {address: bytes(64) for address in (0, 64, 128)}
      for address, value in expected.items():
        system.s.dj.write(address, value)
      def drain():
        for _ in range(1000):
          system.check_invariants()
          if not system.has_pending_work():
            return
          system.step()
        self.fail(f"seed {seed}: workload stalled")
      for iteration in range(100):
        cc = rng.choice((system.cc0, system.cc1))
        address = rng.choice(list(expected))
        operation = rng.randrange(4)
        if operation == 0:
          request = cc.atomic_load(address, "ADD", (1).to_bytes(8, "little"))
          drain()
          old = expected[address][:8]
          self.assertEqual(old, cc.read_response_for(request).payload)
          expected[address] = ((int.from_bytes(old, "little") + 1) % (1 << 64)).to_bytes(8, "little") + expected[address][8:]
        elif operation == 1:
          cc.read_unique(address)
          drain()
          value = rng.randbytes(64)
          cc.store_cached(address, value)
          expected[address] = value
          cc.clean_invalid(address)
          drain()
        elif operation == 2:
          cc.dvm_invalidate()
          cc.dvm_sync()
          drain()
        else:
          cc.clean_shared(address)
          drain()
        self.assertEqual(expected, system.s.dj.data_by_address, (seed, iteration))
      drain()

  def test_orphaned_endpoint_work_cannot_be_reported_idle(self):
    system = Zhujiang()
    system.s.pending_writes[7] = object()
    with self.assertRaises(RuntimeError):
      system.run_until_idle(3)
