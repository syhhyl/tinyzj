import random
import unittest

from tinyzj.chi import Resp, RespErr
from tinyzj.zj import Zhujiang


class StressTest(unittest.TestCase):

  def assert_idle_consistent(self, system):
    self.assertFalse(system.ring.in_flight)
    for name in ("pending_requests", "address_busy", "pending_comp_acks",
                 "pending_snoops", "pending_write_data"):
      self.assertFalse(getattr(system.hf, name), name)
    self.assertFalse(system.s.pending_writes)
    self.assertFalse(system.hf.downstream_requests)
    self.assertFalse(system.hf.write_dbids)
    self.assertFalse(system.hf.downstream_completions)
    self.assertFalse(system.hf.downstream_data_sent)
    holders = {}
    for cc in (system.cc0, system.cc1):
      self.assertFalse(cc.pending_write_data)
      self.assertFalse(cc.write_completions)
      self.assertFalse(cc.write_data_sent)
      self.assertFalse(cc.has_pending_requests())
      self.assertTrue(all(state == "complete" for state in cc.request_states.values()))
      self.assertFalse(cc.waiting_write_data)
      for address, (state, value) in cc.cache.items():
        self.assertNotIn(address, system.s.error_addresses)
        self.assertEqual(system.s.dj.read(address)[0], value)
        holders.setdefault(address, {})[cc.node_name] = state
    self.assertEqual(holders, {a: h for a, h in system.hf.directory.items() if h})
    for states in holders.values():
      if Resp.UC in states.values():
        self.assertEqual(1, len(states))

  def test_seeded_concurrent_workloads(self):
    for capacity in (2, 3, None):
      for slots in (1, 2, None):
        for seed in range(10):
          with self.subTest(capacity=capacity, slots=slots, seed=seed):
            rng = random.Random(seed)
            system = Zhujiang(buffer_capacity=capacity, max_transactions=slots,
                              id_capacity=(1, 2, 3, None)[seed % 4],
                              error_addresses=["E"])
            system.hf.next_home_id = 100
            system.hf.next_downstream_id = 1000
            system.hf.next_dbid = 2000
            system.hf.next_snoop_id = 3000
            system.s.next_dbid = 10000
            for batch in range(3):
              requests = []
              for index in range(30):
                cc = rng.choice((system.cc0, system.cc1))
                address = rng.choice(("A", "B", "C", "D", "E"))
                operation = rng.choice(("read", "read_shared", "read_unique", "write"))
                args = (address, f"{seed}:{batch}:{index}") if operation == "write" else (address,)
                request = getattr(cc, operation)(*args)
                if request is not None:
                  requests.append((cc, operation, request))
                for _ in range(rng.randrange(3)):
                  system.step()
              system.run_until_idle(5000)
              for cc, operation, request in requests:
                response = (cc.write_response_for if operation == "write" else cc.read_response_for)(request)
                self.assertIsNotNone(response)
                expected = RespErr.OK
                if request.address == "E":
                  expected = RespErr.NDERR if operation == "write" else RespErr.DERR
                self.assertEqual(expected, response.resp_err)
              self.assert_idle_consistent(system)

  def test_seeded_sequential_reference_memory(self):
    for seed in range(10):
      with self.subTest(seed=seed):
        rng = random.Random(seed)
        system = Zhujiang(buffer_capacity=2, max_transactions=1)
        expected = {}
        for index in range(100):
          cc = rng.choice((system.cc0, system.cc1))
          address = rng.choice(("A", "B", "C", "D"))
          operation = rng.choice(("write", "read", "read_shared", "read_unique"))
          if operation == "write":
            value = f"{seed}:{index}"
            request = cc.write(address, value)
            expected[address] = value
          else:
            request = getattr(cc, operation)(address)
          system.run_until_idle(200)
          if operation != "write":
            actual = cc.cache[address][1] if request is None else cc.read_response_for(request).payload
            self.assertEqual(expected.get(address, "read data"), actual)
          self.assertEqual(expected, system.s.dj.data_by_address)
          self.assert_idle_consistent(system)
