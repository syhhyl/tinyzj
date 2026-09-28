import unittest
from collections import Counter

from tinyzj.zj import Zhujiang


class LinkCreditTest(unittest.TestCase):

  def test_credit_conservation_with_delayed_returns(self):
    for delay in (1, 3, 8):
      with self.subTest(delay=delay):
        system = Zhujiang(buffer_capacity=2, credit_return_delay=delay,
                          max_transactions=1, retry_enabled=True, id_capacity=2)
        requests = [(system.cc0, system.cc0.write(str(i), str(i))) for i in range(8)]
        requests += [(system.cc1, system.cc1.read_shared(str(i))) for i in range(8)]
        for _ in range(2000):
          system.step()
          ring = system.ring
          returning = Counter(key for _, key in ring.credit_returns)
          for key, credit in ring.link_credits.items():
            node, direction, channel = key
            occupied = len(ring.ring_buffers[node][(direction, channel)])
            self.assertGreaterEqual(credit, 0)
            self.assertEqual(2, credit + occupied + returning[key])
          if not ring.in_flight and not system.cc0.has_pending_requests() and not system.cc1.has_pending_requests():
            break
        self.assertFalse(system.ring.in_flight)
        for cc, request in requests:
          result = cc.write_response_for(request) if cc is system.cc0 else cc.read_response_for(request)
          self.assertIsNotNone(result)
        for _ in range(delay):
          system.step()
        self.assertFalse(system.ring.credit_returns)
        self.assertTrue(all(value == 2 for value in system.ring.link_credits.values()))
