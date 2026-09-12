import unittest

from xj import Message, Ring, RingNode


def make_ring():
  return Ring([
    RingNode("cc", "CPU cluster"),
    RingNode("home", "coherent home"),
    RingNode("io", "memory and IO"),
  ])


class TopologyTest(unittest.TestCase):
  def test_path_wraps_around(self):
    """Path wraps around."""
    ring = make_ring()

    previous, following = ring.neighbors_of("cc")

    self.assertEqual("io", previous.name)
    self.assertEqual("home", following.name)
    self.assertEqual(
      ["io", "cc", "home"],
      [node.name for node in ring.path_from("io", "home")],
    )


class InjectionTest(unittest.TestCase):
  def test_request_stays_at_source(self):
    """Request stays at source."""
    ring = make_ring()
    message = Message("cc", "io", "read request")

    injection = ring.inject(message)

    self.assertIs(injection.message, message)
    self.assertEqual("cc", injection.current_node_name)
    self.assertEqual([injection], ring.in_flight)

  def test_rejects_unknown_endpoint(self):
    """Reject unknown endpoint."""
    ring = make_ring()

    with self.assertRaisesRegex(ValueError, "unknown ring node: dma"):
      ring.inject(Message("dma", "io"))
    with self.assertRaisesRegex(ValueError, "unknown ring node: dma"):
      ring.inject(Message("cc", "dma"))
    self.assertEqual([], ring.in_flight)


if __name__ == "__main__":
  unittest.main()
