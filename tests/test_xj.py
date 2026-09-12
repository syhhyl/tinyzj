import unittest

from tinyzj.xj import Message, Ring, RingNode


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


class TransportTest(unittest.TestCase):
  def test_step_moves_message_one_hop_at_a_time(self):
    """Each step advances a message by one forward ring hop."""
    ring = make_ring()
    injection = ring.inject(Message("cc", "io", "read request"))

    ring.step()
    self.assertEqual("home", injection.current_node_name)

    ring.step()
    self.assertEqual("io", injection.current_node_name)

  def test_step_wraps_around_and_stops_at_target(self):
    """Transport wraps around once and does not pass its target."""
    ring = make_ring()
    injection = ring.inject(Message("io", "home", "write request"))

    ring.step()
    self.assertEqual("cc", injection.current_node_name)

    ring.step()
    self.assertEqual("home", injection.current_node_name)

    ring.step()
    self.assertEqual("home", injection.current_node_name)

  def test_step_leaves_an_empty_ring_unchanged(self):
    """Stepping an empty ring creates no in-flight messages."""
    ring = make_ring()

    ring.step()

    self.assertEqual([], ring.in_flight)

  def test_step_keeps_a_local_message_at_its_target(self):
    """A message addressed to its source does not circulate."""
    ring = make_ring()
    injection = ring.inject(Message("cc", "cc", "local request"))

    ring.step()

    self.assertEqual("cc", injection.current_node_name)


if __name__ == "__main__":
  unittest.main()
