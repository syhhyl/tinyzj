import unittest

from tinyzj.chi import Channel
from tinyzj.xj import Message, Ring, RingNode


def make_ring():
  return Ring([
    RingNode("cc", "CPU cluster"),
    RingNode("home", "coherent home"),
    RingNode("io", "memory and IO"),
  ])


def make_square_ring():
  return Ring([
    RingNode("n00", "CC0"),
    RingNode("n01", "HF"),
    RingNode("n11", "CC1"),
    RingNode("n10", "S"),
  ])


class RecordingEndpoint:

  def __init__(self):
    self.received_messages = []

  def receive(self, message):
    self.received_messages.append(message)


def connect_recording_endpoints(ring):
  endpoints = {}
  for node in ring.nodes:
    endpoint = RecordingEndpoint()
    ring.connect(node.name, endpoint)
    endpoints[node.name] = endpoint
  return endpoints


class TopologyTest(unittest.TestCase):
  def test_three_node_path_uses_the_nearest_neighbor(self):
    ring = make_ring()

    previous, following = ring.neighbors_of("cc")

    self.assertEqual("io", previous.name)
    self.assertEqual("home", following.name)
    self.assertEqual(
      ["io", "home"],
      [node.name for node in ring.path_from("io", "home")],
    )

  def test_square_ring_neighbors_follow_the_perimeter(self):
    ring = make_square_ring()
    expected = {
      "n00": ("n10", "n01"),
      "n01": ("n00", "n11"),
      "n11": ("n01", "n10"),
      "n10": ("n11", "n00"),
    }

    for name, neighbor_names in expected.items():
      with self.subTest(node=name):
        self.assertEqual(
          neighbor_names,
          tuple(node.name for node in ring.neighbors_of(name)),
        )

  def test_square_ring_paths_cover_adjacent_diagonal_and_wraparound(self):
    ring = make_square_ring()
    expected = {
      ("n00", "n01"): ["n00", "n01"],
      ("n01", "n00"): ["n01", "n00"],
      ("n00", "n11"): ["n00", "n01", "n11"],
      ("n11", "n00"): ["n11", "n10", "n00"],
      ("n10", "n01"): ["n10", "n00", "n01"],
      ("n10", "n00"): ["n10", "n00"],
    }

    for (source, target), path in expected.items():
      with self.subTest(source=source, target=target):
        self.assertEqual(
          path,
          [node.name for node in ring.path_from(source, target)],
        )

  def test_square_ring_returns_to_each_start_after_four_forward_hops(self):
    ring = make_square_ring()

    for start in ("n00", "n01", "n11", "n10"):
      with self.subTest(start=start):
        current = start
        for _ in range(4):
          _, next_node = ring.neighbors_of(current)
          current = next_node.name
        self.assertEqual(start, current)


class InjectionTest(unittest.TestCase):
  def test_request_stays_at_source(self):
    """Request stays at source."""
    ring = make_ring()
    message = Message("cc", "io", "read request", channel=Channel.REQ)

    injection = ring.inject(message)

    self.assertIs(injection.message, message)
    self.assertEqual("cc", injection.current_node_name)
    self.assertIsNone(message.transaction_id)
    self.assertEqual([injection], ring.in_flight)

  def test_ring_preserves_present_and_missing_transaction_ids(self):
    ring = make_ring()
    present = Message("cc", "io", transaction_id=7, channel=Channel.REQ)
    missing = Message("cc", "io", channel=Channel.REQ)
    present_injection = ring.inject(present)
    missing_injection = ring.inject(missing)
    endpoints = connect_recording_endpoints(ring)

    ring.step()
    ring.step()
    ring.step()

    self.assertEqual(7, present.transaction_id)
    self.assertIsNone(missing.transaction_id)
    self.assertEqual([present, missing], endpoints["io"].received_messages)
    self.assertEqual([], ring.in_flight)

  def test_rejects_unknown_endpoint(self):
    """Reject unknown endpoint."""
    ring = make_ring()

    with self.assertRaisesRegex(ValueError, "unknown ring node: dma"):
      ring.inject(Message("dma", "io", channel=Channel.REQ))
    with self.assertRaisesRegex(ValueError, "unknown ring node: dma"):
      ring.inject(Message("cc", "dma", channel=Channel.REQ))
    self.assertEqual([], ring.in_flight)

  def test_rejects_unknown_or_missing_channel(self):
    ring = make_ring()

    for channel in (None, "SNP"):
      with self.subTest(channel=channel):
        with self.assertRaisesRegex(ValueError, "message needs a known channel"):
          ring.inject(Message("cc", "io", channel=channel))
    self.assertEqual([], ring.in_flight)


class TransportTest(unittest.TestCase):
  def test_step_moves_message_one_hop_at_a_time(self):
    ring = make_square_ring()
    injection = ring.inject(Message("n00", "n11", channel=Channel.REQ))

    ring.step()
    self.assertEqual("n01", injection.current_node_name)

    ring.step()
    self.assertEqual("n11", injection.current_node_name)

  def test_step_uses_the_backward_neighbor_when_it_is_closer(self):
    ring = make_square_ring()
    injection = ring.inject(Message("n01", "n00", channel=Channel.REQ))

    ring.step()
    self.assertEqual("n00", injection.current_node_name)

  def test_same_channel_contention_moves_earlier_injection_first(self):
    ring = make_square_ring()
    first = ring.inject(Message("n00", "n11", channel=Channel.REQ))
    second = ring.inject(Message("n00", "n11", channel=Channel.REQ))

    ring.step()

    self.assertEqual("n01", first.current_node_name)
    self.assertEqual("n00", second.current_node_name)

    endpoints = connect_recording_endpoints(ring)
    for _ in range(3):
      ring.step()
    self.assertEqual([first.message, second.message], endpoints["n11"].received_messages)

  def test_different_channels_do_not_compete_for_link_bandwidth(self):
    ring = make_square_ring()
    req = ring.inject(Message("n00", "n11", channel=Channel.REQ))
    dat = ring.inject(Message("n00", "n11", channel=Channel.DAT))

    ring.step()

    self.assertEqual("n01", req.current_node_name)
    self.assertEqual("n01", dat.current_node_name)

  def test_opposite_directions_are_distinct_directed_links(self):
    ring = make_square_ring()
    forward = ring.inject(Message("n00", "n01", channel=Channel.REQ))
    backward = ring.inject(Message("n01", "n00", channel=Channel.REQ))

    ring.step()

    self.assertEqual("n01", forward.current_node_name)
    self.assertEqual("n00", backward.current_node_name)

  def test_injection_records_direction_and_follows_it(self):
    ring = make_square_ring()
    injection = ring.inject(Message("n10", "n01", channel=Channel.REQ))

    self.assertEqual(1, injection.direction)
    for expected in ("n00", "n01"):
      ring.step()
      self.assertEqual(expected, injection.current_node_name)


class DeliveryTest(unittest.TestCase):
  def test_step_delivers_an_arrived_message_on_the_next_step(self):
    """Arrival and delivery occur on separate steps."""
    ring = make_ring()
    endpoints = connect_recording_endpoints(ring)
    message = Message("cc", "io", "read request", channel=Channel.REQ)
    injection = ring.inject(message)

    ring.step()

    self.assertEqual("io", injection.current_node_name)
    self.assertEqual([], endpoints["io"].received_messages)
    self.assertEqual([injection], ring.in_flight)

    ring.step()

    self.assertEqual([message], endpoints["io"].received_messages)
    self.assertEqual([], ring.in_flight)

  def test_step_delivers_all_messages_that_are_already_at_a_target(self):
    """All messages already at a target are delivered in one step."""
    ring = make_ring()
    endpoints = connect_recording_endpoints(ring)
    messages = [
      Message("home", "home", "first local request", channel=Channel.REQ),
      Message("home", "home", "second local request", channel=Channel.REQ),
    ]
    for message in messages:
      ring.inject(message)

    ring.step()

    self.assertEqual(messages, endpoints["home"].received_messages)
    self.assertEqual([], ring.in_flight)

  def test_step_rejects_delivery_to_an_unconnected_target(self):
    """An arrived message remains in flight when its target is disconnected."""
    ring = make_ring()
    injection = ring.inject(Message("cc", "home", "read request", channel=Channel.REQ))

    ring.step()
    with self.assertRaisesRegex(ValueError, "ring node is not connected: home"):
      ring.step()

    self.assertEqual([injection], ring.in_flight)

  def test_step_leaves_an_empty_ring_unchanged(self):
    """Stepping an empty ring creates no in-flight messages."""
    ring = make_ring()

    ring.step()

    self.assertEqual([], ring.in_flight)

  def test_step_delivers_a_local_message(self):
    """A message addressed to its source is delivered immediately."""
    ring = make_ring()
    endpoints = connect_recording_endpoints(ring)
    message = Message("cc", "cc", "local request", channel=Channel.REQ)
    ring.inject(message)

    ring.step()

    self.assertEqual([message], endpoints["cc"].received_messages)
    self.assertEqual([], ring.in_flight)


if __name__ == "__main__":
  unittest.main()
