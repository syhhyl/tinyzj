import unittest

from tinyzj.chi import Channel
from tinyzj.xj import Message, Ring, RingNode


def make_ring():
  return Ring([
    RingNode("cc", "CPU cluster"),
    RingNode("home", "coherent home"),
    RingNode("io", "memory and IO"),
  ])


def make_ring_with_capacity(capacity):
  return Ring([
    RingNode("cc", "CPU cluster"),
    RingNode("home", "coherent home"),
    RingNode("io", "memory and IO"),
  ], buffer_capacity=capacity)


def make_square_ring(capacity=None):
  return Ring([
    RingNode("n00", "CC0"),
    RingNode("n01", "HF"),
    RingNode("n11", "CC1"),
    RingNode("n10", "S"),
  ], buffer_capacity=capacity)


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

    for channel in (None, "HRQ"):
      with self.subTest(channel=channel):
        with self.assertRaisesRegex(ValueError, "message needs a known channel"):
          ring.inject(Message("cc", "io", channel=channel))
    self.assertEqual([], ring.in_flight)

  def test_rejects_invalid_buffer_capacity(self):
    for capacity in (0, 1, 2.5, "2"):
      with self.subTest(capacity=capacity):
        with self.assertRaisesRegex(ValueError, "buffer_capacity must be an integer >= 2"):
          make_ring_with_capacity(capacity)


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

  def test_bubble_rule_blocks_full_next_buffer_but_isolated_by_channel_and_direction(self):
    ring = make_square_ring(capacity=2)
    connect_recording_endpoints(ring)
    waiting = ring.inject(Message("n00", "n01", channel=Channel.REQ))
    ring.step()
    self.assertTrue(waiting.in_ring)

    blocked = ring.inject(Message("n00", "n01", channel=Channel.REQ))
    other_channel = ring.inject(Message("n00", "n01", channel=Channel.DAT))
    other_direction = ring.inject(Message("n11", "n01", channel=Channel.REQ))
    ring.step()

    self.assertFalse(blocked.in_ring)
    self.assertTrue(other_channel.in_ring)
    self.assertTrue(other_direction.in_ring)

  def test_on_ring_forwarding_has_priority_over_source_injection(self):
    ring = make_square_ring()
    on_ring = ring.inject(Message("n10", "n01", channel=Channel.REQ))
    ring.step()
    self.assertEqual("n00", on_ring.current_node_name)

    source_queue = ring.inject(Message("n00", "n11", channel=Channel.REQ))
    ring.step()

    self.assertEqual("n01", on_ring.current_node_name)
    self.assertEqual("n00", source_queue.current_node_name)
    self.assertFalse(source_queue.in_ring)

  def test_buffer_snapshot_blocks_forwarding_even_when_buffer_drains(self):
    ring = make_square_ring(capacity=2)
    endpoints = connect_recording_endpoints(ring)
    at_target = ring.inject(Message("n00", "n01", channel=Channel.REQ))
    leaving = ring.inject(Message("n00", "n11", channel=Channel.REQ))
    incoming = ring.inject(Message("n10", "n01", channel=Channel.REQ))

    for injection in (at_target, leaving):
      ring.source_queues[injection.message.source_name][Channel.REQ].remove(injection)
      injection.in_ring = True
      injection.current_node_name = "n01"
      ring.ring_buffers["n01"][(1, Channel.REQ)].append(injection)
    ring.source_queues["n10"][Channel.REQ].remove(incoming)
    incoming.in_ring = True
    incoming.current_node_name = "n00"
    ring.ring_buffers["n00"][(1, Channel.REQ)].append(incoming)

    ring.step()

    self.assertEqual([at_target.message], endpoints["n01"].received_messages)
    self.assertEqual("n11", leaving.current_node_name)
    self.assertEqual("n00", incoming.current_node_name)


class GatedEndpoint:

  def __init__(self):
    self.received_messages = []
    self.open = False

  def can_receive(self, message):
    return self.open

  def receive(self, message):
    self.received_messages.append(message)


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

  def test_step_delivers_one_same_channel_message_per_node_per_step(self):
    """One same-channel message delivers per node per step."""
    ring = make_ring()
    endpoints = connect_recording_endpoints(ring)
    messages = [
      Message("home", "home", "first local request", channel=Channel.REQ),
      Message("home", "home", "second local request", channel=Channel.REQ),
    ]
    for message in messages:
      ring.inject(message)

    ring.step()

    self.assertEqual([messages[0]], endpoints["home"].received_messages)
    self.assertEqual(1, len(ring.in_flight))

    ring.step()

    self.assertEqual(messages, endpoints["home"].received_messages)
    self.assertEqual([], ring.in_flight)

  def test_step_delivers_different_channel_messages_in_the_same_step(self):
    """Different-channel messages deliver in the same step."""
    ring = make_ring()
    endpoints = connect_recording_endpoints(ring)
    messages = [
      Message("home", "home", "local request", channel=Channel.REQ),
      Message("home", "home", "local data", channel=Channel.DAT),
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

  def test_step_delivers_a_local_message(self):
    """A message addressed to its source is delivered immediately."""
    ring = make_ring()
    endpoints = connect_recording_endpoints(ring)
    message = Message("cc", "cc", "local request", channel=Channel.REQ)
    ring.inject(message)

    ring.step()

    self.assertEqual([message], endpoints["cc"].received_messages)
    self.assertEqual([], ring.in_flight)

  def test_can_receive_false_holds_the_message_until_it_turns_true(self):
    """can_receive False holds the message until it turns True."""
    ring = make_ring()
    gate = GatedEndpoint()
    ring.connect("io", gate)
    ring.connect("cc", RecordingEndpoint())
    ring.connect("home", RecordingEndpoint())
    injection = ring.inject(Message("cc", "io", "read request", channel=Channel.REQ))

    ring.step()
    ring.step()

    self.assertEqual([], gate.received_messages)
    self.assertEqual([injection], ring.in_flight)

    gate.open = True
    ring.step()

    self.assertEqual([injection.message], gate.received_messages)
    self.assertEqual([], ring.in_flight)

  def test_blocked_local_message_does_not_hold_back_its_source_queue(self):
    """A blocked local message leaves its source queue free to inject."""
    ring = make_ring()
    gate = GatedEndpoint()
    cc_endpoint = RecordingEndpoint()
    ring.connect("io", gate)
    ring.connect("cc", cc_endpoint)
    ring.connect("home", RecordingEndpoint())
    local = ring.inject(Message("io", "io", "local request", channel=Channel.REQ))
    outgoing = ring.inject(Message("io", "cc", "forwarded request", channel=Channel.REQ))

    ring.step()

    self.assertEqual([], gate.received_messages)
    self.assertFalse(local.in_ring)
    self.assertTrue(outgoing.in_ring)
    self.assertEqual("cc", outgoing.current_node_name)
    self.assertEqual([local, outgoing], ring.in_flight)

    gate.open = True
    ring.step()

    self.assertEqual([local.message], gate.received_messages)
    self.assertEqual([outgoing.message], cc_endpoint.received_messages)
    self.assertEqual([], ring.in_flight)


if __name__ == "__main__":
  unittest.main()
