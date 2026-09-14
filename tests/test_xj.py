import unittest

from tinyzj.xj import Message, Ring, RingNode


def make_ring():
  return Ring([
    RingNode("cc", "CPU cluster"),
    RingNode("home", "coherent home"),
    RingNode("io", "memory and IO"),
  ])


class RecordingEndpoint:

  def __init__(self):
    self.received_messages = []

  def receive(self, message):
    self.received_messages.append(message)


class RejectingEndpoint(RecordingEndpoint):

  def __init__(self, rejected_message):
    super().__init__()
    self.rejected_message = rejected_message

  def receive(self, message):
    super().receive(message)
    if message is self.rejected_message:
      raise ValueError("message rejected")


class InjectingRejectingEndpoint(RejectingEndpoint):

  def __init__(
    self,
    ring,
    trigger_message,
    injected_message,
    rejected_message,
  ):
    super().__init__(rejected_message)
    self.ring = ring
    self.trigger_message = trigger_message
    self.injected_message = injected_message
    self.injected = None

  def receive(self, message):
    super().receive(message)
    if message is self.trigger_message:
      self.injected = self.ring.inject(self.injected_message)


def connect_recording_endpoints(ring):
  endpoints = {}
  for node in ring.nodes:
    endpoint = RecordingEndpoint()
    ring.connect(node.name, endpoint)
    endpoints[node.name] = endpoint
  return endpoints


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
    self.assertEqual(0, message.transaction_id)
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

  def test_step_wraps_around_to_target(self):
    """Transport wraps around once to reach its target."""
    ring = make_ring()
    injection = ring.inject(Message("io", "home", "write request"))

    ring.step()
    self.assertEqual("cc", injection.current_node_name)

    ring.step()
    self.assertEqual("home", injection.current_node_name)


class DeliveryTest(unittest.TestCase):
  def test_step_delivers_an_arrived_message_on_the_next_step(self):
    """Arrival and delivery occur on separate steps."""
    ring = make_ring()
    endpoints = connect_recording_endpoints(ring)
    message = Message("cc", "io", "read request")
    injection = ring.inject(message)

    ring.step()
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
      Message("home", "home", "first local request"),
      Message("home", "home", "second local request"),
    ]
    for message in messages:
      ring.inject(message)

    ring.step()

    self.assertEqual(messages, endpoints["home"].received_messages)
    self.assertEqual([], ring.in_flight)

  def test_step_commits_successful_deliveries_before_a_later_failure(self):
    """Only failed and unattempted arrivals remain after a delivery error."""
    ring = make_ring()
    first_message = Message("home", "home", "first local request")
    rejected_message = Message("home", "home", "rejected local request")
    unattempted_message = Message("home", "home", "unattempted local request")
    moving_message = Message("cc", "io", "moving request")
    injected_message = Message("home", "io", "injected response")
    endpoint = InjectingRejectingEndpoint(
      ring,
      first_message,
      injected_message,
      rejected_message,
    )
    ring.connect("home", endpoint)
    first = ring.inject(first_message)
    rejected = ring.inject(rejected_message)
    unattempted = ring.inject(unattempted_message)
    moving = ring.inject(moving_message)

    with self.assertRaisesRegex(ValueError, "message rejected"):
      ring.step()

    self.assertNotIn(first, ring.in_flight)
    self.assertEqual(
      [rejected, unattempted, moving, endpoint.injected],
      ring.in_flight,
    )
    self.assertEqual(
      [first_message, rejected_message],
      endpoint.received_messages,
    )
    self.assertEqual("cc", moving.current_node_name)
    self.assertEqual("home", endpoint.injected.current_node_name)

  def test_step_retries_a_failed_head_before_later_arrived_messages(self):
    """A persistent head failure prevents later arrivals from being attempted."""
    ring = make_ring()
    rejected_message = Message("home", "home", "rejected local request")
    later_message = Message("home", "home", "later local request")
    endpoint = RejectingEndpoint(rejected_message)
    ring.connect("home", endpoint)
    injections = [
      ring.inject(rejected_message),
      ring.inject(later_message),
    ]

    for attempt_count in (1, 2):
      with self.subTest(attempt_count=attempt_count):
        with self.assertRaisesRegex(ValueError, "message rejected"):
          ring.step()
        self.assertEqual(injections, ring.in_flight)
        self.assertEqual(
          [rejected_message] * attempt_count,
          endpoint.received_messages,
        )

  def test_step_rejects_delivery_to_an_unconnected_target(self):
    """An arrived message remains in flight when its target is disconnected."""
    ring = make_ring()
    injection = ring.inject(Message("cc", "home", "read request"))

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
    message = Message("cc", "cc", "local request")
    ring.inject(message)

    ring.step()

    self.assertEqual([message], endpoints["cc"].received_messages)
    self.assertEqual([], ring.in_flight)


if __name__ == "__main__":
  unittest.main()
