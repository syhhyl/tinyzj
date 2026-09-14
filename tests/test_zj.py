import unittest

from tinyzj.xj import Message
from tinyzj.zj import Zhujiang


class ZhujiangTest(unittest.TestCase):
  def test_home_returns_a_read_response_to_the_requester(self):
    """A read request makes a round trip between Socket and Home."""
    zhujiang = Zhujiang()
    request = zhujiang.socket.read("0x1000")

    self.assertEqual([request], zhujiang.socket.sent_messages)
    self.assertEqual("cc", request.source_name)
    self.assertEqual("home", request.target_name)
    self.assertEqual("read_request", request.message_type)
    self.assertEqual("0x1000", request.address)
    self.assertIsNone(request.payload)
    self.assertEqual(0, request.transaction_id)
    self.assertIsNone(zhujiang.socket.read_response_for(request))

    zhujiang.step()
    zhujiang.step()

    self.assertEqual([request], zhujiang.home.received_messages)
    self.assertEqual(1, len(zhujiang.home.sent_messages))
    response = zhujiang.home.sent_messages[0]
    self.assertEqual("home", response.source_name)
    self.assertEqual("cc", response.target_name)
    self.assertEqual("read_response", response.message_type)
    self.assertEqual("read data", response.payload)
    self.assertEqual(request.address, response.address)
    self.assertFalse(response.data_present)
    self.assertEqual(request.transaction_id, response.transaction_id)
    self.assertEqual("home", zhujiang.ring.in_flight[0].current_node_name)

    zhujiang.step()
    self.assertEqual("io", zhujiang.ring.in_flight[0].current_node_name)

    zhujiang.step()
    self.assertEqual("cc", zhujiang.ring.in_flight[0].current_node_name)

    zhujiang.step()
    self.assertEqual([response], zhujiang.socket.received_messages)
    self.assertIs(response, zhujiang.socket.read_response_for(request))
    self.assertEqual([], zhujiang.ring.in_flight)

  def test_run_until_idle_completes_a_read_request(self):
    """The top-level helper advances the full read round trip."""
    zhujiang = Zhujiang()
    request = zhujiang.socket.read("0x1000")

    steps = zhujiang.run_until_idle()

    self.assertEqual(5, steps)
    self.assertIsNotNone(zhujiang.socket.read_response_for(request))
    self.assertEqual([], zhujiang.ring.in_flight)

  def test_run_until_idle_leaves_an_idle_system_unchanged(self):
    """No steps are needed when no messages are in flight."""
    zhujiang = Zhujiang()

    self.assertEqual(0, zhujiang.run_until_idle())

  def test_run_until_idle_rejects_an_insufficient_step_limit(self):
    """The helper must not loop forever when work remains."""
    zhujiang = Zhujiang()
    zhujiang.socket.read("0x1000")

    with self.assertRaisesRegex(RuntimeError, "system did not become idle"):
      zhujiang.run_until_idle(max_steps=4)

  def test_run_until_idle_rejects_a_negative_step_limit(self):
    """A step limit cannot be negative."""
    zhujiang = Zhujiang()

    with self.assertRaisesRegex(ValueError, "max_steps must be non-negative"):
      zhujiang.run_until_idle(max_steps=-1)

  def test_socket_rejects_a_read_without_an_address(self):
    """A read must identify the Home data to fetch."""
    zhujiang = Zhujiang()

    with self.assertRaisesRegex(ValueError, "read request needs an address"):
      zhujiang.socket.read(None)

    self.assertEqual([], zhujiang.socket.sent_messages)
    self.assertEqual([], zhujiang.ring.in_flight)

  def test_socket_rejects_a_write_without_an_address(self):
    """A write must identify the Home data to change."""
    zhujiang = Zhujiang()

    with self.assertRaisesRegex(ValueError, "write request needs an address"):
      zhujiang.socket.write(None, "value 1")

    self.assertEqual([], zhujiang.socket.sent_messages)
    self.assertEqual([], zhujiang.ring.in_flight)

  def test_io_wrapper_returns_a_response_to_the_requester(self):
    """An io request makes a round trip between Socket and IoWrapper."""
    zhujiang = Zhujiang()
    request = zhujiang.socket.io_request("device 0")

    self.assertEqual([request], zhujiang.socket.sent_messages)
    self.assertEqual("cc", request.source_name)
    self.assertEqual("io", request.target_name)
    self.assertEqual("io_request", request.message_type)
    self.assertEqual("device 0", request.payload)
    self.assertEqual(0, request.transaction_id)
    self.assertIsNone(zhujiang.socket.io_response_for(request))

    steps = zhujiang.run_until_idle()

    self.assertEqual(5, steps)
    self.assertEqual([request], zhujiang.io_wrapper.received_messages)
    self.assertEqual(1, len(zhujiang.io_wrapper.sent_messages))
    response = zhujiang.io_wrapper.sent_messages[0]
    self.assertEqual("io", response.source_name)
    self.assertEqual("cc", response.target_name)
    self.assertEqual("io_response", response.message_type)
    self.assertEqual("io data", response.payload)
    self.assertEqual(request.transaction_id, response.transaction_id)
    self.assertEqual([response], zhujiang.socket.received_messages)
    self.assertIs(response, zhujiang.socket.io_response_for(request))

  def test_socket_matches_multiple_io_responses(self):
    """Each io request can query its own response."""
    zhujiang = Zhujiang()
    requests = [
      zhujiang.socket.io_request("device 0"),
      zhujiang.socket.io_request("device 1"),
    ]

    zhujiang.run_until_idle()

    self.assertEqual([0, 1], [request.transaction_id for request in requests])
    for request, response in zip(requests, zhujiang.io_wrapper.sent_messages):
      self.assertIs(response, zhujiang.socket.io_response_for(request))

  def test_io_wrapper_does_not_respond_to_other_message_types(self):
    """Only the teaching io request has a response in this iteration."""
    zhujiang = Zhujiang()
    request = Message("cc", "io", message_type="read_request")
    zhujiang.ring.inject(request)

    zhujiang.run_until_idle()

    self.assertEqual([request], zhujiang.io_wrapper.received_messages)
    self.assertEqual([], zhujiang.io_wrapper.sent_messages)

  def test_home_returns_a_write_response_to_the_requester(self):
    """A write request makes a round trip between Socket and Home."""
    zhujiang = Zhujiang()
    request = zhujiang.socket.write("0x1000", "value 1")

    self.assertEqual([request], zhujiang.socket.sent_messages)
    self.assertEqual("cc", request.source_name)
    self.assertEqual("home", request.target_name)
    self.assertEqual("write_request", request.message_type)
    self.assertEqual("0x1000", request.address)
    self.assertEqual("value 1", request.payload)
    self.assertEqual(0, request.transaction_id)
    self.assertIsNone(zhujiang.socket.write_response_for(request))

    zhujiang.run_until_idle()

    self.assertEqual([request], zhujiang.home.received_messages)
    self.assertEqual(1, len(zhujiang.home.sent_messages))
    response = zhujiang.home.sent_messages[0]
    self.assertEqual("home", response.source_name)
    self.assertEqual("cc", response.target_name)
    self.assertEqual("write_response", response.message_type)
    self.assertEqual("write complete", response.payload)
    self.assertEqual(request.address, response.address)
    self.assertEqual(request.transaction_id, response.transaction_id)
    self.assertEqual([response], zhujiang.socket.received_messages)
    self.assertIs(response, zhujiang.socket.write_response_for(request))

  def test_home_reads_data_written_to_the_same_address(self):
    """A completed write changes a later read at the same address."""
    zhujiang = Zhujiang()
    write_request = zhujiang.socket.write("0x1000", "value 1")

    zhujiang.run_until_idle()
    read_request = zhujiang.socket.read("0x1000")
    zhujiang.run_until_idle()

    self.assertEqual("value 1", zhujiang.home.dj.data_by_address["0x1000"])
    self.assertIsNotNone(zhujiang.socket.write_response_for(write_request))
    read_response = zhujiang.socket.read_response_for(read_request)
    self.assertEqual("0x1000", read_response.address)
    self.assertEqual("value 1", read_response.payload)
    self.assertTrue(read_response.data_present)

  def test_home_keeps_data_for_different_addresses_separate(self):
    """Writes at different addresses do not overwrite each other."""
    zhujiang = Zhujiang()
    zhujiang.socket.write("0x1000", "value 1")
    zhujiang.socket.write("0x2000", "value 2")

    zhujiang.run_until_idle()
    first_read = zhujiang.socket.read("0x1000")
    second_read = zhujiang.socket.read("0x2000")
    zhujiang.run_until_idle()

    self.assertEqual("value 1", zhujiang.socket.read_response_for(first_read).payload)
    self.assertEqual("value 2", zhujiang.socket.read_response_for(second_read).payload)

  def test_home_reads_the_last_write_to_an_address(self):
    """Later writes replace earlier writes at the same address."""
    zhujiang = Zhujiang()
    first_write = zhujiang.socket.write("0x1000", "value 1")
    second_write = zhujiang.socket.write("0x1000", "value 2")

    zhujiang.run_until_idle()
    read_request = zhujiang.socket.read("0x1000")
    zhujiang.run_until_idle()

    self.assertIsNotNone(zhujiang.socket.write_response_for(first_write))
    self.assertIsNotNone(zhujiang.socket.write_response_for(second_write))
    self.assertEqual("value 2", zhujiang.home.dj.data_by_address["0x1000"])
    self.assertEqual(
      "value 2",
      zhujiang.socket.read_response_for(read_request).payload,
    )

  def test_home_processes_a_batch_write_before_read_in_injection_order(self):
    """A same-address read observes an earlier write in the same batch."""
    zhujiang = Zhujiang()
    write_request = zhujiang.socket.write("0x1000", "value 1")
    read_request = zhujiang.socket.read("0x1000")

    zhujiang.run_until_idle()

    self.assertEqual(
      [write_request, read_request],
      zhujiang.home.received_messages,
    )
    read_response = zhujiang.socket.read_response_for(read_request)
    self.assertEqual("value 1", read_response.payload)
    self.assertTrue(read_response.data_present)
    self.assertIsNotNone(zhujiang.socket.write_response_for(write_request))

  def test_home_processes_a_batch_read_before_write_in_injection_order(self):
    """A same-address read observes the old value before a later write."""
    zhujiang = Zhujiang()
    read_request = zhujiang.socket.read("0x1000")
    write_request = zhujiang.socket.write("0x1000", "value 1")

    zhujiang.run_until_idle()

    self.assertEqual(
      [read_request, write_request],
      zhujiang.home.received_messages,
    )
    read_response = zhujiang.socket.read_response_for(read_request)
    self.assertEqual("read data", read_response.payload)
    self.assertFalse(read_response.data_present)
    self.assertEqual("value 1", zhujiang.home.dj.data_by_address["0x1000"])
    self.assertIsNotNone(zhujiang.socket.write_response_for(write_request))

  def test_socket_matches_mixed_batch_responses_by_type_and_transaction_id(self):
    """Mixed responses remain associated with their original requests."""
    zhujiang = Zhujiang()
    write_request = zhujiang.socket.write("0x1000", "value 1")
    io_request = zhujiang.socket.io_request("device 0")
    read_request = zhujiang.socket.read("0x1000")

    zhujiang.run_until_idle()

    self.assertEqual([0, 1, 2], [
      write_request.transaction_id,
      io_request.transaction_id,
      read_request.transaction_id,
    ])
    write_response = zhujiang.socket.write_response_for(write_request)
    io_response = zhujiang.socket.io_response_for(io_request)
    read_response = zhujiang.socket.read_response_for(read_request)
    self.assertEqual("write complete", write_response.payload)
    self.assertEqual("io data", io_response.payload)
    self.assertEqual("value 1", read_response.payload)
    self.assertTrue(read_response.data_present)
    self.assertEqual(
      ["write_response", "read_response", "io_response"],
      [message.message_type for message in zhujiang.socket.received_messages],
    )

  def test_socket_queries_change_as_staggered_responses_arrive(self):
    """Queries expose no response, partial completion, then full completion."""
    zhujiang = Zhujiang()
    read_request = zhujiang.socket.read("0x1000")

    zhujiang.step()
    io_request = zhujiang.socket.io_request("device 0")
    zhujiang.step()
    zhujiang.step()
    zhujiang.step()

    self.assertIsNone(zhujiang.socket.read_response_for(read_request))
    self.assertIsNone(zhujiang.socket.io_response_for(io_request))

    zhujiang.step()

    read_response = zhujiang.socket.read_response_for(read_request)
    self.assertIsNotNone(read_response)
    self.assertIsNone(zhujiang.socket.io_response_for(io_request))
    self.assertEqual([read_response], zhujiang.socket.received_messages)
    self.assertNotEqual([], zhujiang.ring.in_flight)

    zhujiang.step()

    io_response = zhujiang.socket.io_response_for(io_request)
    self.assertIsNotNone(io_response)
    self.assertEqual(
      [read_response, io_response],
      zhujiang.socket.received_messages,
    )
    self.assertEqual([], zhujiang.ring.in_flight)
    self.assertEqual(0, zhujiang.run_until_idle())
    self.assertIs(read_response, zhujiang.socket.read_response_for(read_request))
    self.assertIs(io_response, zhujiang.socket.io_response_for(io_request))

  def test_socket_matches_multiple_write_responses(self):
    """Each write request can query its own response."""
    zhujiang = Zhujiang()
    requests = [
      zhujiang.socket.write("0x1000", "value 1"),
      zhujiang.socket.write("0x2000", "value 2"),
    ]

    zhujiang.run_until_idle()

    self.assertEqual([0, 1], [request.transaction_id for request in requests])
    for request, response in zip(requests, zhujiang.home.sent_messages):
      self.assertIs(response, zhujiang.socket.write_response_for(request))

  def test_home_does_not_respond_to_unsupported_message_types(self):
    """Only teaching read and write requests receive Home responses."""
    zhujiang = Zhujiang()
    request = Message("cc", "home", message_type="other_request")
    zhujiang.ring.inject(request)

    zhujiang.run_until_idle()

    self.assertEqual([request], zhujiang.home.received_messages)
    self.assertEqual([], zhujiang.home.sent_messages)

  def test_home_rejects_direct_requests_without_an_address(self):
    """Home protects its state when a request bypasses Socket."""
    for message_type in ("read_request", "write_request"):
      with self.subTest(message_type=message_type):
        zhujiang = Zhujiang()
        request = Message("cc", "home", message_type=message_type)
        injection = zhujiang.ring.inject(request)

        zhujiang.step()
        with self.assertRaisesRegex(ValueError, "home request needs an address"):
          zhujiang.step()

        self.assertEqual([request], zhujiang.home.received_messages)
        self.assertEqual([], zhujiang.home.sent_messages)
        self.assertEqual({}, zhujiang.home.dj.data_by_address)
        self.assertEqual([injection], zhujiang.ring.in_flight)

  def test_home_preserves_ids_for_multiple_read_requests(self):
    """Responses retain the IDs assigned to their requests."""
    zhujiang = Zhujiang()
    requests = [
      zhujiang.socket.read("0x1000"),
      zhujiang.socket.read("0x2000"),
    ]

    zhujiang.step()
    zhujiang.step()

    request_ids = [request.transaction_id for request in requests]
    response_ids = [response.transaction_id for response in zhujiang.home.sent_messages]
    self.assertEqual([0, 1], request_ids)
    self.assertEqual(request_ids, response_ids)

    zhujiang.step()
    zhujiang.step()
    zhujiang.step()

    self.assertEqual(response_ids, [
      response.transaction_id for response in zhujiang.socket.received_messages
    ])
    for request, response in zip(requests, zhujiang.home.sent_messages):
      self.assertIs(response, zhujiang.socket.read_response_for(request))

  def test_socket_only_indexes_read_responses(self):
    """Other messages remain available only through the general inbox."""
    zhujiang = Zhujiang()
    request = Message("cc", "home", message_type="read_request", transaction_id=3)
    message = Message("io", "cc", message_type="write_response", transaction_id=3)

    zhujiang.socket.receive(message)

    self.assertEqual([message], zhujiang.socket.received_messages)
    self.assertIsNone(zhujiang.socket.read_response_for(request))

  def test_socket_only_indexes_io_responses(self):
    """Other messages remain available only through the general inbox."""
    zhujiang = Zhujiang()
    request = Message("cc", "io", message_type="io_request", transaction_id=3)
    message = Message("home", "cc", message_type="read_response", transaction_id=3)

    zhujiang.socket.receive(message)

    self.assertEqual([message], zhujiang.socket.received_messages)
    self.assertIsNone(zhujiang.socket.io_response_for(request))

  def test_socket_only_indexes_write_responses(self):
    """Other messages remain available only through the general inbox."""
    zhujiang = Zhujiang()
    request = Message("cc", "home", message_type="write_request", transaction_id=3)
    message = Message("io", "cc", message_type="io_response", transaction_id=3)

    zhujiang.socket.receive(message)

    self.assertEqual([message], zhujiang.socket.received_messages)
    self.assertIsNone(zhujiang.socket.write_response_for(request))


if __name__ == "__main__":
  unittest.main()
