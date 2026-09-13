import unittest

from tinyzj.xj import Message
from tinyzj.zj import Zhujiang


class ZhujiangTest(unittest.TestCase):
  def test_home_returns_a_read_response_to_the_requester(self):
    """A read request makes a round trip between Socket and Home."""
    zhujiang = Zhujiang()
    request = zhujiang.socket.read("address 0x1000")

    self.assertEqual([request], zhujiang.socket.sent_messages)
    self.assertEqual("cc", request.source_name)
    self.assertEqual("home", request.target_name)
    self.assertEqual("read_request", request.message_type)
    self.assertEqual("address 0x1000", request.payload)
    self.assertEqual(0, request.transaction_id)
    self.assertIsNone(zhujiang.socket.read_response_for(request))

    zhujiang.ring.step()
    zhujiang.ring.step()

    self.assertEqual([request], zhujiang.home.received_messages)
    self.assertEqual(1, len(zhujiang.home.sent_messages))
    response = zhujiang.home.sent_messages[0]
    self.assertEqual("home", response.source_name)
    self.assertEqual("cc", response.target_name)
    self.assertEqual("read_response", response.message_type)
    self.assertEqual("read data", response.payload)
    self.assertEqual(request.transaction_id, response.transaction_id)
    self.assertEqual("home", zhujiang.ring.in_flight[0].current_node_name)

    zhujiang.ring.step()
    self.assertEqual("io", zhujiang.ring.in_flight[0].current_node_name)

    zhujiang.ring.step()
    self.assertEqual("cc", zhujiang.ring.in_flight[0].current_node_name)

    zhujiang.ring.step()
    self.assertEqual([response], zhujiang.socket.received_messages)
    self.assertIs(response, zhujiang.socket.read_response_for(request))
    self.assertEqual([], zhujiang.ring.in_flight)

  def test_home_does_not_respond_to_other_message_types(self):
    """Only the teaching read request has a response in this iteration."""
    zhujiang = Zhujiang()
    request = Message("cc", "home", message_type="write_request")
    zhujiang.ring.inject(request)

    zhujiang.ring.step()
    zhujiang.ring.step()

    self.assertEqual([request], zhujiang.home.received_messages)
    self.assertEqual([], zhujiang.home.sent_messages)
    self.assertEqual([], zhujiang.ring.in_flight)

  def test_home_preserves_ids_for_multiple_read_requests(self):
    """Responses retain the IDs assigned to their requests."""
    zhujiang = Zhujiang()
    requests = [
      zhujiang.socket.read("address 0x1000"),
      zhujiang.socket.read("address 0x2000"),
    ]

    zhujiang.ring.step()
    zhujiang.ring.step()

    request_ids = [request.transaction_id for request in requests]
    response_ids = [response.transaction_id for response in zhujiang.home.sent_messages]
    self.assertEqual([0, 1], request_ids)
    self.assertEqual(request_ids, response_ids)

    zhujiang.ring.step()
    zhujiang.ring.step()
    zhujiang.ring.step()

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


if __name__ == "__main__":
  unittest.main()
