import unittest

from tinyzj.xj import Message
from tinyzj.zj import Zhujiang


class ZhujiangTest(unittest.TestCase):
  def test_home_returns_a_read_response_to_the_requester(self):
    """A read request makes a round trip between Socket and Home."""
    zhujiang = Zhujiang()
    request = Message(
      "cc",
      "home",
      payload="address 0x1000",
      message_type="read_request",
    )
    zhujiang.ring.inject(request)

    zhujiang.ring.step()
    zhujiang.ring.step()

    self.assertEqual([request], zhujiang.home.received_messages)
    self.assertEqual(1, len(zhujiang.home.sent_messages))
    response = zhujiang.home.sent_messages[0]
    self.assertEqual("home", response.source_name)
    self.assertEqual("cc", response.target_name)
    self.assertEqual("read_response", response.message_type)
    self.assertEqual("read data", response.payload)
    self.assertEqual("home", zhujiang.ring.in_flight[0].current_node_name)

    zhujiang.ring.step()
    self.assertEqual("io", zhujiang.ring.in_flight[0].current_node_name)

    zhujiang.ring.step()
    self.assertEqual("cc", zhujiang.ring.in_flight[0].current_node_name)

    zhujiang.ring.step()
    self.assertEqual([response], zhujiang.socket.received_messages)
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


if __name__ == "__main__":
  unittest.main()
