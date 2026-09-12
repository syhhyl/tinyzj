import unittest

from tinyzj.xj import Message
from tinyzj.zj import Zhujiang


class ZhujiangTest(unittest.TestCase):
  def test_wiring_delivers_to_home(self):
    """The top-level Home wrapper acts as a message receiver."""
    zhujiang = Zhujiang()
    message = Message("cc", "home", "read request")
    zhujiang.ring.inject(message)

    zhujiang.ring.step()
    zhujiang.ring.step()

    self.assertEqual([message], zhujiang.home.received_messages)
    self.assertEqual([], zhujiang.ring.in_flight)


if __name__ == "__main__":
  unittest.main()
