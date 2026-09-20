import unittest

from tinyzj.xj import Message
from tinyzj.zj import Zhujiang


class ZhujiangTest(unittest.TestCase):

  def test_top_level_connects_four_role_nodes(self):
    zhujiang = Zhujiang()

    self.assertEqual(
      [
        ("n00", "CC0"),
        ("n01", "HF"),
        ("n11", "CC1"),
        ("n10", "S"),
      ],
      [(node.name, node.role) for node in zhujiang.ring.nodes],
    )
    self.assertIs(zhujiang.cc0, zhujiang.ring.connections["n00"])
    self.assertIs(zhujiang.hf, zhujiang.ring.connections["n01"])
    self.assertIs(zhujiang.cc1, zhujiang.ring.connections["n11"])
    self.assertIs(zhujiang.s, zhujiang.ring.connections["n10"])

  def test_both_ccs_send_requests_to_hf(self):
    zhujiang = Zhujiang()

    cc0_request = zhujiang.cc0.read("0x1000")
    cc1_request = zhujiang.cc1.write("0x2000", "value 2")

    self.assertEqual("n00", cc0_request.source_name)
    self.assertEqual("n01", cc0_request.target_name)
    self.assertEqual("read_request", cc0_request.message_type)
    self.assertEqual("n11", cc1_request.source_name)
    self.assertEqual("n01", cc1_request.target_name)
    self.assertEqual("write_request", cc1_request.message_type)
    self.assertEqual([0, 1], [
      cc0_request.transaction_id,
      cc1_request.transaction_id,
    ])

  def test_each_cc_completes_a_read_through_hf_and_s(self):
    for cc_name in ("cc0", "cc1"):
      with self.subTest(cc=cc_name):
        zhujiang = Zhujiang()
        cc = getattr(zhujiang, cc_name)
        request = cc.read("0x1000")

        steps = zhujiang.run_until_idle()

        response = cc.read_response_for(request)
        self.assertEqual(10, steps)
        self.assertEqual("read data", response.payload)
        self.assertFalse(response.data_present)
        self.assertEqual(request.transaction_id, response.transaction_id)
        self.assertEqual(
          ["read_request", "storage_read_response"],
          [message.message_type for message in zhujiang.hf.received_messages],
        )
        self.assertEqual(
          ["storage_read_request"],
          [message.message_type for message in zhujiang.s.received_messages],
        )
        self.assertEqual({}, zhujiang.hf.pending_requests)

  def test_cc0_write_is_visible_to_cc1_read(self):
    zhujiang = Zhujiang()
    write_request = zhujiang.cc0.write("0x1000", "value 1")

    zhujiang.run_until_idle()
    read_request = zhujiang.cc1.read("0x1000")
    zhujiang.run_until_idle()

    self.assertEqual(
      "write complete",
      zhujiang.cc0.write_response_for(write_request).payload,
    )
    read_response = zhujiang.cc1.read_response_for(read_request)
    self.assertEqual("value 1", read_response.payload)
    self.assertTrue(read_response.data_present)
    self.assertEqual("value 1", zhujiang.s.dj.data_by_address["0x1000"])

  def test_hf_forwards_requests_and_responses_without_storing_data(self):
    zhujiang = Zhujiang()
    request = zhujiang.cc0.write("0x1000", "value 1")

    zhujiang.run_until_idle()

    self.assertEqual(
      ["storage_write_request", "write_response"],
      [message.message_type for message in zhujiang.hf.sent_messages],
    )
    storage_request, response = zhujiang.hf.sent_messages
    self.assertEqual("n01", storage_request.source_name)
    self.assertEqual("n10", storage_request.target_name)
    self.assertEqual(request.transaction_id, storage_request.transaction_id)
    self.assertEqual("n01", response.source_name)
    self.assertEqual("n00", response.target_name)
    self.assertEqual(request.transaction_id, response.transaction_id)
    self.assertFalse(hasattr(zhujiang.hf, "dj"))
    self.assertEqual("value 1", zhujiang.s.dj.data_by_address["0x1000"])

  def test_two_ccs_receive_only_their_own_responses(self):
    zhujiang = Zhujiang()
    cc0_request = zhujiang.cc0.read("0x1000")
    cc1_request = zhujiang.cc1.read("0x2000")

    zhujiang.run_until_idle()

    cc0_response = zhujiang.cc0.read_response_for(cc0_request)
    cc1_response = zhujiang.cc1.read_response_for(cc1_request)
    self.assertEqual([cc0_response], zhujiang.cc0.received_messages)
    self.assertEqual([cc1_response], zhujiang.cc1.received_messages)
    self.assertIsNone(zhujiang.cc0.read_response_for(cc1_request))
    self.assertIsNone(zhujiang.cc1.read_response_for(cc0_request))

  def test_same_address_batch_is_processed_in_injection_order(self):
    zhujiang = Zhujiang()
    write_request = zhujiang.cc0.write("0x1000", "value 1")
    read_request = zhujiang.cc1.read("0x1000")

    zhujiang.run_until_idle()

    self.assertEqual(
      [write_request, read_request],
      zhujiang.hf.received_messages[:2],
    )
    self.assertEqual(
      ["storage_write_request", "storage_read_request"],
      [message.message_type for message in zhujiang.s.received_messages],
    )
    self.assertEqual(
      "value 1",
      zhujiang.cc1.read_response_for(read_request).payload,
    )

  def test_s_ignores_non_storage_requests(self):
    zhujiang = Zhujiang()
    request = Message("n00", "n10", message_type="read_request")
    zhujiang.ring.inject(request)

    zhujiang.run_until_idle()

    self.assertEqual([request], zhujiang.s.received_messages)
    self.assertEqual([], zhujiang.s.sent_messages)

  def test_hf_ignores_unsupported_requests(self):
    zhujiang = Zhujiang()
    request = Message("n00", "n01", message_type="other_request")
    zhujiang.ring.inject(request)

    zhujiang.run_until_idle()

    self.assertEqual([request], zhujiang.hf.received_messages)
    self.assertEqual([], zhujiang.hf.sent_messages)

  def test_ccs_reject_requests_without_an_address(self):
    for cc_name in ("cc0", "cc1"):
      with self.subTest(cc=cc_name):
        zhujiang = Zhujiang()
        cc = getattr(zhujiang, cc_name)

        with self.assertRaisesRegex(ValueError, "read request needs an address"):
          cc.read(None)
        with self.assertRaisesRegex(ValueError, "write request needs an address"):
          cc.write(None, "value 1")

        self.assertEqual([], cc.sent_messages)
        self.assertEqual([], zhujiang.ring.in_flight)

  def test_hf_rejects_a_direct_request_without_an_address(self):
    for message_type in ("read_request", "write_request"):
      with self.subTest(message_type=message_type):
        zhujiang = Zhujiang()
        request = Message("n00", "n01", message_type=message_type)
        zhujiang.ring.inject(request)

        zhujiang.step()
        with self.assertRaisesRegex(ValueError, "home request needs an address"):
          zhujiang.step()

        self.assertEqual([request], zhujiang.hf.received_messages)
        self.assertEqual([], zhujiang.hf.sent_messages)
        self.assertEqual({}, zhujiang.hf.pending_requests)
        self.assertEqual({}, zhujiang.s.dj.data_by_address)

  def test_run_until_idle_enforces_its_step_limit(self):
    zhujiang = Zhujiang()
    zhujiang.cc0.read("0x1000")

    with self.assertRaisesRegex(RuntimeError, "system did not become idle"):
      zhujiang.run_until_idle(max_steps=9)

  def test_run_until_idle_handles_an_idle_system(self):
    zhujiang = Zhujiang()

    self.assertEqual(0, zhujiang.run_until_idle())

  def test_run_until_idle_rejects_a_negative_limit(self):
    zhujiang = Zhujiang()

    with self.assertRaisesRegex(ValueError, "max_steps must be non-negative"):
      zhujiang.run_until_idle(max_steps=-1)


if __name__ == "__main__":
  unittest.main()
