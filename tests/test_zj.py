import unittest

from tinyzj.chi import (
  Channel,
  DatOpcode,
  ReqOpcode,
  Resp,
  RespErr,
  RspOpcode,
  SnpOpcode,
)
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
    self.assertEqual(Channel.REQ, cc0_request.channel)
    self.assertEqual(ReqOpcode.READ_NO_SNP, cc0_request.opcode)
    self.assertEqual("n11", cc1_request.source_name)
    self.assertEqual("n01", cc1_request.target_name)
    self.assertEqual(Channel.REQ, cc1_request.channel)
    self.assertEqual(ReqOpcode.WRITE_UNIQUE_FULL, cc1_request.opcode)
    self.assertIsNone(cc1_request.payload)
    self.assertEqual([0, 0], [
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
          [
            (Channel.REQ, ReqOpcode.READ_NO_SNP),
            (Channel.DAT, DatOpcode.COMP_DATA),
          ],
          [
            (message.channel, message.opcode)
            for message in zhujiang.hf.received_messages
          ],
        )
        self.assertEqual(
          [(Channel.REQ, ReqOpcode.READ_NO_SNP)],
          [
            (message.channel, message.opcode)
            for message in zhujiang.s.received_messages
          ],
        )
        self.assertEqual(Channel.DAT, response.channel)
        self.assertEqual(DatOpcode.COMP_DATA, response.opcode)
        self.assertEqual({}, zhujiang.hf.pending_requests)

  def test_read_no_snp_releases_address_after_success_or_error(self):
    for error in (False, True):
      with self.subTest(error=error):
        address = "0x1000"
        zhujiang = Zhujiang(error_addresses=[address] if error else [])
        request = zhujiang.cc0.read(address)

        zhujiang.run_until_idle()

        response = zhujiang.cc0.read_response_for(request)
        self.assertEqual(RespErr.DERR if error else RespErr.OK, response.resp_err)
        self.assertEqual({}, zhujiang.hf.pending_requests)
        self.assertEqual({}, zhujiang.hf.address_busy)
        self.assertEqual({}, zhujiang.hf.pending_comp_acks)

        second = zhujiang.cc1.read(address)
        zhujiang.run_until_idle()
        self.assertIsNotNone(zhujiang.cc1.read_response_for(second))
        self.assertEqual({}, zhujiang.hf.address_busy)

  def test_read_no_snp_unblocks_queued_same_address_requests(self):
    for error in (False, True):
      for operation in ("read", "read_shared", "read_unique", "write"):
        with self.subTest(error=error, operation=operation):
          address = "0x1000"
          zhujiang = Zhujiang(
            buffer_capacity=2,
            max_transactions=1,
            error_addresses=[address] if error else [],
          )
          zhujiang.s.dj.write(address, "old value")
          first = zhujiang.cc0.read(address)
          if operation == "write":
            second = zhujiang.cc1.write(address, "new value")
          else:
            second = getattr(zhujiang.cc1, operation)(address)

          zhujiang.run_until_idle(max_steps=100)

          first_response = zhujiang.cc0.read_response_for(first)
          self.assertEqual("old value", first_response.payload)
          self.assertEqual(
            RespErr.DERR if error else RespErr.OK, first_response.resp_err
          )
          if operation == "write":
            response = zhujiang.cc1.write_response_for(second)
            self.assertEqual(
              RespErr.NDERR if error else RespErr.OK, response.resp_err
            )
            self.assertEqual(
              "old value" if error else "new value",
              zhujiang.s.dj.data_by_address[address],
            )
          else:
            response = zhujiang.cc1.read_response_for(second)
            self.assertEqual("old value", response.payload)
            self.assertEqual(
              RespErr.DERR if error else RespErr.OK, response.resp_err
            )
          self.assertEqual({}, zhujiang.hf.address_busy)
          self.assertEqual({}, zhujiang.hf.pending_requests)
          self.assertEqual({}, zhujiang.hf.pending_comp_acks)
          self.assertEqual({}, zhujiang.hf.pending_snoops)
          self.assertEqual({}, zhujiang.hf.pending_write_data)
          self.assertEqual({}, zhujiang.s.pending_writes)
          self.assertEqual({}, zhujiang.cc1.pending_write_data)
          self.assertEqual([], zhujiang.ring.in_flight)

  def test_cc0_write_is_visible_to_cc1_read(self):
    zhujiang = Zhujiang()
    write_request = zhujiang.cc0.write("0x1000", "value 1")

    steps = zhujiang.run_until_idle()
    read_request = zhujiang.cc1.read("0x1000")
    zhujiang.run_until_idle()

    write_response = zhujiang.cc0.write_response_for(write_request)
    self.assertEqual(20, steps)
    self.assertEqual(Channel.RSP, write_response.channel)
    self.assertEqual(RspOpcode.COMP, write_response.opcode)
    read_response = zhujiang.cc1.read_response_for(read_request)
    self.assertEqual("value 1", read_response.payload)
    self.assertTrue(read_response.data_present)
    self.assertEqual("value 1", zhujiang.s.dj.data_by_address["0x1000"])

  def test_write_separates_address_request_from_data(self):
    zhujiang = Zhujiang()
    request = zhujiang.cc0.write("0x1000", "value 1")

    zhujiang.run_until_idle()

    storage_request = zhujiang.s.received_messages[0]
    storage_data = zhujiang.s.received_messages[1]
    dbid_response = zhujiang.cc0.received_messages[0]
    response = zhujiang.cc0.write_response_for(request)
    self.assertIsNone(request.payload)
    self.assertEqual("0x1000", request.address)
    self.assertEqual(
      [
        (Channel.REQ, ReqOpcode.WRITE_UNIQUE_FULL),
        (Channel.DAT, DatOpcode.NON_COPY_BACK_WRITE_DATA),
        (Channel.RSP, RspOpcode.DBID_RESP),
        (Channel.RSP, RspOpcode.COMP),
      ],
      [
        (message.channel, message.opcode)
        for message in zhujiang.hf.received_messages
      ],
    )
    self.assertEqual(
      [
        (Channel.REQ, ReqOpcode.WRITE_NO_SNP_FULL),
        (Channel.DAT, DatOpcode.NON_COPY_BACK_WRITE_DATA),
      ],
      [
        (message.channel, message.opcode)
        for message in zhujiang.s.received_messages
      ],
    )
    self.assertEqual(
      [
        (Channel.RSP, RspOpcode.DBID_RESP),
        (Channel.RSP, RspOpcode.COMP),
      ],
      [
        (message.channel, message.opcode)
        for message in zhujiang.cc0.received_messages
      ],
    )
    self.assertEqual("n01", storage_request.source_name)
    self.assertEqual("n10", storage_request.target_name)
    self.assertEqual("0x1000", storage_request.address)
    self.assertIsNone(storage_request.payload)
    self.assertIsNone(storage_data.address)
    self.assertEqual("value 1", storage_data.payload)
    self.assertEqual(request.transaction_id, dbid_response.transaction_id)
    storage_completion = next(
      message for message in zhujiang.hf.received_messages
      if message.source_name == "n10" and message.opcode == RspOpcode.COMP
    )
    self.assertEqual(storage_request.transaction_id, storage_completion.transaction_id)
    self.assertEqual("n01", response.source_name)
    self.assertEqual("n00", response.target_name)
    self.assertEqual(request.transaction_id, response.transaction_id)
    self.assertFalse(hasattr(zhujiang.hf, "dj"))
    self.assertEqual({}, zhujiang.cc0.pending_write_data)
    self.assertEqual({}, zhujiang.hf.pending_requests)
    self.assertEqual({}, zhujiang.hf.pending_write_data)
    self.assertEqual({}, zhujiang.s.pending_writes)
    self.assertEqual("value 1", zhujiang.s.dj.data_by_address["0x1000"])

  def test_concurrent_writes_keep_data_with_their_transactions(self):
    zhujiang = Zhujiang()
    cc0_request = zhujiang.cc0.write("0x1000", "value 1")
    cc1_request = zhujiang.cc1.write("0x2000", "value 2")

    zhujiang.run_until_idle()

    self.assertEqual(
      cc0_request.transaction_id,
      cc1_request.transaction_id,
    )
    self.assertEqual(
      {
        "0x1000": "value 1",
        "0x2000": "value 2",
      },
      zhujiang.s.dj.data_by_address,
    )
    self.assertEqual(
      RspOpcode.COMP,
      zhujiang.cc0.write_response_for(cc0_request).opcode,
    )
    self.assertEqual(
      RspOpcode.COMP,
      zhujiang.cc1.write_response_for(cc1_request).opcode,
    )

  def test_finite_buffers_complete_eight_alternating_transactions(self):
    zhujiang = Zhujiang(buffer_capacity=2)
    expected_reads = {}
    expected_writes = {}

    for cc_name in ("cc0", "cc1"):
      cc = getattr(zhujiang, cc_name)
      for index in range(4):
        read_address = f"{cc_name}-read-{index}"
        read_data = f"{cc_name} initial {index}"
        zhujiang.s.dj.write(read_address, read_data)
        read_request = cc.read(read_address)
        expected_reads[read_request] = read_data

        write_address = f"{cc_name}-write-{index}"
        write_data = f"{cc_name} written {index}"
        write_request = cc.write(write_address, write_data)
        expected_writes[write_request] = write_data

    steps = zhujiang.run_until_idle(max_steps=500)
    self.assertEqual(54, steps)

    for cc_name in ("cc0", "cc1"):
      cc = getattr(zhujiang, cc_name)
      self.assertEqual(
        8,
        len([
          message
          for message in cc.received_messages
          if message.opcode in (DatOpcode.COMP_DATA, RspOpcode.COMP)
        ]),
      )
    for request, expected in expected_reads.items():
      cc = zhujiang.cc0 if request.source_name == "n00" else zhujiang.cc1
      self.assertEqual(expected, cc.read_response_for(request).payload)
    for request, expected in expected_writes.items():
      cc = zhujiang.cc0 if request.source_name == "n00" else zhujiang.cc1
      self.assertEqual(expected, zhujiang.s.dj.data_by_address[request.address])
      self.assertEqual(RspOpcode.COMP, cc.write_response_for(request).opcode)

    self.assertEqual({}, zhujiang.hf.pending_requests)
    self.assertEqual({}, zhujiang.hf.pending_write_data)
    self.assertEqual({}, zhujiang.s.pending_writes)
    self.assertEqual({}, zhujiang.cc0.pending_write_data)
    self.assertEqual({}, zhujiang.cc1.pending_write_data)
    self.assertEqual([], zhujiang.ring.in_flight)
    self.assertGreater(steps, 0)

  def test_single_slot_completes_write_while_read_waits_at_home(self):
    zhujiang = Zhujiang(max_transactions=1)
    write_request = zhujiang.cc0.write("0x1000", "value 1")
    read_request = zhujiang.cc1.read("0x2000")

    zhujiang.run_until_idle(max_steps=500)

    self.assertEqual(
      [
        (Channel.REQ, ReqOpcode.WRITE_UNIQUE_FULL),
        (Channel.DAT, DatOpcode.NON_COPY_BACK_WRITE_DATA),
        (Channel.RSP, RspOpcode.DBID_RESP),
        (Channel.RSP, RspOpcode.COMP),
        (Channel.REQ, ReqOpcode.READ_NO_SNP),
        (Channel.DAT, DatOpcode.COMP_DATA),
      ],
      [
        (message.channel, message.opcode)
        for message in zhujiang.hf.received_messages
      ],
    )
    self.assertEqual(
      RspOpcode.COMP,
      zhujiang.cc0.write_response_for(write_request).opcode,
    )
    self.assertEqual("value 1", zhujiang.s.dj.data_by_address["0x1000"])
    read_response = zhujiang.cc1.read_response_for(read_request)
    self.assertEqual("read data", read_response.payload)
    self.assertFalse(read_response.data_present)
    self.assertEqual({}, zhujiang.hf.pending_requests)
    self.assertEqual({}, zhujiang.hf.pending_write_data)
    self.assertEqual({}, zhujiang.s.pending_writes)
    self.assertEqual({}, zhujiang.cc0.pending_write_data)
    self.assertEqual({}, zhujiang.cc1.pending_write_data)
    self.assertEqual([], zhujiang.ring.in_flight)

  def test_finite_buffers_and_one_slot_complete_eight_alternating_transactions(self):
    zhujiang = Zhujiang(buffer_capacity=2, max_transactions=1)
    expected_reads = {}
    expected_writes = {}

    for cc_name in ("cc0", "cc1"):
      cc = getattr(zhujiang, cc_name)
      for index in range(4):
        read_address = f"{cc_name}-read-{index}"
        read_data = f"{cc_name} initial {index}"
        zhujiang.s.dj.write(read_address, read_data)
        read_request = cc.read(read_address)
        expected_reads[read_request] = read_data

        write_address = f"{cc_name}-write-{index}"
        write_data = f"{cc_name} written {index}"
        write_request = cc.write(write_address, write_data)
        expected_writes[write_request] = write_data

    steps = zhujiang.run_until_idle(max_steps=500)
    self.assertGreater(steps, 0)

    for cc_name in ("cc0", "cc1"):
      cc = getattr(zhujiang, cc_name)
      self.assertEqual(
        8,
        len([
          message
          for message in cc.received_messages
          if message.opcode in (DatOpcode.COMP_DATA, RspOpcode.COMP)
        ]),
      )
    for request, expected in expected_reads.items():
      cc = zhujiang.cc0 if request.source_name == "n00" else zhujiang.cc1
      self.assertEqual(expected, cc.read_response_for(request).payload)
    for request, expected in expected_writes.items():
      cc = zhujiang.cc0 if request.source_name == "n00" else zhujiang.cc1
      self.assertEqual(expected, zhujiang.s.dj.data_by_address[request.address])
      self.assertEqual(RspOpcode.COMP, cc.write_response_for(request).opcode)

    self.assertEqual({}, zhujiang.hf.pending_requests)
    self.assertEqual({}, zhujiang.hf.pending_write_data)
    self.assertEqual({}, zhujiang.s.pending_writes)
    self.assertEqual({}, zhujiang.cc0.pending_write_data)
    self.assertEqual({}, zhujiang.cc1.pending_write_data)
    self.assertEqual([], zhujiang.ring.in_flight)

  def test_normal_read_and_write_carry_ok_resp_err_and_resp_states(self):
    zhujiang = Zhujiang()
    read_request = zhujiang.cc0.read("0x1000")
    write_request = zhujiang.cc0.write("0x2000", "value 2")

    zhujiang.run_until_idle()

    read_response = zhujiang.cc0.read_response_for(read_request)
    self.assertEqual(RespErr.OK, read_response.resp_err)
    self.assertEqual(Resp.I, read_response.resp)
    storage_comp_data = [
      message
      for message in zhujiang.hf.received_messages
      if message.channel == Channel.DAT and message.opcode == DatOpcode.COMP_DATA
    ][0]
    self.assertEqual(Resp.UC, storage_comp_data.resp)
    write_response = zhujiang.cc0.write_response_for(write_request)
    self.assertEqual(RespErr.OK, write_response.resp_err)

  def test_error_address_read_returns_derr(self):
    zhujiang = Zhujiang(error_addresses={"0x1000"})
    request = zhujiang.cc0.read("0x1000")

    zhujiang.run_until_idle()

    response = zhujiang.cc0.read_response_for(request)
    self.assertEqual(RespErr.DERR, response.resp_err)
    self.assertEqual({}, zhujiang.hf.pending_requests)
    self.assertEqual({}, zhujiang.s.pending_writes)

  def test_error_address_write_returns_nderr_and_skips_storage(self):
    zhujiang = Zhujiang(error_addresses={"0x1000"})
    request = zhujiang.cc0.write("0x1000", "value 1")

    zhujiang.run_until_idle()

    response = zhujiang.cc0.write_response_for(request)
    self.assertEqual(RespErr.NDERR, response.resp_err)
    self.assertNotIn("0x1000", zhujiang.s.dj.data_by_address)
    self.assertEqual({}, zhujiang.cc0.pending_write_data)
    self.assertEqual({}, zhujiang.hf.pending_requests)
    self.assertEqual({}, zhujiang.hf.pending_write_data)
    self.assertEqual({}, zhujiang.s.pending_writes)

  def test_read_shared_caches_sc_and_registers_directory(self):
    zhujiang = Zhujiang()
    zhujiang.s.dj.write("0x1000", "value 1")
    request = zhujiang.cc0.read_shared("0x1000")

    zhujiang.run_until_idle()

    response = zhujiang.cc0.read_response_for(request)
    self.assertEqual(Resp.SC, response.resp)
    self.assertEqual((Resp.SC, "value 1"), zhujiang.cc0.cache["0x1000"])
    self.assertEqual({"n00": Resp.SC}, zhujiang.hf.directory["0x1000"])
    comp_acks = [
      message
      for message in zhujiang.hf.received_messages
      if message.opcode == RspOpcode.COMP_ACK
    ]
    self.assertEqual(1, len(comp_acks))
    self.assertEqual(request.transaction_id, comp_acks[0].transaction_id)
    self.assertEqual({}, zhujiang.hf.pending_requests)
    self.assertEqual({}, zhujiang.hf.pending_comp_acks)

  def test_read_shared_hit_does_not_reach_the_network(self):
    zhujiang = Zhujiang()
    zhujiang.s.dj.write("0x1000", "value 1")
    zhujiang.cc0.read_shared("0x1000")
    zhujiang.run_until_idle()
    hf_count = len(zhujiang.hf.received_messages)

    hit = zhujiang.cc0.read_shared("0x1000")
    steps = zhujiang.run_until_idle()

    self.assertIsNone(hit)
    self.assertEqual(0, steps)
    self.assertEqual(hf_count, len(zhujiang.hf.received_messages))
    self.assertEqual((Resp.SC, "value 1"), zhujiang.cc0.cache["0x1000"])

  def test_read_shared_error_address_completes_without_caching(self):
    zhujiang = Zhujiang(error_addresses={"0x1000"})
    request = zhujiang.cc0.read_shared("0x1000")

    zhujiang.run_until_idle()

    response = zhujiang.cc0.read_response_for(request)
    self.assertEqual(RespErr.DERR, response.resp_err)
    self.assertEqual({}, zhujiang.cc0.cache)
    self.assertEqual({}, zhujiang.hf.directory)
    self.assertEqual({}, zhujiang.hf.pending_requests)
    self.assertEqual({}, zhujiang.hf.pending_comp_acks)
    self.assertEqual([], zhujiang.ring.in_flight)

  def test_second_read_shared_snoops_the_first_holder(self):
    zhujiang = Zhujiang(buffer_capacity=2)
    zhujiang.s.dj.write("0x1000", "value 1")
    first = zhujiang.cc0.read_shared("0x1000")
    zhujiang.run_until_idle()
    self.assertEqual({"n00": Resp.SC}, zhujiang.hf.directory["0x1000"])
    hf_count = len(zhujiang.hf.received_messages)

    second = zhujiang.cc1.read_shared("0x1000")
    zhujiang.run_until_idle()

    response = zhujiang.cc1.read_response_for(second)
    self.assertEqual("value 1", response.payload)
    self.assertEqual(Resp.SC, response.resp)
    self.assertEqual((Resp.SC, "value 1"), zhujiang.cc0.cache["0x1000"])
    self.assertEqual((Resp.SC, "value 1"), zhujiang.cc1.cache["0x1000"])
    self.assertEqual(
      {"n00": Resp.SC, "n11": Resp.SC},
      zhujiang.hf.directory["0x1000"],
    )
    snp_shared = [
      message
      for message in zhujiang.cc0.received_messages
      if message.channel == Channel.SNP
    ]
    self.assertEqual(1, len(snp_shared))
    self.assertEqual(SnpOpcode.SNP_SHARED, snp_shared[0].opcode)
    snp_resps = [
      message
      for message in zhujiang.hf.received_messages[hf_count:]
      if message.opcode == RspOpcode.SNP_RESP
    ]
    self.assertEqual(1, len(snp_resps))
    self.assertEqual({}, zhujiang.hf.pending_snoops)
    self.assertEqual({}, zhujiang.hf.pending_requests)
    self.assertEqual({}, zhujiang.hf.pending_comp_acks)
    self.assertEqual([], zhujiang.ring.in_flight)

  def test_read_unique_caches_uc(self):
    zhujiang = Zhujiang()
    zhujiang.s.dj.write("0x1000", "value 1")
    request = zhujiang.cc0.read_unique("0x1000")

    zhujiang.run_until_idle()

    response = zhujiang.cc0.read_response_for(request)
    self.assertEqual(Resp.UC, response.resp)
    self.assertEqual((Resp.UC, "value 1"), zhujiang.cc0.cache["0x1000"])
    self.assertEqual({"n00": Resp.UC}, zhujiang.hf.directory["0x1000"])
    self.assertEqual({}, zhujiang.hf.pending_requests)
    self.assertEqual({}, zhujiang.hf.address_busy)

  def test_write_invalidates_sc_holder(self):
    zhujiang = Zhujiang()
    zhujiang.s.dj.write("0x1000", "value 1")
    zhujiang.cc0.read_shared("0x1000")
    zhujiang.run_until_idle()
    self.assertEqual((Resp.SC, "value 1"), zhujiang.cc0.cache["0x1000"])

    write_request = zhujiang.cc1.write("0x1000", "value 2")
    zhujiang.run_until_idle()

    self.assertEqual((Resp.I, "value 1"), zhujiang.cc0.cache["0x1000"])
    self.assertEqual({}, zhujiang.hf.directory)
    self.assertEqual("value 2", zhujiang.s.dj.data_by_address["0x1000"])
    self.assertEqual(
      RspOpcode.COMP,
      zhujiang.cc1.write_response_for(write_request).opcode,
    )
    snp_uniques = [
      message
      for message in zhujiang.cc0.received_messages
      if message.channel == Channel.SNP
    ]
    self.assertEqual(1, len(snp_uniques))
    self.assertEqual(SnpOpcode.SNP_UNIQUE, snp_uniques[0].opcode)

  def test_read_shared_downgrades_uc_holder(self):
    zhujiang = Zhujiang()
    zhujiang.s.dj.write("0x1000", "value 1")
    zhujiang.cc0.read_unique("0x1000")
    zhujiang.run_until_idle()
    second = zhujiang.cc1.read_shared("0x1000")

    zhujiang.run_until_idle()

    self.assertEqual((Resp.SC, "value 1"), zhujiang.cc0.cache["0x1000"])
    self.assertEqual((Resp.SC, "value 1"), zhujiang.cc1.cache["0x1000"])
    self.assertEqual(
      {"n00": Resp.SC, "n11": Resp.SC},
      zhujiang.hf.directory["0x1000"],
    )
    response = zhujiang.cc1.read_response_for(second)
    self.assertEqual(Resp.SC, response.resp)

  def test_write_invalidates_read_holder_after_it_completes(self):
    zhujiang = Zhujiang()
    zhujiang.s.dj.write("0x1000", "value 1")
    zhujiang.cc0.read_shared("0x1000")
    zhujiang.run_until_idle()

    zhujiang.cc1.read_shared("0x1000")
    for _ in range(100):
      if any(
        message.opcode == ReqOpcode.READ_SHARED
        for message in zhujiang.hf.received_messages
      ):
        break
      zhujiang.step()
    self.assertEqual(
      (Resp.SC, "value 1"),
      zhujiang.cc0.cache["0x1000"],
    )

    zhujiang.cc0.write("0x1000", "value 2")
    zhujiang.run_until_idle()

    self.assertEqual((Resp.I, "value 1"), zhujiang.cc1.cache["0x1000"])
    self.assertEqual({}, zhujiang.hf.directory)
    self.assertEqual("value 2", zhujiang.s.dj.data_by_address["0x1000"])

    reread = zhujiang.cc1.read_shared("0x1000")
    zhujiang.run_until_idle()
    self.assertEqual(
      (Resp.SC, "value 2"),
      zhujiang.cc1.cache["0x1000"],
    )
    self.assertEqual("value 2", zhujiang.cc1.read_response_for(reread).payload)

  def test_coherent_race_on_one_address_under_finite_buffers(self):
    zhujiang = Zhujiang(buffer_capacity=2)
    zhujiang.s.dj.write("0x1000", "value 0")
    reads = [
      zhujiang.cc0.read_shared("0x1000"),
      zhujiang.cc1.read_shared("0x1000"),
    ]
    write_request = zhujiang.cc0.write("0x1000", "value 1")
    reads.append(zhujiang.cc1.read_shared("0x1000"))
    reads.append(zhujiang.cc0.read_unique("0x1000"))

    steps = zhujiang.run_until_idle(max_steps=500)
    self.assertGreater(steps, 0)

    self.assertEqual("value 1", zhujiang.s.dj.data_by_address["0x1000"])
    for request in reads:
      cc = zhujiang.cc0 if request.source_name == "n00" else zhujiang.cc1
      self.assertIsNotNone(cc.read_response_for(request))
    self.assertEqual(
      RspOpcode.COMP,
      zhujiang.cc0.write_response_for(write_request).opcode,
    )
    for cc in (zhujiang.cc0, zhujiang.cc1):
      for state, payload in cc.cache.values():
        self.assertEqual("value 1", payload)
    cached = set(
      cc.node_name
      for cc in (zhujiang.cc0, zhujiang.cc1)
      if "0x1000" in cc.cache and cc.cache["0x1000"][0] != Resp.I
    )
    self.assertEqual(cached, set(zhujiang.hf.directory.get("0x1000", {})))
    self.assertEqual({}, zhujiang.hf.pending_requests)
    self.assertEqual({}, zhujiang.hf.pending_comp_acks)
    self.assertEqual({}, zhujiang.hf.pending_snoops)
    self.assertEqual({}, zhujiang.hf.address_busy)
    self.assertEqual({}, zhujiang.hf.pending_write_data)
    self.assertEqual({}, zhujiang.s.pending_writes)
    self.assertEqual({}, zhujiang.cc0.pending_write_data)
    self.assertEqual({}, zhujiang.cc1.pending_write_data)
    self.assertEqual([], zhujiang.ring.in_flight)

    for cc in (zhujiang.cc0, zhujiang.cc1):
      request = cc.read_shared("0x1000")
      if request is not None:
        zhujiang.run_until_idle()
        self.assertEqual("value 1", cc.read_response_for(request).payload)
        self.assertEqual("value 1", cc.cache["0x1000"][1])

  def test_two_ccs_receive_only_their_own_responses(self):
    zhujiang = Zhujiang()
    cc0_request = zhujiang.cc0.read("0x1000")
    cc1_request = zhujiang.cc1.read("0x2000")

    zhujiang.run_until_idle()

    cc0_response = zhujiang.cc0.read_response_for(cc0_request)
    cc1_response = zhujiang.cc1.read_response_for(cc1_request)
    self.assertEqual([cc0_response], zhujiang.cc0.received_messages)
    self.assertEqual([cc1_response], zhujiang.cc1.received_messages)
    self.assertEqual(0, cc0_request.transaction_id)
    self.assertEqual(0, cc1_request.transaction_id)

  def test_home_dbid_is_distinct_from_each_ccs_txnid(self):
    zhujiang = Zhujiang()
    zhujiang.hf.next_dbid = 50
    zhujiang.cc0.read("0x1000")
    request = zhujiang.cc1.write("0x2000", "value 2")

    zhujiang.run_until_idle()

    dbid_response = [
      message
      for message in zhujiang.cc1.received_messages
      if message.opcode == RspOpcode.DBID_RESP
    ][0]
    self.assertEqual(0, request.transaction_id)
    self.assertEqual(0, dbid_response.transaction_id)
    self.assertEqual(50, dbid_response.dbid)

  def test_transaction_tables_are_empty_after_concurrent_transactions(self):
    zhujiang = Zhujiang()
    zhujiang.cc0.write("0x1000", "value 1")
    zhujiang.cc1.write("0x2000", "value 2")

    zhujiang.run_until_idle()

    self.assertEqual({}, zhujiang.cc0.pending_write_data)
    self.assertEqual({}, zhujiang.cc1.pending_write_data)
    self.assertEqual({}, zhujiang.hf.pending_requests)
    self.assertEqual({}, zhujiang.hf.pending_write_data)
    self.assertEqual({}, zhujiang.s.pending_writes)

  def test_s_ignores_non_storage_requests(self):
    zhujiang = Zhujiang()
    request = Message(
      "n00",
      "n10",
      channel=Channel.REQ,
      opcode=ReqOpcode.READ_SHARED,
    )
    zhujiang.ring.inject(request)

    zhujiang.run_until_idle()

    self.assertEqual([request], zhujiang.s.received_messages)
    self.assertEqual([], zhujiang.hf.received_messages)

  def test_hf_ignores_unsupported_requests(self):
    zhujiang = Zhujiang()
    request = Message("n00", "n01", channel=Channel.REQ, opcode="other")
    zhujiang.ring.inject(request)

    zhujiang.run_until_idle()

    self.assertEqual([request], zhujiang.hf.received_messages)
    self.assertEqual([], zhujiang.s.received_messages)
    self.assertEqual([], zhujiang.cc0.received_messages)

  def test_ccs_reject_requests_without_an_address(self):
    for cc_name in ("cc0", "cc1"):
      with self.subTest(cc=cc_name):
        zhujiang = Zhujiang()
        cc = getattr(zhujiang, cc_name)

        with self.assertRaisesRegex(ValueError, "read request needs an address"):
          cc.read(None)
        with self.assertRaisesRegex(ValueError, "write request needs an address"):
          cc.write(None, "value 1")

        self.assertEqual([], zhujiang.ring.in_flight)

  def test_hf_rejects_a_direct_request_without_an_address(self):
    requests = (
      Message(
        "n00",
        "n01",
        channel=Channel.REQ,
        opcode=ReqOpcode.READ_NO_SNP,
      ),
      Message(
        "n00",
        "n01",
        channel=Channel.REQ,
        opcode=ReqOpcode.WRITE_NO_SNP_FULL,
      ),
    )
    for request in requests:
      with self.subTest(
        message_type=request.message_type,
        channel=request.channel,
        opcode=request.opcode,
      ):
        zhujiang = Zhujiang()
        zhujiang.ring.inject(request)

        zhujiang.step()
        with self.assertRaisesRegex(ValueError, "home request needs an address"):
          zhujiang.step()

        self.assertEqual([request], zhujiang.hf.received_messages)
        self.assertEqual({}, zhujiang.hf.pending_requests)
        self.assertEqual([], zhujiang.s.received_messages)
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

  def test_rejects_invalid_max_transactions(self):
    for value in (0, -1, 1.5, "1"):
      with self.subTest(max_transactions=value):
        with self.assertRaisesRegex(
          ValueError,
          "max_transactions must be a positive integer",
        ):
          Zhujiang(max_transactions=value)


if __name__ == "__main__":
  unittest.main()
