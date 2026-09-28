import unittest
import re
import unicodedata
from contextlib import redirect_stdout
from io import StringIO
from unittest.mock import patch

from tinyzj.dashboard import Dashboard, main


class DashboardTest(unittest.TestCase):

  def test_footer_layout_and_command_response_completion(self):
    dashboard = Dashboard()
    request = dashboard.write("A", "hello")
    entry = {"words": ["write", "A", "hello"], "text": "write A hello",
             "state": "active", "request": request, "cc": "cc0"}
    dashboard.commands = [entry]
    view = dashboard.render()
    self.assertGreater(view.index("命令列表"), view.rindex("╰"))
    headings = next(line for line in view.splitlines() if "命令列表" in line)
    self.assertIn("Memory · S", headings)
    self.assertIn("▶ 执行中", view)
    for _ in range(100):
      if not dashboard.system.ring.in_flight:
        break
      dashboard.step()
    self.assertEqual("done", entry["state"])
    self.assertIn("✓ 已完成", dashboard.render())
    self.assertIn("'A' = 'hello'", dashboard.render())

  def test_viewport_fits_single_screen_without_advancing(self):
    dashboard = Dashboard()
    dashboard.read("A")
    text = dashboard.render(color=True)
    first = dashboard.viewport(text, 40, 12)
    plain = re.sub(r"\033\[[0-9;]*m", "", first)
    self.assertLessEqual(len(plain.splitlines()), 8)
    for line in plain.splitlines():
      self.assertLessEqual(sum(
        2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in line
      ), 39)
    self.assertNotIn("Page ", plain)
    self.assertNotIn("n/p", plain)
    self.assertEqual(first, dashboard.viewport(text, 40, 12))
    self.assertEqual(0, dashboard.steps)

  def test_script_eof_fails_with_outstanding_work(self):
    for source, inputs in (
      ("write A hello cc0\n", ["", EOFError()]),
      ("step 2\n", ["", EOFError()]),
    ):
      with self.subTest(source=source), \
        patch("sys.argv", ["dashboard", "--script", "commands.txt"]), \
        patch("builtins.open", return_value=StringIO(source)), \
        patch("builtins.input", side_effect=inputs), \
        redirect_stdout(StringIO()), patch("sys.stderr", new_callable=StringIO):
        with self.assertRaises(SystemExit) as error:
          main()
        self.assertEqual(1, error.exception.code)

  def test_script_batches_requests_and_waits_at_end_of_file(self):
    dashboard = Dashboard()
    source = "write A hello cc0\n# same time\n\nread A cc1\n"
    def enter(prompt):
      if "同时发起" in prompt:
        self.assertEqual([], dashboard.system.ring.in_flight)
        self.assertEqual(0, dashboard.steps)
      elif dashboard.steps == 0:
        self.assertEqual(2, len(dashboard.system.ring.in_flight))
      return ""
    with patch("sys.argv", ["dashboard", "--script", "commands.txt"]), \
      patch("builtins.open", return_value=StringIO(source)), \
      patch("builtins.input", side_effect=enter), \
      patch("tinyzj.dashboard.Dashboard", return_value=dashboard), \
      redirect_stdout(StringIO()):
      main()
    self.assertEqual("hello", dashboard.system.cc1.cache["A"][1])
    self.assertEqual([], dashboard.system.ring.in_flight)
    self.assertTrue(all(entry["state"] == "done" for entry in dashboard.commands))

  def test_script_step_separates_request_batches(self):
    dashboard = Dashboard()
    source = "write A hello cc0\nstep 3\nread A cc1\n"
    batches = []
    def enter(prompt):
      if "同时发起" in prompt:
        batches.append(dashboard.steps)
        return "" if len(batches) == 1 else "q"
      return ""
    with patch("sys.argv", ["dashboard", "--script", "commands.txt"]), \
      patch("builtins.open", return_value=StringIO(source)), \
      patch("builtins.input", side_effect=enter), \
      patch("tinyzj.dashboard.Dashboard", return_value=dashboard), \
      redirect_stdout(StringIO()):
      main()
    self.assertEqual([0, 3], batches)
    self.assertEqual(1, len(dashboard.requests))
    self.assertEqual("pending", dashboard.commands[-1]["state"])

  def test_render_does_not_advance_and_steps_preserve_read_timing(self):
    dashboard = Dashboard()
    request = dashboard.read("A")
    initial = dashboard.render()
    self.assertEqual(initial, dashboard.render())
    self.assertEqual(0, dashboard.steps)
    self.assertIn("REQ · ReadShared", initial)
    self.assertIn("TxnID=0", initial)
    self.assertGreater(initial.index("REQ · ReadShared"), initial.index("╭"))
    moved = dashboard.step()
    self.assertGreater(moved.index("REQ · ReadShared"), moved.index("╭"))
    self.assertIsNone(dashboard.system.cc0.read_response_for(request))
    dashboard.step(9)
    self.assertEqual(10, dashboard.steps)
    self.assertIsNotNone(dashboard.system.cc0.read_response_for(request))
    self.assertNotIn("M0", dashboard.render())

  def test_write_read_and_coherent_hit_through_api(self):
    dashboard = Dashboard()
    dashboard.write("A", "hello")
    dashboard.step(20)
    request = dashboard.read("A", cc="cc1")
    for _ in range(100):
      if not dashboard.system.ring.in_flight:
        break
      dashboard.step()
    self.assertEqual("hello", dashboard.system.cc1.read_response_for(request).payload)
    before = dashboard.steps
    self.assertIsNone(dashboard.read("A", cc="cc1"))
    self.assertEqual(before, dashboard.steps)
    self.assertIn("缓存命中", dashboard.events[0])
    self.assertEqual({}, dashboard.system.hf.address_busy)
    self.assertEqual([], dashboard.system.ring.in_flight)

    dashboard.read("A", cc="cc0")
    for _ in range(100):
      if not dashboard.system.ring.in_flight:
        break
      dashboard.step()
    self.assertEqual("SC", dashboard.system.cc0.cache["A"][0])
    self.assertEqual("SC", dashboard.system.cc1.cache["A"][0])
    dashboard.write("A", "new", cc="cc0")
    for _ in range(100):
      if not dashboard.system.ring.in_flight:
        break
      dashboard.step()
    self.assertEqual({}, dashboard.system.cc0.cache)
    self.assertEqual({}, dashboard.system.cc1.cache)
    self.assertEqual("new", dashboard.system.s.dj.data_by_address["A"])
    self.assertEqual({}, dashboard.system.hf.address_busy)

  def test_invalid_requester_does_not_inject(self):
    dashboard = Dashboard()
    with self.assertRaises(ValueError):
      dashboard.read("A", cc="cc2")
    self.assertEqual([], dashboard.system.ring.in_flight)

  def test_step_highlights_only_current_links_and_state_changes(self):
    dashboard = Dashboard()
    dashboard.read("A")
    first = dashboard.step()
    self.assertIn(" REQ ", first)
    self.assertIn("━", first)
    self.assertEqual([("n00", "n01")], dashboard.moves)
    self.assertEqual([], dashboard.changes)
    second = dashboard.step()
    self.assertNotIn("━", second)
    self.assertEqual([], dashboard.moves)
    self.assertIn("HF 活跃事务: [] → [0]", dashboard.changes)
    self.assertIn("交付 M0", dashboard.events[0])
    self.assertIn("新消息 M1", dashboard.events[1])
    self.assertNotIn("── 队列占用", second)
    self.assertIn("REQ · ReadNoSnp", second)
    self.assertIn("TxnID=0", second)
    self.assertEqual(2, dashboard.steps)

  def test_new_request_is_inside_source_node(self):
    dashboard = Dashboard()
    dashboard.write("A", "value")
    view = dashboard.render()
    self.assertLess(view.index("Node CC0"), view.index("REQ · WriteNoSnpFull"))
    self.assertLess(view.index("REQ · WriteNoSnpFull"), view.index("╰"))
    self.assertEqual(0, dashboard.steps)
    self.assertFalse(dashboard.system.ring.in_flight[0].in_ring)

  def test_node_expands_and_messages_use_single_colored_rows(self):
    dashboard = Dashboard()
    for index in range(12):
      dashboard.read(str(index))
    view = dashboard.render(color=True)
    self.assertNotIn("more", view)
    for index in range(12):
      self.assertIn(f"REQ · ReadShared  TxnID={index}", view)
    rows = [line for line in view.splitlines() if "REQ · ReadShared" in line]
    self.assertEqual(12, len(rows))
    self.assertTrue(all("\033[36m" in line for line in rows))

  def test_blocked_request_is_displayed_outside_node(self):
    dashboard = Dashboard(max_transactions=1)
    dashboard.write("A", "value", cc="cc0")
    dashboard.read("B", cc="cc1")
    dashboard.step(2)
    before = len(dashboard.system.ring.in_flight)
    view = dashboard.render()
    self.assertIn("TxnID=0  WAIT", view)
    self.assertNotIn("× WAIT", view)
    self.assertIn("REQ · ReadShared", view)
    self.assertLess(view.index("REQ · ReadShared"), view.index("╭"))
    self.assertNotIn("接收等待区", view)
    self.assertEqual(4, view.count("Node "))
    self.assertEqual(before, len(dashboard.system.ring.in_flight))
    for _ in range(100):
      if not dashboard.system.ring.in_flight:
        break
      dashboard.step()
    self.assertNotIn("WAIT", dashboard.render())
    self.assertEqual({}, dashboard.system.hf.address_busy)
