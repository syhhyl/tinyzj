"""Interactive, step-by-step view of the tinyzj behavior model."""

import argparse
from copy import deepcopy
import os
import re
import shlex
import shutil
import sys
import time
import unicodedata

from .zj import Zhujiang


class Dashboard:

  def __init__(self, buffer_capacity=2, max_transactions=1, error_addresses=None):
    self.system = Zhujiang(
      buffer_capacity=buffer_capacity,
      max_transactions=max_transactions,
      error_addresses=error_addresses,
    )
    self.steps = 0
    self.requests = []
    self.events = ["就绪：可以发起请求，或按回车推进一步。"]
    self.message_ids = {}
    self.changes = []
    self.moves = []
    self.channel_moves = []
    self.commands = []
    self.current_command = None
    self.command_steps = 0
    self.page = 0

  def _state(self):
    z = self.system
    return deepcopy({
      "CC0 cache": z.cc0.cache,
      "CC1 cache": z.cc1.cache,
      "HF directory": z.hf.directory,
      "HF 地址占用": z.hf.address_busy,
      "HF 活跃事务": sorted(z.hf.pending_requests),
      "S memory": z.s.dj.data_by_address,
    })

  def _cc(self, name):
    if name not in ("cc0", "cc1"):
      raise ValueError("请求者必须是 cc0 或 cc1")
    return getattr(self.system, name)

  def _request(self, operation, address, cc, *args):
    endpoint = self._cc(cc)
    request = getattr(endpoint, operation)(address, *args)
    self.moves = []
    self.changes = []
    self.channel_moves = []
    if request is None:
      self.events = [f"{cc} 缓存命中：{address!r} = {endpoint.cache[address]!r}"]
    else:
      label = f"T{len(self.requests)}"
      self.requests.append((label, cc, operation, request))
      self.events = [
        f"{label} {cc}.{operation}({address!r}) 已进入源队列；尚未推进时间。"
      ]
      self._identify_messages()
    return request

  def read(self, address, cc="cc0"):
    return self._request("read_shared", address, cc)

  def write(self, address, data, cc="cc0"):
    return self._request("write", address, cc, data)

  def memory(self, address=None):
    data = self.system.s.dj.data_by_address
    if address is not None:
      if address not in data:
        return f"S memory: {address!r} 尚未写入"
      return f"{address!r} = {data[address]!r}"
    lines = [f"S memory · {len(data)} 个已写入地址"]
    lines.extend(f"  {key!r} = {value!r}" for key, value in data.items())
    if not data:
      lines.append("  （空）")
    return "\n".join(lines)

  def _with_commands(self, lines, color):
    def paint(text, style):
      return f"\033[{style}m{text}\033[0m" if color else text

    if self.commands:
      panel = [paint("命令列表", "1;37"), ""]
      for entry in self.commands:
        state = entry["state"]
        marker, style = {
          "pending": ("○ 未执行", "2;37"),
          "active": ("▶ 执行中", "1;33"),
          "done": ("✓ 已执行", "32"),
        }[state]
        progress = ""
        if entry is self.current_command and entry["words"][0] == "step":
          progress = f"  [{self.command_steps}/{entry['words'][1]}]"
        panel.append(paint(f"{marker}  {entry['text']}{progress}", style))
      panel.extend(["", "read/write 已执行 = 请求已发出"])

      def columns(text):
        plain = re.sub(r"\033\[[0-9;]*m", "", text)
        return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in plain)

      diagram_width = max(columns(line) for line in lines)
      combined = []
      for index in range(max(len(lines), len(panel))):
        diagram = lines[index] if index < len(lines) else ""
        sidebar = panel[index] if index < len(panel) else ""
        combined.append(diagram + " " * (diagram_width - columns(diagram) + 4) + sidebar)
      lines = combined
    return "\n".join(lines)

  def _identify_messages(self):
    for injection in self.system.ring.in_flight:
      message = injection.message
      if message not in self.message_ids:
        self.message_ids[message] = f"M{len(self.message_ids)}"

  def _describe(self, injection):
    message = injection.message
    return (
      f"{self.message_ids[message]} {message.channel}/{message.opcode} "
      f"{message.source_name}→{message.target_name} "
      f"txn={message.transaction_id} dbid={message.dbid}"
    )

  def step(self, count=1):
    if not isinstance(count, int) or count < 1:
      raise ValueError("步数必须是正整数")
    for _ in range(count):
      ring = self.system.ring
      self._identify_messages()
      before = {
        injection: (injection.current_node_name, injection.in_ring)
        for injection in ring.in_flight
      }
      state_before = self._state()
      self.system.step()
      self.steps += 1
      self._identify_messages()
      events = []
      self.moves = []
      self.channel_moves = []
      for injection, (node, in_ring) in before.items():
        description = self._describe(injection)
        if injection not in ring.in_flight:
          events.append(f"交付 {description} @ {node}")
        elif injection.current_node_name != node:
          kind = "转发" if in_ring else "入环"
          self.moves.append((node, injection.current_node_name))
          self.channel_moves.append((node, injection.current_node_name, injection.message.channel))
          events.append(
            f"{kind} {description}：{node}→{injection.current_node_name}"
          )
        else:
          events.append(f"等待 {description} @ {node}（本步未移动或交付）")
      for injection in ring.in_flight:
        if injection not in before:
          events.append(f"新消息 {self._describe(injection)}（源队列）")
      self.events = events or ["本步无网络活动。"]
      self.changes = [
        f"{name}: {state_before[name]!r} → {value!r}"
        for name, value in self._state().items()
        if state_before[name] != value
      ]
    return self.render()

  def render(self, color=False):
    ring = self.system.ring
    self._identify_messages()
    def paint(text, style):
      return f"\033[{style}m{text}\033[0m" if color else text

    colors = {"REQ": "36", "RSP": "32", "DAT": "33", "SNP": "35"}
    width = 32
    gap = 28
    visible_messages = 2
    height = 6

    def centered(text):
      columns = sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in text)
      padding = max(0, width - columns)
      return " " * (padding // 2) + text + " " * (padding - padding // 2)

    blocked_messages = set()
    for injection in ring.in_flight:
      message = injection.message
      if not injection.in_ring or injection.current_node_name != message.target_name:
        continue
      receiver = ring.connections[message.target_name]
      can_receive = getattr(receiver, "can_receive", None)
      if callable(can_receive) and not can_receive(message):
        blocked_messages.add(injection)

    def box(node, role, source=False):
      messages = [
        i for i in ring.in_flight
        if i.current_node_name == node and i.in_ring != source
      ]
      edge = paint("│", "2;37")
      if source:
        edge = " "
        rows = []
      else:
        rows = [paint("╭" + "─" * width + "╮", "2;37")]
        rows.append(edge + paint("Router".center(width), "1;37") + edge)
        rows.append(edge + " " * width + edge)
      content = []
      limit = 1 if source else visible_messages
      for injection in messages[:limit]:
        message = injection.message
        ids = f"TxnID={message.transaction_id}"
        if message.dbid is not None:
          ids += f"  DBID={message.dbid}"
        if injection in blocked_messages:
          ids += "  WAIT"
        for label in (
          f"{message.channel} · {message.opcode}",
          ids,
          "",
        ):
          style = "1;31" if injection in blocked_messages else colors[message.channel]
          content.append(edge + paint(centered(label), style) + edge)
      rows.extend(content)
      for _ in range((3 if source else height) - len(content)):
        rows.append(edge + " " * width + edge)
      extra = f"+{len(messages) - limit} more" if len(messages) > limit else ""
      rows.append(edge + paint(extra.center(width), "2;37") + edge)
      if not source:
        rows.append(paint("╰" + "─" * width + "╯", "2;37"))
      return rows

    def horizontal(left, right, row):
      if row not in (3, 5, 7, 9):
        return " " * gap
      channel = list(colors)[(row - 3) // 2]
      forward = (left, right, channel) in self.channel_moves
      backward = (right, left, channel) in self.channel_moves
      active = forward or backward
      stroke = "━" if active else "─"
      label = f" {channel} ".center(gap - 2, stroke)
      start = "◀" if backward or not active else stroke
      end = "▶" if forward or not active else stroke
      return paint(start + label + end, ("1;" if active else "2;") + colors[channel])

    def vertical(top, bottom, row):
      lanes = []
      for channel in colors:
        down = (top, bottom, channel) in self.channel_moves
        up = (bottom, top, channel) in self.channel_moves
        active = down or up
        if row == 3:
          symbol = channel
        elif active:
          symbol = ("↕" if up and down else "▼" if down else "▲") if row == 2 else "┃"
        else:
          symbol = "▲" if row == 0 else "▼" if row == 6 else "│"
        lanes.append(paint(symbol.center(6), ("1;" if active else "2;") + colors[channel]))
      return " " * 5 + "".join(lanes) + " " * 5

    def endpoint(node, role):
      held = any(i.current_node_name == node for i in blocked_messages)
      return [
        paint(role.center(width + 2), "1;37"),
        paint(("× WAIT" if held else "↕").center(width + 2), "1;31" if held else "2;37"),
      ]

    lines = []
    for left, right, roles in (("n00", "n01", ("CC0", "HF")), ("n10", "n11", ("S", "CC1"))):
      sources = [
        "  " + a + " " * gap + b
        for a, b in zip(box(left, roles[0], True), box(right, roles[1], True))
      ]
      if left == "n00":
        lines.extend(sources)
        for a, b in zip(endpoint(left, roles[0]), endpoint(right, roles[1])):
          lines.append("  " + a + " " * gap + b)
      left_box, right_box = box(left, roles[0]), box(right, roles[1])
      for row, (a, b) in enumerate(zip(left_box, right_box)):
        link = horizontal(left, right, row)
        lines.append("  " + a + link + b)
      if left == "n10":
        for a, b in zip(reversed(endpoint(left, roles[0])), reversed(endpoint(right, roles[1]))):
          lines.append("  " + a + " " * gap + b)
        lines.extend(sources)
      if left == "n00":
        for row in range(7):
          lines.append(
            "  "
            + vertical("n00", "n10", row)
            + " " * gap
            + vertical("n01", "n11", row)
          )
    return self._with_commands(lines, color)

  def viewport(self, text, columns, rows):
    columns = max(1, columns - 1)
    page_height = max(1, rows - 5)
    lines = text.splitlines()
    ansi = re.compile(r"\033\[[0-9;]*m|.")

    def cells(line):
      return sum(
        0 if token.startswith("\033") else
        2 if unicodedata.east_asian_width(token) in ("W", "F") else 1
        for token in ansi.findall(line)
      )

    horizontal = max(1, (max(map(cells, lines), default=0) + columns - 1) // columns)
    vertical = max(1, (len(lines) + page_height - 1) // page_height)
    pages = horizontal * vertical
    self.page %= pages
    y, x = divmod(self.page, horizontal)
    result = []
    for line in lines[y * page_height:(y + 1) * page_height]:
      position = 0
      output = []
      for token in ansi.findall(line):
        if token.startswith("\033"):
          output.append(token)
          continue
        size = 2 if unicodedata.east_asian_width(token) in ("W", "F") else 1
        start, end = x * columns, (x + 1) * columns
        if position >= start and position + size <= end:
          output.append(token)
        elif position < end and position + size > start:
          output.append(" " * (min(position + size, end) - max(position, start)))
        position += size
      result.append("".join(output) + ("\033[0m" if "\033[" in text else ""))
    result.append(f"Page {self.page + 1}/{pages}  n/p: view  Enter: step"[:columns])
    return "\n".join(result)

  def show(self, refresh=False):
    terminal = sys.stdout.isatty()
    text = self.render(color=terminal and "NO_COLOR" not in os.environ)
    if terminal:
      size = shutil.get_terminal_size()
      text = self.viewport(text, size.columns, size.lines)
    if refresh and sys.stdout.isatty():
      print("\033[2J\033[H", end="")
    print(text)


HELP = """命令：
  read ADDRESS [cc0|cc1]
  write ADDRESS DATA [cc0|cc1]
  memory [ADDRESS]     查看 S 的全部内存或指定地址，不推进时间
  step [N] / 回车       推进 N 步，默认 1；逐步显示
  show                 查看当前状态，不推进时间
  n / p                下一页 / 上一页，不推进时间
  help                 显示帮助
  quit                 退出
地址和数据作为字符串；含空格时用引号，例如 write A "hello world"。
read 自动查缓存并发起共享读；write 自动失效旧副本并写存储。默认请求者为 cc0。"""


def main():
  parser = argparse.ArgumentParser(description="tinyzj 单步终端仪表盘")
  parser.add_argument("--buffer-capacity", type=int, default=2)
  parser.add_argument("--max-transactions", type=int, default=1)
  parser.add_argument("--script", help="加载 read/write/step N 命令；step N 需按 N 次 Enter")
  parser.add_argument("--delay", type=float, default=0.4, help="终端连续播放间隔秒数，0 表示不等待")
  args = parser.parse_args()
  if args.delay < 0:
    parser.error("--delay 不能为负数")
  script = None
  commands = []
  if args.script:
    try:
      with open(args.script, encoding="utf-8") as source:
        for line_number, line in enumerate(source.read().splitlines(), 1):
          words = shlex.split(line, comments=True)
          if words:
            commands.append({
              "line": line_number, "text": shlex.join(words),
              "words": words, "state": "pending",
            })
        script = iter(commands)
    except OSError as error:
      parser.error(str(error))
    except ValueError as error:
      parser.exit(1, f"{args.script}:{line_number}: {error}\n")
  try:
    dashboard = Dashboard(args.buffer_capacity, args.max_transactions)
  except ValueError as error:
    parser.error(str(error))
  dashboard.commands = commands
  dashboard.show(refresh=True)

  def wait_for_enter(prompt):
    while True:
      action = input(prompt).strip().lower()
      if action in ("n", "p"):
        dashboard.page += 1 if action == "n" else -1
        dashboard.show(refresh=True)
      elif action in ("", "q", "quit", "exit"):
        return not action

  def next_step():
    if not wait_for_enter(f"\nSTEP {dashboard.steps} | Enter / n/p / q> "):
      return False
    dashboard.step()
    if dashboard.current_command is not None:
      dashboard.command_steps += 1
      if dashboard.command_steps == int(dashboard.current_command["words"][1]):
        dashboard.current_command["state"] = "done"
    dashboard.show(refresh=True)
    return True

  pending_entry = None
  while True:
    try:
      if script is not None:
        entry = pending_entry if pending_entry is not None else next(script, None)
        pending_entry = None
        if entry is None:
          dashboard.current_command = None
          while dashboard.system.ring.in_flight:
            if not next_step():
              return
          break
        line_number = entry["line"]
        words = entry["words"]
        if words[0] in ("read", "write"):
          batch = [entry]
          for following in script:
            if following["words"][0] not in ("read", "write"):
              pending_entry = following
              break
            batch.append(following)
          for request_entry in batch:
            line_number = request_entry["line"]
            request_words = request_entry["words"]
            valid_lengths = (2, 3) if request_words[0] == "read" else (3, 4)
            if len(request_words) not in valid_lengths:
              raise ValueError("read/write 参数不正确")
            if len(request_words) == valid_lengths[-1]:
              dashboard._cc(request_words[-1])
          if not wait_for_enter(f"\nEnter 同时发起 {len(batch)} 条请求 / n/p / q> "):
            return
          dashboard.current_command = None
          for request_entry in batch:
            line_number = request_entry["line"]
            request_words = request_entry["words"]
            getattr(dashboard, request_words[0])(*request_words[1:])
            request_entry["state"] = "done"
          dashboard.show(refresh=True)
          continue
        dashboard.current_command = entry
        dashboard.command_steps = 0
        entry["state"] = "active"
        if words[0] not in ("read", "write", "step"):
          raise ValueError("命令文件只支持 read、write、step N")
        if words[0] == "step" and len(words) != 2:
          raise ValueError("请使用 step N，N 为正整数")
      else:
        words = shlex.split(input("\ntinyzj> "))
      command = words[0] if words else "step"
      arguments = words[1:]
      if command in ("quit", "exit") and not arguments:
        break
      if command == "help" and not arguments:
        print(HELP)
      elif command == "memory" and len(arguments) <= 1:
        print(dashboard.memory(*arguments))
      elif command == "show" and not arguments:
        dashboard.show(refresh=True)
      elif command in ("n", "p") and not arguments:
        dashboard.page += 1 if command == "n" else -1
        dashboard.show(refresh=True)
      elif command == "step" and len(arguments) <= 1:
        count = int(arguments[0]) if arguments else 1
        if count < 1:
          raise ValueError("步数必须是正整数")
        if script is not None:
          dashboard.show(refresh=True)
        for _ in range(count):
          if script is not None:
            if not next_step():
              return
          else:
            dashboard.step()
            dashboard.show(refresh=True)
          if script is None and count > 1 and sys.stdout.isatty():
            time.sleep(args.delay)
      elif command == "read" and len(arguments) in (1, 2):
        dashboard.read(*arguments)
        if script is not None:
          entry["state"] = "done"
        dashboard.show(refresh=True)
      elif command == "write" and len(arguments) in (2, 3):
        dashboard.write(*arguments)
        if script is not None:
          entry["state"] = "done"
        dashboard.show(refresh=True)
      else:
        raise ValueError("命令或参数不正确。输入 help 查看用法。")
    except ValueError as error:
      if script is not None:
        parser.exit(1, f"{args.script}:{line_number}: {error}\n")
      print(f"输入错误：{error}")
    except EOFError:
      if script is not None:
        parser.exit(1, "命令执行中输入结束：命令或事务尚未完成。\n")
      print("\n退出仪表盘。")
      break
    except KeyboardInterrupt:
      if script is not None:
        parser.exit(130, "命令执行被中断。\n")
      print("\n退出仪表盘。")
      break


if __name__ == "__main__":
  main()
