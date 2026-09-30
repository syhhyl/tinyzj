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

  def __init__(self, buffer_capacity=2, max_transactions=1, error_addresses=None,
               retry_enabled=False):
    self.system = Zhujiang(
      buffer_capacity=buffer_capacity,
      max_transactions=max_transactions,
      error_addresses=error_addresses,
      retry_enabled=retry_enabled,
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

  def _complete(self, endpoint, request):
    if request is None:
      return True
    if endpoint.read_response_for(request) is not None:
      return True
    if endpoint.write_response_for(request) is not None:
      return True
    return endpoint.request_states.get(request) == "complete"

  def read(self, address, cc="cc0"):
    return self._request("read_shared", address, cc)

  def write(self, address, data, cc="cc0"):
    return self._request("store", address, cc, data)

  def clean_invalid(self, address, cc="cc0"):
    return self._request("clean_invalid", address, cc)

  def clean_shared(self, address, cc="cc0"):
    return self._request("clean_shared", address, cc)

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

  def _with_memory(self, lines, color):
    def paint(text, style):
      return f"\033[{style}m{text}\033[0m" if color else text

    def columns(text):
      plain = re.sub(r"\033\[[0-9;]*m", "", text)
      return sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in plain)

    commands = [paint("命令列表", "1;37")]
    for entry in self.commands:
      state = entry["state"]
      marker, style = {
        "pending": ("○ 未执行", "2;37"),
        "active": ("▶ 执行中", "1;33"),
        "done": ("✓ 已完成", "32"),
      }[state]
      progress = ""
      if entry is self.current_command and entry["words"][0] == "step":
        progress = f" [{self.command_steps}/{entry['words'][1]}]"
      commands.append(paint(f"{marker}  {entry['text']}{progress}", style))
    panel = [paint("Memory · S", "1;37")]
    data = self.system.s.dj.data_by_address
    if data:
      panel.extend(f"  {address!r} = {value!r}" for address, value in data.items())
    else:
      panel.append("  （空）")
    for name, cache in (("CC0 cache", self.system.cc0.cache),
                        ("CC1 cache", self.system.cc1.cache)):
      panel.append(paint(name, "1;37"))
      panel.extend(f"  {address!r} = {data!r} [{state}]"
                   for address, (state, data) in cache.items())
    panel.append(paint("HF directory", "1;37"))
    panel.extend(
      f"  {address!r} = {', '.join(f'{node}:{state}' for node, state in holders.items())}"
      for address, holders in self.system.hf.directory.items()
    )
    panel.append(paint("HF dirty", "1;37"))
    panel.extend(f"  {address!r} = {data!r}"
                 for address, data in self.system.hf.dirty_data.items())

    total_width = max(map(columns, lines), default=0)
    left_width = max(total_width // 2, max(map(columns, commands), default=0) + 2)
    footer = ["", paint("─" * total_width, "2;37")]
    for index in range(max(len(commands), len(panel))):
      left = commands[index] if index < len(commands) else ""
      right = panel[index] if index < len(panel) else ""
      footer.append(left + " " * (left_width - columns(left)) + "│  " + right)
    return "\n".join(lines + footer)

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
      for entry in self.commands:
        request = entry.get("request")
        if entry["state"] == "active" and request is not None:
          endpoint = self._cc(entry["cc"])
          if self._complete(endpoint, request):
            entry["state"] = "done"
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
    gap = 16

    def message_label(injection):
      message = injection.message
      label = f"{message.opcode}[{message.channel}]  TxnID={message.transaction_id}"
      if message.dbid is not None:
        label += f"  DBID={message.dbid}"
      if injection in blocked_messages:
        label += "  WAIT"
      return label

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

    width = max(46, max((len(message_label(i)) + 2 for i in ring.in_flight), default=0))
    if sys.stdout.isatty():
      width = max(width, (shutil.get_terminal_size().columns - gap - 7) // 2)

    def messages_at(node, waiting=False):
      return [
        i for i in ring.in_flight
        if i.current_node_name == node and (i in blocked_messages) == waiting
      ]

    def box(node, role, height, waiting=False):
      messages = messages_at(node, waiting)
      edge = paint("│", "2;37")
      if waiting:
        edge = " "
        rows = []
      else:
        rows = [paint("╭" + "─" * width + "╮", "2;37")]
        rows.append(edge + paint(f"Node {role}".center(width), "1;37") + edge)
      content = []
      for injection in messages:
        message = injection.message
        style = "1;31" if injection in blocked_messages else colors[message.channel]
        content.append(edge + paint(centered(message_label(injection)), style) + edge)
      rows.extend(content)
      for _ in range(height - len(content)):
        rows.append(edge + " " * width + edge)
      if not waiting:
        rows.append(paint("╰" + "─" * width + "╯", "2;37"))
      return rows

    def horizontal(left, right, row):
      if row not in (2, 3, 4, 5):
        return " " * gap
      channel = list(colors)[row - 2]
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
      padding = width + 2 - 24
      return " " * (padding // 2) + "".join(lanes) + " " * (padding - padding // 2)

    lines = []
    for left, right, roles in (("n00", "n01", ("CC0", "HF")), ("n10", "n11", ("S", "CC1"))):
      waiting_height = max(len(messages_at(left, True)), len(messages_at(right, True)))
      height = max(4, len(messages_at(left)), len(messages_at(right)))
      sources = [
        "  " + a + " " * gap + b
        for a, b in zip(box(left, roles[0], waiting_height, True), box(right, roles[1], waiting_height, True))
      ]
      if left == "n00":
        lines.extend(sources)
      left_box, right_box = box(left, roles[0], height), box(right, roles[1], height)
      for row, (a, b) in enumerate(zip(left_box, right_box)):
        link = horizontal(left, right, row)
        lines.append("  " + a + link + b)
      if left == "n10":
        lines.extend(sources)
      if left == "n00":
        for row in range(7):
          lines.append(
            "  "
            + vertical("n00", "n10", row)
            + " " * gap
            + vertical("n01", "n11", row)
          )
    return self._with_memory(lines, color)

  def viewport(self, text, columns, rows):
    columns = max(1, columns - 1)
    page_height = max(1, rows - 5)
    lines = text.splitlines()
    ansi = re.compile(r"\033\[[0-9;]*m|.")
    plain_lines = [re.sub(r"\033\[[0-9;]*m", "", line) for line in lines]
    required_width = max((sum(2 if unicodedata.east_asian_width(c) in ("W", "F") else 1 for c in line) for line in plain_lines), default=0)
    overflow = len(lines) > page_height or required_width > columns
    if overflow:
      page_height = max(1, page_height - 1)

    result = []
    for line in lines[:page_height]:
      position = 0
      output = []
      for token in ansi.findall(line):
        if token.startswith("\033"):
          output.append(token)
          continue
        size = 2 if unicodedata.east_asian_width(token) in ("W", "F") else 1
        start, end = 0, columns
        if position >= start and position + size <= end:
          output.append(token)
        elif position < end and position + size > start:
          output.append(" " * (min(position + size, end) - max(position, start)))
        position += size
      result.append("".join(output) + ("\033[0m" if "\033[" in text else ""))
    result.extend([""] * (page_height - len(result)))
    if overflow:
      result.append(f"Resize terminal: need {required_width + 1} x {len(lines) + 5}"[:columns])
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
  read ADDRESS [cc0|cc1]        共享读（ReadShared），未命中才进网络
  write ADDRESS DATA [cc0|cc1]  读改本地缓存（ReadUnique 后写为 UD）
  clean-invalid ADDRESS [cc0|cc1] 维护：使其他副本失效（CleanInvalid）
  clean-shared ADDRESS [cc0|cc1]  维护：清共享但保留可读副本（CleanShared）
  memory [ADDRESS]     查看 S 的全部内存或指定地址，不推进时间
  step [N] / 回车       推进 N 步，默认 1；逐步显示
  show                 查看当前状态，不推进时间
  help                 显示帮助
  quit                 退出
地址和数据作为字符串；含空格时用引号，例如 write A "hello world"。
read 自动查缓存并发起共享读；write 取得独占权限后更新本地缓存为 UD，不立即写入 S。默认请求者为 cc0。
clean-invalid / clean-shared 发出的维护请求是 Dataless；若被探测副本为 UD，SnpRespData 仍会通过 DAT 返回脏数据。--retry 开启 RetryAck / PCrdGrant 流控。"""


SCRIPT_REQUESTS = {
  "read": (2, 3),
  "write": (3, 4),
  "clean-invalid": (2, 3),
  "clean-shared": (2, 3),
}
SCRIPT_METHODS = {
  "read": "read",
  "write": "write",
  "clean-invalid": "clean_invalid",
  "clean-shared": "clean_shared",
}


def _main():
  parser = argparse.ArgumentParser(description="tinyzj 单步终端仪表盘")
  parser.add_argument("--buffer-capacity", type=int, default=2)
  parser.add_argument("--max-transactions", type=int, default=1)
  parser.add_argument("--retry", action="store_true",
                      help="开启 RetryAck / PCrdGrant 流控（retry_enabled=True）")
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
    dashboard = Dashboard(args.buffer_capacity, args.max_transactions,
                        retry_enabled=args.retry)
  except ValueError as error:
    parser.error(str(error))
  dashboard.commands = commands
  dashboard.show(refresh=True)

  def wait_for_enter(prompt):
    while True:
      action = input(prompt).strip().lower()
      if action in ("", "q", "quit", "exit"):
        return not action

  def next_step():
    if not wait_for_enter(f"\nSTEP {dashboard.steps} | Enter / q> "):
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
        if words[0] in SCRIPT_REQUESTS:
          batch = [entry]
          if words[0] in ("read", "write"):
            for following in script:
              if following["words"][0] not in ("read", "write"):
                pending_entry = following
                break
              batch.append(following)
          for request_entry in batch:
            line_number = request_entry["line"]
            request_words = request_entry["words"]
            if len(request_words) not in SCRIPT_REQUESTS[request_words[0]]:
              raise ValueError(f"{request_words[0]} 参数不正确")
            if len(request_words) == SCRIPT_REQUESTS[request_words[0]][-1]:
              dashboard._cc(request_words[-1])
          if not wait_for_enter(f"\nEnter 同时发起 {len(batch)} 条请求 / q> "):
            return
          dashboard.current_command = None
          for request_entry in batch:
            line_number = request_entry["line"]
            request_words = request_entry["words"]
            request = getattr(dashboard, SCRIPT_METHODS[request_words[0]])(*request_words[1:])
            request_entry["request"] = request
            default_length = 3 if request_words[0] == "write" else 2
            request_entry["cc"] = request_words[-1] if len(request_words) > default_length else "cc0"
            request_entry["state"] = "done" if request is None else "active"
          dashboard.show(refresh=True)
          continue
        dashboard.current_command = entry
        dashboard.command_steps = 0
        entry["state"] = "active"
        if words[0] != "step":
          raise ValueError("命令文件只支持 read/write/clean-invalid/clean-shared/step N")
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
      elif command == "clean-invalid" and len(arguments) in (1, 2):
        dashboard.clean_invalid(*arguments)
        if script is not None:
          entry["state"] = "done"
        dashboard.show(refresh=True)
      elif command == "clean-shared" and len(arguments) in (1, 2):
        dashboard.clean_shared(*arguments)
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


def main():
  terminal = sys.stdout.isatty()
  if terminal:
    print("\033[?1049h\033[2J\033[H", end="", flush=True)
  try:
    _main()
  finally:
    if terminal:
      print("\033[0m\033[?1049l", end="", flush=True)


if __name__ == "__main__":
  main()
