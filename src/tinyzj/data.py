"""64-byte line packetization for the modeled 128/256/512-bit DAT buses."""

from copy import copy

from .chi import Channel, RespErr


def packets(message, width):
  if message.channel != Channel.DAT or not isinstance(message.payload, bytes) or len(message.payload) != 64:
    return [message]
  result = []
  for offset in range(0, 64, width):
    packet = copy(message)
    packet.payload = message.payload[offset:offset + width]
    packet.data_id = offset // 16
    packet.line_bytes = 64
    result.append(packet)
  return result


class DataAssembly:

  def __init__(self, width):
    self.width = width
    self.pending = {}

  def accept(self, message):
    if message.channel != Channel.DAT or message.line_bytes is None:
      return message
    expected = set(range(0, 4, self.width // 16))
    if message.line_bytes != 64 or message.data_id not in expected:
      raise ValueError("invalid full-line DataID")
    if not isinstance(message.payload, bytes) or len(message.payload) != self.width:
      raise ValueError("DAT payload does not match bus width")
    key = (message.source_name, message.transaction_id, message.opcode)
    parts = self.pending.get(key, {})
    if message.data_id in parts:
      raise ValueError("duplicate DAT packet")
    if parts:
      first = next(iter(parts.values()))
      for field in ("target_name", "home_nid", "dbid", "resp", "pass_dirty", "address", "data_present"):
        if getattr(message, field) != getattr(first, field):
          raise ValueError(f"inconsistent DAT field: {field}")
    self.pending.setdefault(key, {})[message.data_id] = message
    if set(self.pending[key]) != expected:
      return None
    parts = self.pending.pop(key)
    result = copy(parts[min(parts)])
    result.payload = b"".join(parts[index].payload for index in sorted(parts))
    result.line_bytes = None
    errors = {part.resp_err for part in parts.values()}
    result.resp_err = next((error for error in (RespErr.NDERR, RespErr.DERR) if error in errors), RespErr.OK)
    return result
