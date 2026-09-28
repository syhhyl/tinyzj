"""Integer semantics of CHI AtomicLoad/AtomicStore (IHI0050 G B4.2)."""

OPERATIONS = ("ADD", "CLR", "EOR", "SET", "SMAX", "SMIN", "UMAX", "UMIN")
LOAD_OPCODES = tuple("AtomicLoad" + op for op in OPERATIONS)
STORE_OPCODES = tuple("AtomicStore" + op for op in OPERATIONS)


def calculate(operation, initial, operand):
  bits = 8 * len(initial)
  mask = (1 << bits) - 1
  old = int.from_bytes(initial, "little")
  value = int.from_bytes(operand, "little")
  signed_old = int.from_bytes(initial, "little", signed=True)
  signed_value = int.from_bytes(operand, "little", signed=True)
  result = {
    "ADD": lambda: (old + value) & mask,
    "CLR": lambda: old & (~value & mask),
    "EOR": lambda: old ^ value,
    "SET": lambda: old | value,
    "SMAX": lambda: value if signed_value > signed_old else old,
    "SMIN": lambda: value if signed_value < signed_old else old,
    "UMAX": lambda: max(old, value),
    "UMIN": lambda: min(old, value),
  }[operation]()
  return result.to_bytes(len(initial), "little")
