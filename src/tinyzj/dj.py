class DongJiang:

  def __init__(self):
    self.data_by_address = {}

  def read(self, address):
    self._require_address(address)
    return (
      self.data_by_address.get(address, "read data"),
      address in self.data_by_address,
    )

  def write(self, address, data):
    self._require_address(address)
    self.data_by_address[address] = data

  def _require_address(self, address):
    if address is None:
      raise ValueError("home request needs an address")

  def write_partial(self, address, data, byte_enable):
    old = self.data_by_address.get(address, bytes(64))
    if not isinstance(old, bytes) or len(old) != 64:
      raise ValueError("partial write requires byte-addressed line storage")
    self.write(address, bytes(data[i] if byte_enable & (1 << i) else old[i] for i in range(64)))
