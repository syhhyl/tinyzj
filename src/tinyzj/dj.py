class DongJiang:

  def __init__(self):
    self.data_by_address = {}

  def read(self, address):
    self._require_address(address)
    return self.data_by_address.get(address, "read data")

  def write(self, address, data):
    self._require_address(address)
    self.data_by_address[address] = data

  def _require_address(self, address):
    if address is None:
      raise ValueError("home request needs an address")
