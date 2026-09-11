"""
xijiang
"""


class Ring:

  def __init__(self):
    self.connections: dict[str, object] = {}
    
  def connect(self, port: str, module: object):
    if port in self.connections:
      raise ValueError(f"Port already connected: {port}")
    self.connections[port] = module