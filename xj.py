"""
xijiang
"""

class RingNode:
  
  def __init__(self, name, role):
    if not name:
      raise ValueError("ring node needs a name")
    self.name = name
    self.role = role


class Ring:

  def __init__(self, nodes):
    self.nodes = list(nodes)
    if len(self.nodes) < 3:
      raise ValueError("ring needs at least three nodes")
    
    self.connections = {}
    for node in self.nodes:
      if node.name in self.connections:
        raise ValueError(f"duplicate ring node: {node.name}")
      self.connections[node.name] = None
    
    
  def connect(self, node_name, module):
    if node_name not in self.connections:
      raise ValueError(f"unknown ring node: {node_name}")
    if self.connections[node_name] is not None:
      raise ValueError(f"ring node already connected: {node_name}")
    
    self.connections[node_name] = module
    

  def neighbors_of(self, node_name):
    for index, node in enumerate(self.nodes):
      if node.name == node_name:
        return (
          self.nodes[(index-1) % len(self.nodes)],
          self.nodes[(index+1) % len(self.nodes)],
        )

    raise ValueError(f"unknown ring node: {node_name}")