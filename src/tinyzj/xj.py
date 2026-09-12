"""
xijiang
"""

class RingNode:
  
  def __init__(self, name, role):
    if not name:
      raise ValueError("ring node needs a name")
    self.name = name
    self.role = role

class Message:

  def __init__(self, source_name, target_name, payload=None):
    if not source_name:
      raise ValueError("message source needs a name")
    if not target_name:
      raise ValueError("message target needs a name")

    self.source_name = source_name
    self.target_name = target_name
    self.payload = payload 

class Injection:

  def __init__(self, message):
    self.message = message
    self.current_node_name = message.source_name



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
    
    self.in_flight = []
    
    
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
    
  def inject(self, message):
    self._node_index(message.source_name)
    self._node_index(message.target_name)
    
    injection = Injection(message)
    self.in_flight.append(injection)
    return injection

  def path_from(self, source_name, target_name):
    index = self._node_index(source_name)
    target_index = self._node_index(target_name)
    
    path = []
    
    while True:
      path.append(self.nodes[index])
      if index == target_index:
        return path
      index = (index + 1) % len(self.nodes)
    
  
  def _node_index(self, node_name):
    for index, node in enumerate(self.nodes):
      if node.name == node_name:
        return index
    
    raise ValueError(f"unknown ring node: {node_name}")