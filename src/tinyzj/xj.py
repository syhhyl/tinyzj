from .chi import Channel


class RingNode:
  
  def __init__(self, name, role):
    if not name:
      raise ValueError("ring node needs a name")
    self.name = name
    self.role = role

class Message:

  def __init__(
    self,
    source_name,
    target_name,
    payload=None,
    address=None,
    message_type="message",
    transaction_id=None,
    data_present=None,
    channel=None,
    opcode=None,
    dbid=None,
  ):
    if not source_name:
      raise ValueError("message source needs a name")
    if not target_name:
      raise ValueError("message target needs a name")

    self.source_name = source_name
    self.target_name = target_name
    self.payload = payload
    self.address = address
    self.message_type = message_type
    self.transaction_id = transaction_id
    self.dbid = dbid
    self.data_present = data_present
    self.channel = channel
    self.opcode = opcode

class Injection:

  def __init__(self, message):
    self.message = message
    self.current_node_name = message.source_name
    self.direction = None



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
    if message.channel not in (Channel.REQ, Channel.RSP, Channel.DAT, Channel.ERQ):
      raise ValueError("message needs a known channel")
    self._node_index(message.source_name)
    self._node_index(message.target_name)

    injection = Injection(message)
    if message.source_name != message.target_name:
      path = self.path_from(message.source_name, message.target_name)
      source_index = self._node_index(message.source_name)
      next_index = self._node_index(path[1].name)
      injection.direction = 1 if next_index == (source_index + 1) % len(self.nodes) else -1
    self.in_flight.append(injection)
    return injection

  def path_from(self, source_name, target_name):
    index = self._node_index(source_name)
    target_index = self._node_index(target_name)

    forward_distance = (target_index - index) % len(self.nodes)
    backward_distance = (index - target_index) % len(self.nodes)
    direction = 1 if forward_distance <= backward_distance else -1
    path = []

    while True:
      path.append(self.nodes[index])
      if index == target_index:
        return path
      index = (index + direction) % len(self.nodes)

  def step(self):
    in_flight = list(self.in_flight)
    arrived = [
      injection
      for injection in in_flight
      if injection.current_node_name == injection.message.target_name
    ]
    moving = [injection for injection in in_flight if injection not in arrived]
    receivers = []
    for injection in arrived:
      receiver = self.connections[injection.current_node_name]
      if receiver is None:
        raise ValueError(
          f"ring node is not connected: {injection.current_node_name}"
        )
      if not callable(getattr(receiver, "receive", None)):
        raise TypeError(
          f"ring node cannot receive messages: {injection.current_node_name}"
        )
      receivers.append(receiver)

    for injection, receiver in zip(arrived, receivers):
      receiver.receive(injection.message)

    new_injections = [
      injection for injection in self.in_flight if injection not in in_flight
    ]
    self.in_flight = moving + new_injections

    occupied_links = set()
    for injection in moving:
      current_index = self._node_index(injection.current_node_name)
      next_index = (current_index + injection.direction) % len(self.nodes)
      link = (
        injection.current_node_name,
        self.nodes[next_index].name,
        injection.message.channel,
      )
      if link in occupied_links:
        continue
      occupied_links.add(link)
      injection.current_node_name = self.nodes[next_index].name
    
  
  def _node_index(self, node_name):
    for index, node in enumerate(self.nodes):
      if node.name == node_name:
        return index
    
    raise ValueError(f"unknown ring node: {node_name}")
