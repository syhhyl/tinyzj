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
    self.data_present = data_present

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
    self.next_transaction_id = 0
    
    
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

    if message.transaction_id is None:
      message.transaction_id = self.next_transaction_id
      self.next_transaction_id += 1
    
    injection = Injection(message)
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

    for injection in moving:
      path = self.path_from(
        injection.current_node_name,
        injection.message.target_name,
      )
      injection.current_node_name = path[1].name
    
  
  def _node_index(self, node_name):
    for index, node in enumerate(self.nodes):
      if node.name == node_name:
        return index
    
    raise ValueError(f"unknown ring node: {node_name}")
