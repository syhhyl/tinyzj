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
    resp_err="OK",
    resp=None,
    exp_comp_ack=False,
    home_nid=None,
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
    self.resp_err = resp_err
    self.resp = resp
    self.exp_comp_ack = exp_comp_ack
    self.home_nid = home_nid

class Injection:

  def __init__(self, message):
    self.message = message
    self.current_node_name = message.source_name
    self.direction = None
    self.in_ring = False



class Ring:

  def __init__(self, nodes, buffer_capacity=None):
    self.nodes = list(nodes)
    if len(self.nodes) < 3:
      raise ValueError("ring needs at least three nodes")

    self.buffer_capacity = buffer_capacity
    if buffer_capacity is not None:
      if not isinstance(buffer_capacity, int) or buffer_capacity < 2:
        raise ValueError("buffer_capacity must be an integer >= 2")
    
    self.source_queues = {
      node.name: {
        channel: []
        for channel in (Channel.REQ, Channel.RSP, Channel.DAT, Channel.SNP)
      }
      for node in self.nodes
    }
    self.ring_buffers = {
      node.name: {
        (direction, channel): []
        for direction in (-1, 1)
        for channel in (Channel.REQ, Channel.RSP, Channel.DAT, Channel.SNP)
      }
      for node in self.nodes
    }

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
    if message.channel not in (
      Channel.REQ,
      Channel.RSP,
      Channel.DAT,
      Channel.SNP,
    ):
      raise ValueError("message needs a known channel")
    self._node_index(message.source_name)
    self._node_index(message.target_name)

    injection = Injection(message)
    self.source_queues[message.source_name][message.channel].append(injection)
    if message.source_name != message.target_name:
      path = self.path_from(message.source_name, message.target_name)
      source_index = self._node_index(message.source_name)
      next_index = self._node_index(path[1].name)
      injection.direction = (
        1 if next_index == (source_index + 1) % len(self.nodes) else -1
      )
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
    initial = list(self.in_flight)
    buffer_occupancy = {
      (node_name, direction, channel): len(buffer)
      for node_name, buffers in self.ring_buffers.items()
      for (direction, channel), buffer in buffers.items()
    }
    waiting = [
      injection
      for injection in initial
      if injection.current_node_name == injection.message.target_name
    ]
    deliveries = []
    delivery_slots = set()
    for injection in waiting:
      slot = (injection.current_node_name, injection.message.channel)
      if slot in delivery_slots:
        continue
      receiver = self.connections[injection.current_node_name]
      if receiver is None:
        raise ValueError(
          f"ring node is not connected: {injection.current_node_name}"
        )
      if not callable(getattr(receiver, "receive", None)):
        raise TypeError(
          f"ring node cannot receive messages: {injection.current_node_name}"
        )
      delivery_slots.add(slot)
      can_receive = getattr(receiver, "can_receive", None)
      if callable(can_receive) and not can_receive(injection.message):
        continue
      deliveries.append((injection, receiver))

    for injection, receiver in deliveries:
      receiver.receive(injection.message)

    new_injections = [
      injection for injection in self.in_flight if injection not in initial
    ]
    delivered = [injection for injection, _ in deliveries]
    for injection in delivered:
      message = injection.message
      if injection.in_ring:
        self.ring_buffers[injection.current_node_name][
          (injection.direction, message.channel)
        ].remove(injection)
      else:
        self.source_queues[message.source_name][message.channel].remove(
          injection
        )

    self.in_flight = [
      injection for injection in initial if injection not in delivered
    ] + new_injections
    occupied_links = set()
    for injection in initial:
      if not injection.in_ring:
        continue
      if injection.current_node_name == injection.message.target_name:
        continue
      current_index = self._node_index(injection.current_node_name)
      next_index = (current_index + injection.direction) % len(self.nodes)
      link = (
        injection.current_node_name,
        self.nodes[next_index].name,
        injection.message.channel,
      )
      if link in occupied_links:
        continue
      next_node_name = self.nodes[next_index].name
      buffer = self.ring_buffers[next_node_name][
        (injection.direction, injection.message.channel)
      ]
      if (
        self.buffer_capacity is not None
        and buffer_occupancy[
          (next_node_name, injection.direction, injection.message.channel)
        ] >= self.buffer_capacity
      ):
        continue
      occupied_links.add(link)
      self.ring_buffers[injection.current_node_name][
        (injection.direction, injection.message.channel)
      ].remove(injection)
      injection.current_node_name = next_node_name
      buffer.append(injection)

    queue_heads = []
    seen_queues = set()
    for injection in initial:
      if injection.in_ring:
        continue
      if injection.current_node_name == injection.message.target_name:
        continue
      queue_key = (injection.message.source_name, injection.message.channel)
      if queue_key in seen_queues:
        continue
      seen_queues.add(queue_key)
      queue_heads.append(injection)

    for injection in queue_heads:
      current_name = injection.current_node_name
      current_index = self._node_index(current_name)
      next_index = (current_index + injection.direction) % len(self.nodes)
      next_node_name = self.nodes[next_index].name
      link = (current_name, next_node_name, injection.message.channel)
      if link in occupied_links:
        continue
      occupancy_key = (
        next_node_name,
        injection.direction,
        injection.message.channel,
      )
      if (
        self.buffer_capacity is not None
        and buffer_occupancy[occupancy_key] > self.buffer_capacity - 2
      ):
        continue
      occupied_links.add(link)
      self.source_queues[current_name][injection.message.channel].remove(injection)
      injection.in_ring = True
      injection.current_node_name = next_node_name
      self.ring_buffers[next_node_name][
        (injection.direction, injection.message.channel)
      ].append(injection)
    
  
  def _node_index(self, node_name):
    for index, node in enumerate(self.nodes):
      if node.name == node_name:
        return index
    
    raise ValueError(f"unknown ring node: {node_name}")
