from dj import DongJiang
from xj import Ring, RingNode

class Zhujiang:
  
  def __init__(self):
    nodes = []
    
    nodes.append(RingNode("cc", "CPU cluster"))
    nodes.append(RingNode("home", "coherent home"))
    nodes.append(RingNode("io", "memory and IO"))
    
    self.ring = Ring(nodes)
    
    self.socket = Socket()
    self.home = HomeWrapper()
    self.io_wrapper = IoWrapper()

    self.ring.connect("cc", self.socket)
    self.ring.connect("home", self.home)
    self.ring.connect("io", self.io_wrapper)



class Socket:
  pass


class HomeWrapper:
  
  def __init__(self):
    self.dj = DongJiang()
    

class IoWrapper:
  pass




if __name__ == "__main__":
  system = Zhujiang()
  
  for node in system.ring.nodes:
    module = system.ring.connections[node.name]
    prev, next = system.ring.neighbors_of(node.name)
    print(f"{node.name}: {node.role}, previous={prev.name}, next={next.name}, {type(module).__name__}")
    
  print(f"HomeWrapper 内部：{type(system.home.dj).__name__}")
  
  path = system.ring.path_from("cc", "io")
  print(f"cc 到 io 的前向路径：{" -> ".join(node.name for node in path)}")
