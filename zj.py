from dj import DongJiang
from xj import Ring

class Zhujiang:
  
  def __init__(self):
    self.ring = Ring()
    
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
  print("Zhujiang / Ring connections: ")
  for port, module in system.ring.connections.items():
    print(f" {port} <-> {type(module).__name__}")
  print(f"HomeWrapper: {type(system.home.dj).__name__}")