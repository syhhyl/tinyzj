import unittest

from tinyzj.dj import DongJiang


class DongJiangTest(unittest.TestCase):
  def test_reads_written_data_and_returns_default_for_other_addresses(self):
    dongjiang = DongJiang()

    dongjiang.write("0x1000", "value 1")

    self.assertEqual("value 1", dongjiang.read("0x1000"))
    self.assertEqual("read data", dongjiang.read("0x2000"))

  def test_rejects_a_missing_address(self):
    dongjiang = DongJiang()

    with self.assertRaisesRegex(ValueError, "home request needs an address"):
      dongjiang.read(None)
    with self.assertRaisesRegex(ValueError, "home request needs an address"):
      dongjiang.write(None, "value 1")


if __name__ == "__main__":
  unittest.main()
