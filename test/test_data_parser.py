import unittest
import sys
sys.path.append('..')
from data_parser import DataParser

class TestDataParser(unittest.TestCase):

    # Should initialize
    def test_should_word(self):
        asd = DataParser()
        print "data parser is ok..."


if __name__ == "__main__":
    unittest.main()
