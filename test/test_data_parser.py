from sets import Set
import unittest
import sys
import config_manager
sys.path.append('..')
from data_parser import DataParser

class TestDataParser(unittest.TestCase):

    def setUp(self):
        self.config = config_manager.Config()

    # name generator should work
    def test_name_generator(self):
        parser = DataParser()
        target_folder = self.config.data_path + "base-ano/"


        expected = Set(["ano2005.csv",
                    "ano2006.csv",
                    "ano2007.csv",
                    "ano2008.csv" ])

        answer = Set()
        for file_name in parser.file_name_generator(target_folder, 0):
            answer.add(file_name)
        self.assertEquals(answer, expected)

        answer = Set()
        for file_name in parser.file_name_generator(target_folder):
            answer.add(file_name)
        self.assertEquals(answer, expected)

        # listdir doesn't have a predictable order, let's just count the return numbers
        answer = Set()
        for file_name in parser.file_name_generator(target_folder, 1):
            answer.add(file_name)
        self.assertEquals(len(answer), 1)

        answer = Set()
        for file_name in parser.file_name_generator(target_folder, 2):
            answer.add(file_name)
        self.assertEquals(len(answer), 2)

        answer = Set()
        for file_name in parser.file_name_generator(target_folder, 3):
            answer.add(file_name)
        self.assertEquals(len(answer), 3)

        answer = Set()
        for file_name in parser.file_name_generator(target_folder, 4):
            answer.add(file_name)
        self.assertEquals(len(answer), 4)

        answer = Set()
        for file_name in parser.file_name_generator(target_folder, 100):
            answer.add(file_name)
        self.assertEquals(len(answer), 4)

    # should retrieve lines from files on demand.
    def test_line_generator(self):
        parser = DataParser()
        target_folder = self.config.data_path + "base-ano/"

        lines = []

        lines_to_fetch = 10
        for file_name in parser.file_name_generator(target_folder, 1):
            for line in parser.file_line_reader(target_folder + file_name, lines_to_fetch):
                lines.append(line)

        self.assertEquals(len(lines), lines_to_fetch)






if __name__ == "__main__":
    unittest.main()
