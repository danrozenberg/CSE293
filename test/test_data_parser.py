import unittest
import sys
sys.path.insert(0, '../src/')
from data_parser import Pis12DataParser

class TestPis12DataParser(unittest.TestCase):

    def test_find_files(self):
        parser = Pis12DataParser()

        # should return no files for empty folder
        files = parser.find_files("./empty_folder/")
        found_files = []
        for f in files :
            found_files.append(f)
        self.assertEquals(0, len(found_files))

        # should return 3 csv files in test folder
        files = parser.find_files("./test_file_path_folder/")
        found_files = []
        for f in files :
            found_files.append(f)
        self.assertEquals(3, len(found_files))
        
        # should return 3 csv files in test folder
        files = parser.find_files("./test_file_path_folder/", "csv")
        found_files = []
        expected_files = ["./test_file_path_folder/asd.csv",
                          "./test_file_path_folder/basd.csv",
                          "./test_file_path_folder/alpha.csv"]
        for f in files :
            found_files.append(f)
        self.assertEquals(3, len(found_files))
        self.assertItemsEqual(expected_files, found_files)

        
        # should return 1 txt files in test folder
        files = parser.find_files("./test_file_path_folder/", file_type="txt")
        found_files = []
        expected_files = ["./test_file_path_folder/nope.txt"]
        for f in files :
            found_files.append(f)
        self.assertEquals(1, len(found_files))
        self.assertItemsEqual(expected_files, found_files)


        # should return 2 csv files in test folder if requested for only 2
        files = parser.find_files("./test_file_path_folder/", 2)
        found_files = []
        for f in files :
            found_files.append(f)
        self.assertEquals(2, len(found_files))

    def test_raw_lines_reader(self):
        parser = Pis12DataParser()

        # read entire file
        self.assertIsNone(parser.get_last_read_line())
        lines_reader = parser.lines_reader("./test_data/raw_graph.csv")
        lines_read = 0
        for line in lines_reader:
            # this data source has 66 fields
            self.assertEquals(67, len(line))

            lines_read += 1
        self.assertEquals(30, lines_read)

        # read just a bit
        self.assertIsNone(parser.get_last_read_line())
        lines_reader = parser.lines_reader("./test_data/raw_graph.csv", 22)
        lines_read = 0
        for line in lines_reader:
            # this data source has 66 fields
            self.assertEquals(67, len(line))

            lines_read += 1
        self.assertEquals(22, lines_read)

        # can't read more than what is already there...
        self.assertIsNone(parser.get_last_read_line())
        lines_reader = parser.lines_reader("./test_data/raw_graph.csv", 999)
        lines_read = 0
        for line in lines_reader:
            # this data source has 66 fields
            self.assertEquals(67, len(line))

            lines_read += 1
        self.assertEquals(30, lines_read)

    def test_lines_reader(self):

        parser = Pis12DataParser()

        # read
        self.assertIsNone(parser.get_last_read_line())
        lines_reader = parser.lines_reader("./test_data/raw_graph.csv")
        lines_read = 0
        for line in lines_reader:
            # this data source has 66 fields
            self.assertEquals(67, len(line))

            lines_read += 1
        self.assertEquals(30, lines_read)

    def test_parse_line(self):
        parser = Pis12DataParser()

        with self.assertRaises(ValueError) as bad_call:
            parser.parse_line([1,2,3])
        the_exception = bad_call.exception
        self.assertIn("Unexpected format", the_exception.message)

        valid_line = ['2010', '', '', '', '', '',
                      '-1', '785', '', '', '', '',
                      '', '', '', '', '', '', '',
                      '', '1', '8', '10', '', '',
                      '100', '', '', '', '', '',
                      '', '', '', '', '7', '5',
                      '', '4483', '10', '', '',
                      '', '', '', '1', '', '',
                      '', '28,10', '', '30,3',
                      '', '', '', '1', '1', '25',
                      '', '19,24', '', '1', '', '',
                      '1', '', '']
        answer = parser.parse_line(valid_line)

        # test only a few of the entries in the dictionary
        self.assertEquals('2010', answer['ANO'])
        self.assertEquals('1', answer['PIS'])
        self.assertEquals('100', answer['IDENTIFICAD'])











if __name__ == "__main__":
    unittest.main()
