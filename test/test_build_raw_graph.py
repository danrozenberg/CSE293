import unittest
import sys
import mock
sys.path.insert(0, '../src/')
from build_raw_graph import *
import data_parser
import graph_manager


class TestDataAnalysis(unittest.TestCase):

    # TODO: if there is enough time, decouple from other classes.
    @mock.patch('build_raw_graph.process_file')
    def test_process_files(self, process_file_mock):
        src_folder = "./test_file_path_folder/"
        parser = data_parser.Pis12DataParser()
        manager = graph_manager.SnapManager

        process_files(src_folder, parser, manager)
        self.assertEquals(3, process_file_mock.call_count)






# class MockGraphManager():










if __name__ == "__main__":
    unittest.main()
