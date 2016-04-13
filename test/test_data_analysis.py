from mock import patch
from mock import Mock
import unittest
import sys
sys.path.insert(0, '../src/')
from data_analysis import DataAnalysis
import data_parser

class TestDataAnalysis(unittest.TestCase):

    @patch('data_parser.file_paths', return_value=['a'])
    @patch('data_parser.lines')
    def test_build_employee_employer_graph(self, lines, file_paths):

        mock_manager = Mock()

        # call everything
        analysis = DataAnalysis(mock_manager)
        analysis.build_employee_employer_graph()








if __name__ == "__main__":
    unittest.main()
