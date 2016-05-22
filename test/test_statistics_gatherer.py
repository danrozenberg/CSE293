import os
import unittest
import sys
sys.path.insert(0, '../src/')
from statistics_gatherer import StatisticsGatherer
from data_parser import ClassificationLoader
import graph_manager


class TestSnapStatisticsGatherer(unittest.TestCase):

    def test_get_node_sample(self):
        gatherer = StatisticsGatherer
        manager = graph_manager.SnapManager()
        manager.generate_random_graph(20,2,0.5)

        sample = gatherer.get_node_sample(manager, 5)
        self.assertEquals(5, len(sample))

        sample = gatherer.get_node_sample(manager, 999)
        self.assertEquals(20, len(sample))

    def test_build_ground_truth(self):
        # also tests save and load"

        gatherer = StatisticsGatherer
        load_folder = "./classification_files/"
        output_path = "./classification_files/ground_truth.p"

        # file gets built
        truth_data = gatherer.build_ground_truth(load_folder, output_path)

        # file is saved
        self.assertTrue(os.path.isfile(output_path))

        # file gets loaded
        truth_data2 = gatherer.load_ground_truth(output_path)

        # matchin a few things is enough, I trust python...
        self.assertTrue(len(truth_data) > 2)
        self.assertEqual(len(truth_data), len(truth_data2))

        # cleanup
        os.remove(output_path)

    def test_get_plant_sample(self):
        loader = ClassificationLoader()
        loader.parse_line()

        valid_line = ['7','56750184','1999','777','1995','0','0','1','0']
        answer = parser.parse_line(valid_line)



if __name__ == "__main__":
    unittest.main()
