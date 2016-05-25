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

    def test_valid_plants(self):
        affiliation_graph = graph_manager.SnapManager()

        # some workers
        affiliation_graph.add_node(10)
        affiliation_graph.add_node(20)
        affiliation_graph.add_node(30)
        affiliation_graph.add_node_attr(10, "type", "worker")
        affiliation_graph.add_node_attr(20, "type", "worker")
        affiliation_graph.add_node_attr(30, "type", "worker")

        affiliation_graph.add_node(100)
        affiliation_graph.add_node(200)
        affiliation_graph.add_node(300)
        affiliation_graph.add_node_attr(100, "type", "employer")
        affiliation_graph.add_node_attr(200, "type", "employer")
        affiliation_graph.add_node_attr(300, "type", "employer")


        # create truth_data
        parser = ClassificationLoader()
        valid_line = ['999','10','1999','777','1995','0','0','1','0']
        parser.parse_line(valid_line)
        valid_line = ['999','200','1999','777','1995','0','0','0','1']
        parser.parse_line(valid_line)
        valid_line = ['999','300','1999','777','1995','0','0','0','0']
        parser.parse_line(valid_line)

        truth_data = parser.truth_data
        valid_truth_data = StatisticsGatherer.get_valid_plants(truth_data,
                                                               affiliation_graph)
        self.assertEqual(2, len(valid_truth_data))
        self.assertTrue(200 in valid_truth_data.keys())
        self.assertTrue(300 in valid_truth_data.keys())







if __name__ == "__main__":
    unittest.main()
