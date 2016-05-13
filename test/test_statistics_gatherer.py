import os
import unittest
import sys
sys.path.insert(0, '../src/')
from statistics_gatherer import StatisticsGatherer
import graph_manager


class TestSnapStatisticsGatherer(unittest.TestCase):

    def test_historgram(self):
        gatherer = StatisticsGatherer
        manager = graph_manager.SnapManager()
        manager.generate_random_graph(20,2,0.5)
        node_list = range(20)
        method = manager.get_shortest_path_size

        results = gatherer.fill_results(node_list, method)
        file_path = "./test.jpg"
        gatherer.build_histogram(results, file_path)

        # file is saved
        self.assertTrue(os.path.isfile(file_path))

        # cleanup
        os.remove(file_path)

    def test_get_node_sample(self):
        gatherer = StatisticsGatherer
        manager = graph_manager.SnapManager()
        manager.generate_random_graph(20,2,0.5)

        sample = gatherer.get_node_sample(manager, 5)
        self.assertEquals(5, len(sample))

        sample = gatherer.get_node_sample(manager, 999)
        self.assertEquals(20, len(sample))


if __name__ == "__main__":
    unittest.main()
