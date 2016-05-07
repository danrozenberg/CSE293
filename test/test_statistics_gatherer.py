import os
import unittest
import sys
sys.path.insert(0, '../src/')
from statistics_gatherer import StatisticsGatherer
import graph_manager


class TestSnapStatisticsGatherer(unittest.TestCase):

    def test_fill_results(self):
        gatherer = StatisticsGatherer
        f = lambda x: x * 10

        # do nothing
        results =  gatherer.fill_results([], f)
        self.assertEquals([], results)

        # do something
        results =  gatherer.fill_results([5,7,8,9], f)
        self.assertEquals([50,70,80,90], results)

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


if __name__ == "__main__":
    unittest.main()
