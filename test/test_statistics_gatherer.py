import unittest
import sys
sys.path.insert(0, '../src/')
from statistics_gatherer import SnapStatisticsGatherer
import graph_manager


class TestSnapStatisticsGatherer(unittest.TestCase):

    def test_init(self):
        # should initialize with no errors
        manager = graph_manager.SnapManager()
        gatherer = SnapStatisticsGatherer(manager)
        self.assertIsNotNone(gatherer)

    def test_fill_results(self):
        manager = graph_manager.SnapManager()
        gatherer = SnapStatisticsGatherer(manager)

        f = lambda x: x * 10

        # do nothing
        results =  gatherer.fill_results([], f)
        self.assertEquals([], results)

        # do something
        results =  gatherer.fill_results([5,7,8,9], f)
        self.assertEquals([50,70,80,90], results)





if __name__ == "__main__":
    unittest.main()
