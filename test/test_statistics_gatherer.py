import os
import unittest
import sys
sys.path.insert(0, '../src/')
from statistics_gatherer import StatisticsGatherer
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

    def test_get_node_sample_with_type(self):
        gatherer = StatisticsGatherer
        manager = graph_manager.SnapManager()
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)
        manager.add_node(4)
        manager.add_node(5)
        manager.add_node(6)
        manager.add_node(7)
        manager.add_node(8)
        manager.add_node(9)
        manager.add_node(10)
        manager.add_node_attr(1, "type", "worker")
        manager.add_node_attr(2, "type", "plant")
        manager.add_node_attr(3, "type", "worker")
        manager.add_node_attr(4, "type", "plant")
        manager.add_node_attr(5, "type", "worker")
        manager.add_node_attr(6, "type", "plant")
        manager.add_node_attr(7, "type", "worker")
        manager.add_node_attr(8, "type", "plant")
        manager.add_node_attr(9, "type", "worker")
        manager.add_node_attr(10, "type", "plant")

        sample = gatherer.get_node_sample(manager, 4, "worker")
        self.assertEquals(4, len(sample))

        for x in sample:
            type = manager.get_node_attr(x,"type")
            self.assertEquals("worker", type)



if __name__ == "__main__":
    unittest.main()
