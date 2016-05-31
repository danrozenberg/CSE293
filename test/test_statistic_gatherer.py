import os
import unittest
import sys
sys.path.insert(0, '../src/')
import statistics_gatherer
from data_parser import ClassificationLoader
import graph_manager
import graph_manager

class TestSnapStatisticsGatherer(unittest.TestCase):

    def test_filter_nodes_with_wage_in_year(self):
        manager = graph_manager.SnapManager()
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)
        manager.add_node(4)
        manager.add_node(5)
        manager.add_node(5)


        # add wage
        manager.add_wage(1, 2002, 10.10,1)
        manager.add_wage(1, 2003, 12.20,2)
        manager.add_wage(2, 1999, 1.1,3)
        manager.add_wage(2, 1993, 3.3,4)
        manager.add_wage(4, 2002, 43.3,5)

        candidates = statistics_gatherer.filter_nodes_with_wage_in_year(manager,
                                                                        1986)
        self.assertEqual([],candidates)

        candidates = statistics_gatherer.filter_nodes_with_wage_in_year(manager,
                                                                        2002)
        self.assertEqual([1,4], sorted(candidates))

        candidates = statistics_gatherer.filter_nodes_with_wage_in_year(manager,
                                                                        1999)
        self.assertEqual([2], sorted(candidates))



if __name__ == "__main__":
    unittest.main()
