import unittest
import sys
import logging
from datetime import datetime
sys.path.insert(0, '../src/')
import graph_manager
import connect_workers


class ConnectWorkersTest(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s',
        datefmt='%d %b - %H:%M:%S -',
        level=logging.INFO)

    def test_get_worker_iterator(self):
        manager = graph_manager.SnapManager()

        # add a few nodes
        manager.add_node(10)
        manager.add_node(20)
        manager.add_node(30)
        manager.add_node(40)
        manager.add_node(50)
        manager.add_node(60)
        manager.add_node(70)

        # make some worker nodes, others as employer nodes
        manager.add_node_attr(10, "type", "worker")
        manager.add_node_attr(20, "type", "employer")
        manager.add_node_attr(30, "type", "worker")
        manager.add_node_attr(40, "type", "employer")
        manager.add_node_attr(50, "type", "worker")
        manager.add_node_attr(60, "type", "employer")
        manager.add_node_attr(70, "type", "worker")

        # get iterator
        it = connect_workers.get_worker_iterator(manager)

        # test it
        found_ids = []
        for node in it:
            self.assertEquals("worker",
                              manager.get_node_attribute(node, "type"))
            found_ids.append(node)

        self.assertListEqual([10,30,50,70], found_ids)

    def test_connect_workers(self):
        manager = graph_manager.SnapManager()
        self.create_association_graph(manager)

        new_graph = graph_manager.SnapManager()
        connect_workers.connect_workers(manager,new_graph)

        # Graph should have 9 nodes only
        self.assertEqual(9, new_graph.get_node_count())

        # All edges that should exist
        self.assertTrue(new_graph.is_edge_between(1,2))
        self.assertTrue(new_graph.is_edge_between(1,3))
        self.assertTrue(new_graph.is_edge_between(2,3))
        self.assertTrue(new_graph.is_edge_between(9,1))
        self.assertTrue(new_graph.is_edge_between(9,2))
        self.assertTrue(new_graph.is_edge_between(9,3))
        self.assertTrue(new_graph.is_edge_between(9,4))
        self.assertTrue(new_graph.is_edge_between(9,5))
        self.assertTrue(new_graph.is_edge_between(4,7))
        self.assertTrue(new_graph.is_edge_between(4,6))
        self.assertTrue(new_graph.is_edge_between(4,5))
        self.assertTrue(new_graph.is_edge_between(5,6))
        self.assertTrue(new_graph.is_edge_between(5,7))
        self.assertTrue(new_graph.is_edge_between(6,9))

        # some edges that should not exist
        self.assertFalse(new_graph.is_edge_between(3,7))
        self.assertFalse(new_graph.is_edge_between(7,7))
        self.assertFalse(new_graph.is_edge_between(9,8))
        self.assertFalse(new_graph.is_edge_between(9,7))
        self.assertFalse(new_graph.is_edge_between(1,7))
        self.assertFalse(new_graph.is_edge_between(1,5))

    def test_get_overlapping_time(self):
        start_1 = datetime(2010,1,1)
        end_1 = datetime(2015,1,1)
        start_2 = datetime(2011,10,10)
        end_2 = datetime(2011,10,20)
        self.assertEquals(11, connect_workers.get_overlapping_time(start_1,
                                                                   end_1,
                                                                   start_2,
                                                                   end_2))

        start_2 = datetime(2010,1,1)
        end_2 = datetime(2015,1,1)
        start_1 = datetime(2011,10,10)
        end_1 = datetime(2011,10,20)
        self.assertEquals(11, connect_workers.get_overlapping_time(start_1,
                                                               end_1,
                                                               start_2,
                                                               end_2))

        start_2 = datetime(2010,1,1)
        end_2 = datetime(2010,1,1)
        start_1 = datetime(2010,1,1)
        end_1 = datetime(2010,1,1)
        self.assertEquals(1, connect_workers.get_overlapping_time(start_1,
                                                               end_1,
                                                               start_2,
                                                               end_2))

        start_2 = datetime(2010,1,1)
        end_2 = datetime(2010,1,2)
        start_1 = datetime(2010,1,2)
        end_1 = datetime(2010,1,2)
        self.assertEquals(1,connect_workers.get_overlapping_time(start_1,
                                                               end_1,
                                                               start_2,
                                                               end_2))

        start_1 = datetime(2010,1,1)
        end_1 = datetime(2015,1,1)
        start_2 = datetime(2066,10,10)
        end_2 = datetime(2077,10,20)
        self.assertEquals(0, connect_workers.get_overlapping_time(start_1,
                                                               end_1,
                                                               start_2,
                                                               end_2))

        start_1 = datetime(2010,1,1)
        end_1 = datetime(2015,1,1)
        start_2 = datetime(2011,10,10)
        end_2 = datetime(2012,10,20)
        self.assertEquals(377, connect_workers.get_overlapping_time(start_1,
                                                               end_1,
                                                               start_2,
                                                               end_2))


    def create_association_graph(self, manager):
        # 9 workers
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)
        manager.add_node(4)
        manager.add_node(5)
        manager.add_node(6)
        manager.add_node(7)
        manager.add_node(8)
        manager.add_node(9)
        manager.add_node_attr(1, "type", "worker")
        manager.add_node_attr(2, "type", "worker")
        manager.add_node_attr(3, "type", "worker")
        manager.add_node_attr(4, "type", "worker")
        manager.add_node_attr(5, "type", "worker")
        manager.add_node_attr(6, "type", "worker")
        manager.add_node_attr(7, "type", "worker")
        manager.add_node_attr(8, "type", "worker")
        manager.add_node_attr(9, "type", "worker")

        # 3 plants
        manager.add_node(10)
        manager.add_node(20)
        manager.add_node(30)
        manager.add_node_attr(10, "type", "employer")
        manager.add_node_attr(20, "type", "employer")
        manager.add_node_attr(30, "type", "employer")

        # connect them
        manager.add_edge(1, 10)
        manager.add_edge(2, 10)
        manager.add_edge(3, 10)
        manager.add_edge(9, 10)

        manager.add_edge(4, 20)
        manager.add_edge(5, 20)
        manager.add_edge(6, 20)
        manager.add_edge(9, 20)

        manager.add_edge(4, 30)
        manager.add_edge(5, 30)
        manager.add_edge(7, 30)

class SimpleConnectorTest(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(format='%(asctime)s %(message)s',
        datefmt='%d %b - %H:%M:%S -',
        level=logging.INFO)

    def test_get_time_at_worker_attrs(self):
        connector = connect_workers.SimpleConnector()

        manager = graph_manager.SnapManager()
        self.create_association_graph(manager)

    def create_association_graph(self, manager):
        # 9 workers
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)
        manager.add_node(4)
        manager.add_node(5)
        manager.add_node(6)
        manager.add_node(7)
        manager.add_node(8)
        manager.add_node(9)
        manager.add_node_attr(1, "type", "worker")
        manager.add_node_attr(2, "type", "worker")
        manager.add_node_attr(3, "type", "worker")
        manager.add_node_attr(4, "type", "worker")
        manager.add_node_attr(5, "type", "worker")
        manager.add_node_attr(6, "type", "worker")
        manager.add_node_attr(7, "type", "worker")
        manager.add_node_attr(8, "type", "worker")
        manager.add_node_attr(9, "type", "worker")

        # 3 plants
        manager.add_node(10)
        manager.add_node(20)
        manager.add_node(30)
        manager.add_node_attr(10, "type", "employer")
        manager.add_node_attr(20, "type", "employer")
        manager.add_node_attr(30, "type", "employer")

        # connect them
        manager.add_edge(1, 10)
        manager.add_edge(2, 10)
        manager.add_edge(3, 10)
        manager.add_edge(9, 10)

        manager.add_edge(4, 20)
        manager.add_edge(5, 20)
        manager.add_edge(6, 20)
        manager.add_edge(9, 20)

        manager.add_edge(4, 30)
        manager.add_edge(5, 30)
        manager.add_edge(7, 30)


if __name__ == "__main__":
    unittest.main()
