import sys
import os
import unittest
sys.path.append('../src')
import graph_manager


class TestSnapManager(unittest.TestCase):

    def setUp(self):
        self.manager = graph_manager.SnapManager()

    # should initialize allright
    def test_init(self):
        manager = graph_manager.SnapManager()
        self.assertIsNotNone(manager.network)

    # should add node
    def test_add_node(self):
        manager = self.manager

        # no nodes
        self.assertEquals(0, self.manager.get_node_count())

        # got nodes
        node_id = manager.add_node(-55)
        self.assertEquals(1, self.manager.get_node_count())
        self.assertEquals(-55, node_id)

        node_id = manager.add_node(0)
        self.assertEquals(2, self.manager.get_node_count())
        self.assertEquals(0, node_id)

        node_id = manager.add_node(1)
        self.assertEquals(3, self.manager.get_node_count())
        self.assertEquals(1, node_id)

        node_id = manager.add_node(33)
        self.assertEquals(4, self.manager.get_node_count())
        self.assertEquals(33, node_id)

        # don't freak out with adding same node
        node_id = manager.add_node(33)
        self.assertEquals(4, self.manager.get_node_count())
        self.assertEquals(33, node_id)

        # need to be able to add big values
        node_id = manager.add_node(999999999999)
        self.assertEquals(5, self.manager.get_node_count())
        self.assertEquals(999999999999, node_id)

    # should delete nodes
    def test_delete_node(self):
        manager = self.manager

        manager.add_node(11)
        self.assertEquals(1, self.manager.get_node_count())
        remaining_nodes = manager.get_nodes()
        self.assertListEqual([11], remaining_nodes)

        manager.add_node(22)
        self.assertEquals(2, self.manager.get_node_count())
        remaining_nodes = manager.get_nodes()
        self.assertListEqual([11, 22], remaining_nodes)

        manager.add_node(33)
        self.assertEquals(3, self.manager.get_node_count())
        remaining_nodes = manager.get_nodes()
        self.assertListEqual([11, 22, 33], remaining_nodes)

        manager.delete_node(11)
        self.assertEquals(2, self.manager.get_node_count())
        remaining_nodes = manager.get_nodes()
        self.assertListEqual([22, 33], remaining_nodes)

        manager.delete_node(22)
        self.assertEquals(1, self.manager.get_node_count())
        remaining_nodes = manager.get_nodes()
        self.assertListEqual([33], remaining_nodes)

        # don't freak out!
        manager.delete_node(11)
        self.assertEquals(1, self.manager.get_node_count())
        remaining_nodes = manager.get_nodes()
        self.assertListEqual([33], remaining_nodes)

    # should add and retrieve attribute to/from a node
    # also tests get node attrs/attr
    def test_add_node_attr(self):
        manager = self.manager

        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)
        manager.add_node_attr(1, "one_attr", 13.5)
        manager.add_node_attr(2, "another_attr", 5)
        manager.add_node_attr(1, "a_third_one", "asd")

        # First node:
        d = manager.get_node_attributes(1)
        self.assertEquals(13.5, d["one_attr"])
        self.assertEquals("asd", d["a_third_one"])
        self.assertEquals("asd", manager.get_node_attribute(1, 'a_third_one'))
        self.assertEquals(13.5, manager.get_node_attribute(1, 'one_attr'))

        # Second node:
        d = manager.get_node_attributes(2)
        self.assertEquals(5, d["another_attr"])
        self.assertEquals(5, manager.get_node_attribute(2, 'another_attr'))

        # third, empty node
        d = manager.get_node_attributes(3)
        self.assertEquals(len(d.keys()), 0)

        # Error
        with self.assertRaises(RuntimeError) as bad_call:
            manager.get_node_attribute(3, "i dont exist")
        the_exception = bad_call.exception
        self.assertIn("does not have attribute", the_exception.message)


    def test_is_node(self):
        manager = self.manager
        self.assertFalse(manager.is_node(10))
        self.assertFalse(manager.is_node(2))
        node1 = manager.add_node(10)
        node2 = manager.add_node(2)
        self.assertTrue(manager.is_node(node1))
        self.assertTrue(manager.is_node(node2))
        manager.add_node(10)
        manager.add_node(2)
        self.assertTrue(manager.is_node(10))
        self.assertTrue(manager.is_node(2))

    # should add and retrieve attribute to/from an edge
    # also tests get edge attrs/attr
    def test_add_edge_attribute(self):
        manager = self.manager
        self.assertEquals(0, manager.get_edge_count())

        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)

        edge = manager.add_edge(1, 2, 1)
        self.assertEquals(1, manager.get_edge_count())
        self.assertEquals(1, edge)

        edge = manager.add_edge(2, 3, 2)
        self.assertEquals(2, manager.get_edge_count())
        self.assertEquals(2, edge)

        edge = manager.add_edge(1, 3, 3)
        self.assertEquals(3, manager.get_edge_count())
        self.assertEquals(3, edge)

        manager.add_edge_attr(1, "first", 33.33)
        manager.add_edge_attr(2, "second", 100)
        manager.add_edge_attr(1, "third", "asd")

        # First edge:
        d = manager.get_edge_attributes(1)
        self.assertEquals(33.33, d["first"])
        self.assertEquals("asd", d["third"])
        self.assertEquals(33.33, manager.get_edge_attribute(1, 'first'))
        self.assertEquals("asd", manager.get_edge_attribute(1, 'third'))

        # Second node:
        d = manager.get_edge_attributes(2)
        self.assertEquals(100, d["second"])
        self.assertEquals(100, manager.get_edge_attribute(2, 'second'))

        # third, empty node
        d = manager.get_edge_attributes(3)
        self.assertEquals(len(d.keys()), 0)
        with self.assertRaises(RuntimeError) as bad_call:
            manager.get_edge_attribute(3, "i dont exist")
        the_exception = bad_call.exception
        self.assertIn("does not have attribute", the_exception.message)

    def test_get_nodes(self):
        manager = self.manager

        expected = []
        answer = manager.get_nodes()
        self.assertListEqual(expected, answer)

        manager.add_node(10)
        expected = [10]
        answer = manager.get_nodes()
        self.assertListEqual(expected, answer)

        manager.add_node(20)
        expected = [10, 20]
        answer = manager.get_nodes()
        self.assertListEqual(expected, answer)

        manager.add_node(30)
        expected = [10, 20, 30]
        answer = manager.get_nodes()
        self.assertListEqual(expected, answer)

        manager.delete_node(20)
        expected = [10, 30]
        answer = manager.get_nodes()
        self.assertListEqual(expected, answer)

        manager.delete_node(10)
        expected = [30]
        answer = manager.get_nodes()
        self.assertListEqual(expected, answer)

        manager.delete_node(30)
        expected = []
        answer = manager.get_nodes()
        self.assertListEqual(expected, answer)

    def test_save_and_load_graph(self):
        manager = self.manager
        manager.add_node(10)
        manager.add_node(20)
        manager.add_node(30)
        file_path = "./test.graph"
        manager.save_graph(file_path)

        # file is saved
        self.assertTrue(os.path.isfile(file_path))

        # file gets loaded
        manager = graph_manager.SnapManager()
        manager.load_graph(file_path)
        self.assertEquals(3, manager.get_node_count())

        # cleanup
        os.remove(file_path)

        # except if not found
        with self.assertRaises(RuntimeError) as bad_call:
            manager.load_graph("i dont exist")
        the_exception = bad_call.exception
        self.assertIn("Can not open file", the_exception.message)

    def test_get_edges_between(self):
        manager = self.manager
        manager.add_node(10)
        manager.add_node(20)
        manager.add_node(30)

        # no edges yet.
        self.assertEquals(0, len(manager.get_edges_between(10, 20)))
        self.assertEquals(0, len(manager.get_edges_between(10, 30)))
        self.assertEquals(0, len(manager.get_edges_between(20, 30)))

        # add 3 edges between nodes 10 and 20
        manager.add_edge(10, 20, 1)
        manager.add_edge(10, 20, 2)
        manager.add_edge(10, 20, 3)
        self.assertEquals(3, len(manager.get_edges_between(10, 20)))
        self.assertEquals(0, len(manager.get_edges_between(10, 30)))
        self.assertEquals(0, len(manager.get_edges_between(20, 30)))
        self.assertListEqual([1,2,3], manager.get_edges_between(10, 20))

        # Hmm, add some edges between 10 and 30
        manager.add_edge(10, 30, 4)
        manager.add_edge(10, 30, 5)
        self.assertEquals(3, len(manager.get_edges_between(10, 20)))
        self.assertEquals(2, len(manager.get_edges_between(10, 30)))
        self.assertEquals(0, len(manager.get_edges_between(20, 30)))
        self.assertListEqual([1,2,3], manager.get_edges_between(10, 20))
        self.assertListEqual([4,5], manager.get_edges_between(10, 30))

        # Hmm, add more edges to first pair
        manager.add_edge(10, 20, 6)
        manager.add_edge(10, 20, 7)
        self.assertEquals(5, len(manager.get_edges_between(10, 20)))
        self.assertEquals(2, len(manager.get_edges_between(10, 30)))
        self.assertEquals(0, len(manager.get_edges_between(20, 30)))
        self.assertListEqual([1,2,3,6,7], manager.get_edges_between(10, 20))
        self.assertListEqual([4,5], manager.get_edges_between(10, 30))

if __name__ == "__main__":
    unittest.main()
