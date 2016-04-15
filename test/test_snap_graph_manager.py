import sys
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
        self.assertEquals(0, self.manager.node_count())

        # got nodes
        NId = manager.add_node(-55)
        self.assertEquals(1, self.manager.node_count())
        self.assertEquals(-55, NId)

        NId = manager.add_node()
        self.assertEquals(2, self.manager.node_count())
        self.assertEquals(0, NId)

        NId = manager.add_node()
        self.assertEquals(3, self.manager.node_count())
        self.assertEquals(1, NId)

        NId = manager.add_node(33)
        self.assertEquals(4, self.manager.node_count())
        self.assertEquals(33, NId)

        # don't freak out with adding same node
        NId = manager.add_node(33)
        self.assertEquals(4, self.manager.node_count())
        self.assertEquals(33, NId)

    # should delete nodes
    def test_delete_nod(self):
        manager = self.manager

        manager.add_node(11)
        self.assertEquals(1, self.manager.node_count())

        manager.delete_node(11)
        self.assertEquals(0, self.manager.node_count())

        # don't freak out!
        manager.delete_node(11)
        self.assertEquals(0, self.manager.node_count())

    # should add and retrieve attribute to/from a node
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

        # Second node:
        d = manager.get_node_attributes(2)
        self.assertEquals(5, d["another_attr"])

        # third, empty node
        d = manager.get_node_attributes(3)
        self.assertEquals(len(d.keys()), 0)

    def test_is_node(self):
        manager = self.manager
        self.assertFalse(manager.is_node(10))
        self.assertFalse(manager.is_node(2))
        manager.add_node(10)
        manager.add_node(2)
        self.assertTrue(manager.is_node(10))
        self.assertTrue(manager.is_node(2))

    # should add and retrieve attribute to/from an edge
    def test_add_edge_attribute(self):
        manager = self.manager

        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)

        manager.add_edge(1, 2, 1)
        manager.add_edge(2, 3, 2)
        manager.add_edge(1, 3, 3)
        manager.add_edge_att(1, "first", 33.33)
        manager.add_edge_att(2, "second", 100)
        manager.add_edge_att(1, "third", "asd")

        # First edge:
        d = manager.get_edge_attributes(1)
        self.assertEquals(33.33, d["first"])
        self.assertEquals("asd", d["third"])

        # Second node:
        d = manager.get_edge_attributes(2)
        self.assertEquals(100, d["second"])

        # third, empty node
        d = manager.get_edge_attributes(3)
        self.assertEquals(len(d.keys()), 0)

    def test_get_nodes(self):
        manager = self.manager

        expected = []
        answer = manager.get_nodes()
        self.assertAlmostEqual(expected, answer)

        manager.add_node(10)
        expected = [10]
        answer = manager.get_nodes()
        self.assertAlmostEqual(expected, answer)

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

if __name__ == "__main__":
    unittest.main()
