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
        d = manager.get_node_attrs(1)
        self.assertEquals(13.5, d["one_attr"])
        self.assertEquals("asd", d["a_third_one"])
        self.assertEquals("asd", manager.get_node_attr(1, 'a_third_one'))
        self.assertEquals(13.5, manager.get_node_attr(1, 'one_attr'))

        # Second node:
        d = manager.get_node_attrs(2)
        self.assertEquals(5, d["another_attr"])
        self.assertEquals(5, manager.get_node_attr(2, 'another_attr'))

        # third, empty node
        d = manager.get_node_attrs(3)
        self.assertEquals(len(d.keys()), 0)

        # Error
        with self.assertRaises(RuntimeError) as bad_call:
            manager.get_node_attr(3, "i dont exist")
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
        d = manager.get_edge_attrs(1)
        self.assertEquals(33.33, d["first"])
        self.assertEquals("asd", d["third"])
        self.assertEquals(33.33, manager.get_edge_attr(1, 'first'))
        self.assertEquals("asd", manager.get_edge_attr(1, 'third'))

        # Second node:
        d = manager.get_edge_attrs(2)
        self.assertEquals(100, d["second"])
        self.assertEquals(100, manager.get_edge_attr(2, 'second'))

        # third, empty node
        d = manager.get_edge_attrs(3)
        self.assertEquals(len(d.keys()), 0)
        with self.assertRaises(RuntimeError) as bad_call:
            manager.get_edge_attr(3, "i dont exist")
        the_exception = bad_call.exception
        self.assertIn("does not have attribute", the_exception.message)

        # what if we add the same attribute again?
        manager.add_edge_attr(1, "third", "basd")
        d = manager.get_edge_attrs(1)
        self.assertEquals("basd", d["third"])

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

    def test_node_generator(self):

        manager = self.manager

        # no nodes, should have empty iterator
        it = manager.get_node_iterator()
        for node in it:
            self.fail("iteraor should be empty")

        # add some nodes
        manager.add_node(10)
        manager.add_node(20)
        manager.add_node(30)
        found_nodes = []
        it = manager.get_node_iterator()
        for node in it:
            found_nodes.append(node)
        self.assertListEqual([10,20,30], found_nodes)

        # try again with more nodes
        manager.add_node(40)
        manager.add_node(50)
        manager.add_node(30) #should ignore this
        found_nodes = []
        it = manager.get_node_iterator()
        for node in it:
            found_nodes.append(node)
        self.assertListEqual([10,20,30,40,50], found_nodes)

    def test_is_edge(self):

        manager = self.manager

        # add some nodes
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)

        manager.add_edge(1,2, 100)
        manager.add_edge(1,2, 200)

        self.assertTrue(manager.is_edge(100))
        self.assertTrue(manager.is_edge(200))
        self.assertFalse(manager.is_edge(300))
        self.assertFalse(manager.is_edge(400))
        self.assertFalse(manager.is_edge(500))

        manager.add_edge(1,3, 300)
        manager.add_edge(1,3, 400)
        manager.add_edge(2,3, 500)

        self.assertTrue(manager.is_edge(100))
        self.assertTrue(manager.is_edge(200))
        self.assertTrue(manager.is_edge(300))
        self.assertTrue(manager.is_edge(400))
        self.assertTrue(manager.is_edge(500))

    def test_get_edges(self):
        manager = self.manager
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)
        manager.add_node(4)
        manager.add_node(5)

        # should be empty
        edge_ids = manager.get_edges(1)
        self.assertEquals([], edge_ids)

        # add some edges
        manager.add_edge(1, 2, 100)
        manager.add_edge(1, 2, 150) # is not considered in this implementation
        manager.add_edge(1, 3, 200)
        manager.add_edge(2, 3, 300)
        manager.add_edge(4, 1, 400)
        self.assertEquals([100, 200, 400], sorted(manager.get_edges(1)))
        self.assertEquals([100, 300], manager.get_edges(2))
        self.assertEquals([], manager.get_edges(5))

        manager.add_edge(2, 5, 500)
        self.assertEquals([100, 300, 500], sorted(manager.get_edges(2)))
        self.assertEquals([500], manager.get_edges(5))

    def test_get_edge_between(self):

        manager = self.manager

        # add some nodes
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)
        manager.add_node(4)

        manager.add_edge(1,2, 100)
        manager.add_edge(1,3, 200)

        self.assertEquals(100, manager.get_edge_between(1,2))
        self.assertEquals(200, manager.get_edge_between(1,3))
        self.assertEquals(100, manager.get_edge_between(2,1))
        self.assertEquals(200, manager.get_edge_between(3,1))
        self.assertIsNone(manager.get_edge_between(1,4))
        self.assertIsNone(manager.get_edge_between(2,4))

        manager.add_edge(2,4, 300)
        manager.add_edge(1,4, 400)
        self.assertEquals(300, manager.get_edge_between(2,4))
        self.assertEquals(400, manager.get_edge_between(1,4))

    def test_is_edge_between(self):
        manager = graph_manager.SnapManager()

        # add a few nodes
        manager.add_node(10)
        manager.add_node(20)
        manager.add_node(30)
        manager.add_node(40)
        manager.add_node(50)
        manager.add_node(60)

        manager.add_edge(40, 50)
        manager.add_edge(10, 30)
        manager.add_edge(20, 10)
        manager.add_edge(20, 30)
        manager.add_edge(30, 60)
        manager.add_edge(10, 20)

        # some edges that do exist
        self.assertTrue(manager.is_edge_between(10, 20))
        self.assertTrue(manager.is_edge_between(10, 30))
        self.assertTrue(manager.is_edge_between(20, 30))
        self.assertTrue(manager.is_edge_between(20, 10))
        self.assertTrue(manager.is_edge_between(30, 60))
        self.assertTrue(manager.is_edge_between(40, 50))

        # invert order to make sure
        self.assertTrue(manager.is_edge_between(20, 10))
        self.assertTrue(manager.is_edge_between(30, 10))
        self.assertTrue(manager.is_edge_between(30, 20))
        self.assertTrue(manager.is_edge_between(10, 20))
        self.assertTrue(manager.is_edge_between(60, 30))
        self.assertTrue(manager.is_edge_between(50, 40))

        # some edges that do not exist
        self.assertFalse(manager.is_edge_between(40, 10))
        self.assertFalse(manager.is_edge_between(10, 60))
        self.assertFalse(manager.is_edge_between(10, 10))
        self.assertFalse(manager.is_edge_between(60, 60))
        self.assertFalse(manager.is_edge_between(60, 20))
        self.assertFalse(manager.is_edge_between(30, 40))

    def test_get_neighboring_nodes(self):
        manager = graph_manager.SnapManager()

        # add a few nodes
        manager.add_node(10)
        manager.add_node(20)
        manager.add_node(30)
        manager.add_node(40)
        manager.add_node(50)
        manager.add_node(60)

        # make some worker nodes, others as employer nodes
        manager.add_node_attr(10, "type", "worker")
        manager.add_node_attr(20, "type", "worker")
        manager.add_node_attr(30, "type", "employer")
        manager.add_node_attr(40, "type", "employer")
        manager.add_node_attr(50, "type", "employer")
        manager.add_node_attr(60, "type", "employer")

        manager.add_edge(20, 30, 400)
        manager.add_edge(10, 60, 300)
        manager.add_edge(20, 40, 500)
        manager.add_edge(20, 50, 600)
        manager.add_edge(10, 40, 100)

        # we should get unique nodes only, let's
        # try to trick the class by doing this:
        manager.add_edge(10, 30)
        manager.add_edge(30, 10)
        manager.add_edge(10, 30)
        manager.add_edge(10, 30)

        # check for node 10
        self.assertListEqual(sorted([30,40,60]),
                             sorted(manager.get_neighboring_nodes(10)))

        # check for node 20
        self.assertListEqual(sorted([30,40,50]),
                      sorted(manager.get_neighboring_nodes(20)))

        # check for node 30
        self.assertListEqual(sorted([10,20]),
                      sorted(manager.get_neighboring_nodes(30)))

    def test_deleting_nodes_also_deletes_edges(self):

        manager = self.manager

        # add some nodes
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)

        manager.add_edge(1,2, 100)
        manager.add_edge(1,2, 200)
        manager.add_edge(1,3, 300)
        manager.add_edge(1,3, 400)
        manager.add_edge(2,3, 500)
        manager.add_edge(2,3, 600)

        # delete node 1, should remove all associated edges
        manager.delete_node(1)
        self.assertEquals(False, manager.is_edge(100))
        self.assertEquals(False, manager.is_edge(200))
        self.assertEquals(False, manager.is_edge(300))
        self.assertEquals(False, manager.is_edge(400))

        # other edges should remain
        self.assertEquals(True, manager.is_edge(500))
        self.assertEquals(True, manager.is_edge(600))

    def test_copy_node(self):

        src_graph = graph_manager.SnapManager()
        dst_graph = graph_manager.SnapManager()

        # can't copy what doesn't exist
        self.assertFalse(src_graph.copy_node(10, dst_graph))
        self.assertEquals(0, src_graph.get_node_count())
        self.assertEquals(0, dst_graph.get_node_count())

        src_graph.add_node(2)
        src_graph.add_node_attr(2, "firstAttr", "Cool!")
        src_graph.add_node_attr(2, "secondAttr", "Awesome!")

        src_graph.add_node(1)
        src_graph.add_node_attr(1, "firstAttr", "Lame")
        src_graph.add_node_attr(1, "secondAttr", "Bummer")

        # copy node 2
        src_graph.copy_node(2, dst_graph)
        self.assertEquals(2, src_graph.get_node_count())
        self.assertEquals(1, dst_graph.get_node_count())

        original_attrs = src_graph.get_node_attrs(2)
        copied_attrs = dst_graph.get_node_attrs(2)
        self.assertEquals(original_attrs, copied_attrs)

        # can't copy the same node id
        self.assertFalse(src_graph.copy_node(2, dst_graph))

        # also, changes in one doesn't reflect changes in another
        src_graph.add_node_attr(2, "ThirdAttr", "Tubular!")
        original_attrs = src_graph.get_node_attrs(2)
        copied_attrs = dst_graph.get_node_attrs(2)
        self.assertEquals(3, len(original_attrs))
        self.assertEquals(2, len(copied_attrs))

if __name__ == "__main__":
    unittest.main()
