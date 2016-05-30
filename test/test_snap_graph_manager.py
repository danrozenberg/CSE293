from datetime import datetime
import sys
import snap
import os
from time import mktime
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

    def test_add_wage(self):
        manager = self.manager
        manager.add_node(1)
        self.assertFalse("2002_aw" in manager.get_node_attrs(1))
        self.assertFalse("2003_aw" in manager.get_node_attrs(1))


        # add wage
        manager.add_wage(1, 2002, 10.10)
        self.assertTrue("2002_aw" in manager.get_node_attrs(1))
        self.assertFalse("2003_aw" in manager.get_node_attrs(1))
        wage = manager.get_node_attrs(1)["2002_aw"]
        self.assertEqual(10.10, wage)

        # add lower wage
        manager.add_wage(1, 2002, 5.50)
        self.assertTrue("2002_aw" in manager.get_node_attrs(1))
        self.assertFalse("2003_aw" in manager.get_node_attrs(1))
        wage = manager.get_node_attrs(1)["2002_aw"]
        self.assertEqual(10.10, wage)

        # add higher wage
        manager.add_wage(1, 2002, 20.20)
        self.assertTrue("2002_aw" in manager.get_node_attrs(1))
        self.assertFalse("2003_aw" in manager.get_node_attrs(1))
        wage = manager.get_node_attrs(1)["2002_aw"]
        self.assertEqual(20.20, wage)

        # add different year
        manager.add_wage(1, 2003, 30.30)
        self.assertTrue("2002_aw" in manager.get_node_attrs(1))
        self.assertTrue("2003_aw" in manager.get_node_attrs(1))
        wage = manager.get_node_attrs(1)["2002_aw"]
        self.assertEqual(20.20, wage)
        wage = manager.get_node_attrs(1)["2003_aw"]
        self.assertEqual(30.30, wage)


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
    def test_add_and_get_edge_attribute(self):
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

    def test_quick_add_edge(self):
        manager = self.manager
        self.assertEquals(0, manager.get_edge_count())

        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)

        edge = manager.quick_add_edge(1, 2, 1)
        self.assertEquals(1, manager.get_edge_count())
        self.assertEquals(1, edge)

        edge = manager.quick_add_edge(2, 3, 2)
        self.assertEquals(2, manager.get_edge_count())
        self.assertEquals(2, edge)

        edge = manager.quick_add_edge(1, 3, 3)
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

    def test_edge_attr_dictionary(self):
        manager = self.manager
        self.assertEquals(0, manager.get_edge_count())

        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)
        manager.add_edge(1, 2, 10)
        manager.add_edge(2, 3, 20)
        manager.add_edge(1, 3, 30)
        self.assertEquals(0,len(manager.get_edge_attrs(10)))
        self.assertEquals(0,len(manager.get_edge_attrs(20)))
        self.assertEquals(0,len(manager.get_edge_attrs(30)))

        manager.add_edge_attr(10, "first", 33.33)
        manager.add_edge_attr(10, "third", "asd")

        # First edge:
        d = manager.get_edge_attrs(10)
        self.assertEquals(33.33, d["first"])
        self.assertEquals("asd", d["third"])

        #change some things, dict should reflect it.
        manager.add_edge_attr(10, "first", 44.44)
        manager.add_edge_attr(10, "third", "basd")
        d = manager.get_edge_attrs(10)
        self.assertEquals(44.44, d["first"])
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

    def test_print_info(self):
        manager = self.manager
        manager.add_node(10)
        manager.add_node(20)
        manager.add_node(30)
        manager.add_edge(10,30)
        manager.add_edge(20,30)
        file_path = "../output_stats/test_info.txt"
        manager.print_info(file_path, "my_graph")

        # file is saved
        self.assertTrue(os.path.isfile(file_path))

        # cleanup
        os.remove(file_path)

        # except if not found
        with self.assertRaises(RuntimeError) as bad_call:
            manager.load_graph("i dont exist")
        the_exception = bad_call.exception
        self.assertIn("Can not open file", the_exception.message)

    def test_load_undirected_graph(self):
        graph_path = "./naive_bayes/random.graph"
        manager = graph_manager.SnapManager()
        graph_type = snap.TUNGraph
        manager.load_graph(graph_path, graph_type)
        self.assertEquals(1000, manager.get_node_count())

    def test_save_and_load_graph_with_dictionary_preservation(self):
        manager = self.manager
        manager.add_node(10)
        manager.add_node(20)
        manager.add_node(30)
        manager.add_node(40)
        manager.add_edge(20,40)
        manager.add_edge(10,30)
        manager.add_edge(10,20, 567)

        graph_path = "./test.graph"
        nid_from_id_path = "./test_nid_from_id.p"
        id_from_nid_path = "./test_id_from_nid.p"
        edge_from_tuple_path = "./test_edge_from_tuple.p"
        manager.save_graph(graph_path)

        # files are saved
        self.assertTrue(os.path.isfile(graph_path))
        self.assertTrue(os.path.isfile(nid_from_id_path))
        self.assertTrue(os.path.isfile(id_from_nid_path))

        # file gets loaded
        manager2 = graph_manager.SnapManager()
        manager2.load_graph(graph_path)

        # internal dictionaries work fine
        self.assertTrue(manager2.is_node(10))
        self.assertTrue(manager2.is_node(20))
        self.assertTrue(manager2.is_node(30))
        self.assertTrue(manager2.is_node(40))
        self.assertEquals(1, manager2.get_edge_between(10,30))
        self.assertEquals(0, manager2.get_edge_between(20,40))
        self.assertEquals(567, manager2.get_edge_between(10,20))
        self.assertEquals(4, manager2.get_node_count())
        self.assertEquals(10, manager2.id_from_NId[0])
        self.assertEquals(20, manager2.id_from_NId[1])
        self.assertEquals(30, manager2.id_from_NId[2])


        # cleanup
        os.remove(graph_path)
        os.remove(nid_from_id_path)
        os.remove(id_from_nid_path)

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
        self.assertFalse(manager.is_edge(300))
        self.assertFalse(manager.is_edge(400))
        self.assertFalse(manager.is_edge(500))

        # we only add 1 edge per pair...
        self.assertFalse(manager.is_edge(200))

        manager.add_edge(1,3, 300)
        manager.add_edge(1,3, 400)
        manager.add_edge(2,3, 500)

        self.assertTrue(manager.is_edge(100))
        self.assertTrue(manager.is_edge(300))
        self.assertTrue(manager.is_edge(500))

        # we only add 1 edge per pair...
        self.assertFalse(manager.is_edge(400))
        self.assertFalse(manager.is_edge(200))

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

        # delete node 1, should remove all associated edges
        manager.delete_node(1)
        self.assertEquals(False, manager.is_edge(100))
        self.assertEquals(False, manager.is_edge(200))
        self.assertEquals(False, manager.is_edge(300))
        self.assertEquals(False, manager.is_edge(400))

        # other edges should remain
        self.assertEquals(True, manager.is_edge(500))

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

    def test_get_random_graph(self):
        manager = self.manager

        # should have nothing
        self.assertEquals(0, manager.get_node_count())
        self.assertEquals(0, manager.get_edge_count())

        # generate stuff
        manager.generate_random_graph(20, 3, 0)
        self.assertEquals(20, manager.get_node_count())
        self.assertEquals(60, manager.get_edge_count())

    def test_get_degree_centrality(self):
        manager = self.manager
        manager.generate_random_graph(20, 3, 0)
        self.assertGreater(manager.get_degree_centrality(0), 0)

    def test_get_eccentricity(self):
        manager = self.manager
        manager.generate_random_graph(20, 3, 0)
        self.assertGreater(manager.get_eccentricity(0), 0)

    def test_get_clustering_coefficient(self):
        manager = self.manager
        manager.generate_random_graph(20, 3, 0)
        self.assertGreater(manager.get_clustering_coefficient(1), 0)

    def test_get_eigenvector_centrallity(self):
        manager = self.manager
        manager.generate_random_graph(20, 3, 0)

        answer_hash = manager.get_eigenvector_centrality()
        self.assertGreater(answer_hash[0], 0)
        self.assertGreater(answer_hash[2], 0)
        self.assertGreater(answer_hash[10], 0)

        answer = manager.get_eigenvector_centrality(2)
        self.assertGreater(answer, 0)

    def test_get_degree_dist(self):
        manager = self.manager
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)
        manager.add_node(4)
        answer = manager.get_degree_dist()
        self.assertEqual(4, answer[0])
        self.assertEqual(0, answer[1])

        manager.add_edge(1,2)
        manager.add_edge(2,3)
        answer = manager.get_degree_dist()
        self.assertEqual(1, answer[0])
        self.assertEqual(2, answer[1])
        self.assertEqual(1, answer[2])

    def test_get_betweeness_centrality(self):
        manager = self.manager
        manager.generate_random_graph(20, 3, 0)

        answer_hash = manager.get_betweeness_centrality()
        self.assertGreater(answer_hash[0], 0)
        self.assertGreater(answer_hash[2], 0)
        self.assertGreater(answer_hash[10], 0)

    def test_get_short_path_size(self):
        manager = self.manager
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)
        manager.add_node(4)
        self.assertEquals(0, manager.get_shortest_path_size(1))

        manager.add_edge(1,2)
        manager.add_edge(3,4)
        self.assertEquals(1, manager.get_shortest_path_size(1))

        manager.add_edge(2,3)
        self.assertEquals(3, manager.get_shortest_path_size(1))

        manager.add_edge(2,4)
        self.assertEquals(2, manager.get_shortest_path_size(1))

    def test_get_connected_components(self):
        manager = self.manager
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)
        manager.add_node(4)

        self.assertEquals(4, len(manager.get_connected_components()))
        manager.add_edge(1,2)
        manager.add_edge(3,4)
        self.assertEquals(2, len(manager.get_connected_components()))

        manager.add_edge(2,3)
        self.assertEquals(1, len(manager.get_connected_components()))

    def test_get_random_node(self):
        manager = self.manager
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)
        manager.add_node(4)
        manager.add_node(55)
        manager.add_node(66)
        manager.add_node(77)
        manager.add_node(88)

        possible_range = [1,2,3,4,55,66,77,88]
        for i in range(50):
            self.assertIn(manager.get_random_node()
                          ,possible_range)

    def test_get_possible_coworkers(self):
        manager = self.manager
        self.create_affiliation_graph(manager)
        possible_coworkers = manager.get_possible_coworkers(5)
        self.assertEqual([4,6,7,9], sorted(possible_coworkers))

    def test_get_employees(self):
        manager = self.manager
        self.create_affiliation_graph(manager)
        possible_coworkers = manager.get_employees(20)
        self.assertEqual([4,5,6,9], sorted(possible_coworkers))

    def test_get_employer(self):
        manager = self.manager
        self.create_affiliation_graph(manager)
        possible_coworkers = manager.get_employees(5)
        self.assertEqual([20,30], sorted(possible_coworkers))

    def test_get_connected(self):
        manager = self.manager
        self.create_affiliation_graph(manager)
        possible_coworkers = manager.get_connected(5)
        self.assertEqual([20,30], sorted(possible_coworkers))

    def test_get_diameter(self):
       manager = self.manager
       manager.generate_random_graph(20, 3, 0)
       self.assertGreater(manager.get_diameter(), 0)

    def test_get_degree(self):
        manager = self.manager
        manager.add_node(1)
        manager.add_node(2)
        manager.add_node(3)

        self.assertEqual(0, manager.get_node_degree(1))
        self.assertEqual(0, manager.get_node_degree(2))
        self.assertEqual(0, manager.get_node_degree(3))

        manager.quick_add_edge(1,2)
        self.assertEqual(1, manager.get_node_degree(1))
        self.assertEqual(1, manager.get_node_degree(2))
        self.assertEqual(0, manager.get_node_degree(3))

        manager.quick_add_edge(2,1)
        self.assertEqual(2, manager.get_node_degree(1))
        self.assertEqual(2, manager.get_node_degree(2))
        self.assertEqual(0, manager.get_node_degree(3))

        manager.quick_add_edge(3,1)
        self.assertEqual(3, manager.get_node_degree(1))
        self.assertEqual(2, manager.get_node_degree(2))
        self.assertEqual(1, manager.get_node_degree(3))

        node_degrees = [self.manager.get_node_degree(n) for n in [1,2,3]]
        self.assertEqual(node_degrees, [3,2,1])

    def create_affiliation_graph(self, manager):
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
        manager.add_edge(1, 10, 100)
        manager.add_edge(2, 10, 200)
        manager.add_edge(3, 10, 300)
        manager.add_edge(9, 10, 400)

        manager.add_edge(4, 20, 500)
        manager.add_edge(5, 20, 600)
        manager.add_edge(6, 20, 700)
        manager.add_edge(9, 20, 800)

        manager.add_edge(4, 30, 900)
        manager.add_edge(5, 30, 1000)
        manager.add_edge(7, 30, 1100)


        # 1 worked with 3 and 2
        manager.add_edge_attr(100, "1999_admission_date",
                              mktime(datetime(1999,1,1).timetuple()))
        manager.add_edge_attr(100, "1999_demission_date",
                              mktime(datetime(1999,12,31).timetuple()))
        manager.add_edge_attr(100, "2000_admission_date",
                              mktime(datetime(2000,1,1).timetuple()))
        manager.add_edge_attr(100, "2000_demission_date",
                              mktime(datetime(2000,12,31).timetuple()))
        manager.add_edge_attr(100, "2001_admission_date",
                              mktime(datetime(2001,1,1).timetuple()))
        manager.add_edge_attr(100, "2001_demission_date",
                              mktime(datetime(2001,12,31).timetuple()))

        manager.add_edge_attr(200, "1999_admission_date",
                              mktime(datetime(1999,5,5).timetuple()))
        manager.add_edge_attr(200, "1999_demission_date",
                              mktime(datetime(1999,12,31).timetuple()))

        manager.add_edge_attr(300, "2001_admission_date",
                              mktime(datetime(2001,6,6).timetuple()))
        manager.add_edge_attr(300, "2001_demission_date",
                              mktime(datetime(2001,12,31).timetuple()))

        # 2 worked with 9
        manager.add_edge_attr(200, "2002_admission_date",
                              mktime(datetime(2002,5,5).timetuple()))
        manager.add_edge_attr(200, "2002_demission_date",
                              mktime(datetime(2002,12,31).timetuple()))
        manager.add_edge_attr(400, "2002_admission_date",
                              mktime(datetime(2002,1,1).timetuple()))
        manager.add_edge_attr(400, "2002_demission_date",
                              mktime(datetime(2002,6,1).timetuple()))

        # 9 worked with 4
        manager.add_edge_attr(800, "2003_admission_date",
                              mktime(datetime(2003,1,1).timetuple()))
        manager.add_edge_attr(800, "2003_demission_date",
                              mktime(datetime(2003,6,1).timetuple()))
        manager.add_edge_attr(800, "2004_admission_date",
                              mktime(datetime(2004,1,1).timetuple()))
        manager.add_edge_attr(800, "2004_demission_date",
                              mktime(datetime(2004,6,1).timetuple()))

        manager.add_edge_attr(500, "2004_admission_date",
                              mktime(datetime(2004,1,1).timetuple()))
        manager.add_edge_attr(500, "2004_demission_date",
                              mktime(datetime(2004,10,31).timetuple()))


        # 4 worked with 5
        manager.add_edge_attr(600, "2004_admission_date",
                              mktime(datetime(2004,7,1).timetuple()))
        manager.add_edge_attr(600, "2004_demission_date",
                              mktime(datetime(2004,12,31).timetuple()))

        # 5 worked with 6
        manager.add_edge_attr(700, "2004_admission_date",
                              mktime(datetime(2004,11,1).timetuple()))
        manager.add_edge_attr(700, "2004_demission_date",
                              mktime(datetime(2004,12,25).timetuple()))

        # 5 worked with 7
        manager.add_edge_attr(1000, "2005_admission_date",
                              mktime(datetime(2005,1,1).timetuple()))
        manager.add_edge_attr(1000, "2005_demission_date",
                              mktime(datetime(2005,7,1).timetuple()))

        manager.add_edge_attr(1100, "2005_admission_date",
                              mktime(datetime(2005,1,1).timetuple()))
        manager.add_edge_attr(1100, "2005_demission_date",
                              mktime(datetime(2005,6,1).timetuple()))

        # try to trick the algorithm
        # 4 will work with 5 again, but in a different company
        manager.add_edge_attr(900, "2005_admission_date",
                              mktime(datetime(2005,7,1).timetuple()))
        manager.add_edge_attr(900, "2005_demission_date",
                              mktime(datetime(2005,7,1).timetuple()))

if __name__ == "__main__":
    unittest.main()
