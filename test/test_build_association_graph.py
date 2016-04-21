import unittest
import sys, os
import mock
import logging
sys.path.insert(0, '../src/')
from build_association_graph import *
import data_parser
import graph_manager


class TestBuildAssociationGraph(unittest.TestCase):
    # TODO: if there is enough time, decouple from other classes.

    def setUp(self):
        logging.disable(logging.CRITICAL)

    @mock.patch('build_association_graph.process_file')
    def test_process_files(self, process_file_mock):

        save_path = "./test.graph"

        # should process 3 files, don't save
        src_folder = "./test_file_path_folder/"
        parser = data_parser.Pis12DataParser()
        interpreter = data_parser.Pis12DataInterpreter()
        manager = graph_manager.SnapManager
        process_files(src_folder, parser, interpreter, manager)
        self.assertEquals(3, process_file_mock.call_count)
        self.assertFalse(os.path.isfile(save_path))

        # should process 3 files and save
        src_folder = "./test_file_path_folder/"
        parser = data_parser.Pis12DataParser()
        interpreter = data_parser.Pis12DataInterpreter()
        manager = graph_manager.SnapManager
        process_files(src_folder, parser, interpreter, manager, save_path)

        # we add 3 more calls to process_file...
        self.assertEquals(6, process_file_mock.call_count)
        self.assertTrue(os.path.isfile(save_path))

        # cleanup
        os.remove(save_path)

    def test_process_file(self):

        # should call process_line 30 times
        file_path = './test_data/raw_graph.csv'
        parser = data_parser.Pis12DataParser()
        interpreter = data_parser.Pis12DataInterpreter()
        graph = graph_manager.SnapManager()
        process_file(file_path, parser, interpreter, graph)

        # from association_graph, we should see 5 worker nodes
        # we should also see 3 employer nodes.
        self.assertEquals(8, graph.get_node_count())

    @mock.patch('build_association_graph.create_nodes')
    @mock.patch('build_association_graph.create_edges')
    @mock.patch('build_association_graph.passes_filter', return_value=True)
    def test_process_line_if_filter_is_ok(self, mock_filter,
                           mock_edges, mock_nodes):

        process_line(FakeInterpreter(), graph_manager.SnapManager())
        self.assertEquals(1, mock_nodes.call_count)
        self.assertEquals(1, mock_filter.call_count)
        self.assertEquals(1, mock_edges.call_count)

    @mock.patch('build_association_graph.create_nodes')
    @mock.patch('build_association_graph.create_edges')
    @mock.patch('build_association_graph.passes_filter', return_value=False)
    def test_process_line_if_filter_is_not_ok(self, mock_filter,
                           mock_edges, mock_nodes):

        process_line(FakeInterpreter(), graph_manager.SnapManager())
        self.assertEquals(1, mock_filter.call_count)
        self.assertEquals(0, mock_edges.call_count)
        self.assertEquals(0, mock_nodes.call_count)


    def test_create_nodes(self):

        interpreter = FakeInterpreter()

        # with an empty graph, should add both nodes
        graph = graph_manager.SnapManager()
        interpreter.worker_id = 19
        interpreter.employer_id = 888
        expected_nodes = [19,888]
        create_nodes(interpreter, graph)
        self.assertLessEqual(expected_nodes, graph.get_nodes())
        self.assertEquals('worker', graph.get_node_attribute(19, 'type'))
        self.assertEquals('employer', graph.get_node_attribute(888, 'type'))

        # same graph, should add just another employer node
        interpreter.worker_id = 19
        interpreter.employer_id = 999
        expected_nodes = [19,888,999]
        create_nodes(interpreter, graph)
        self.assertLessEqual(expected_nodes, graph.get_nodes())
        self.assertEquals('worker', graph.get_node_attribute(19, 'type'))
        self.assertEquals('employer', graph.get_node_attribute(888, 'type'))
        self.assertEquals('employer', graph.get_node_attribute(999, 'type'))

        # add an entry with same ids, don't change anything
        interpreter.worker_id = 19
        interpreter.employer_id = 999
        expected_nodes = [19,888,999]
        create_nodes(interpreter, graph)
        self.assertLessEqual(expected_nodes, graph.get_nodes())
        self.assertEquals('worker', graph.get_node_attribute(19, 'type'))
        self.assertEquals('employer', graph.get_node_attribute(888, 'type'))
        self.assertEquals('employer', graph.get_node_attribute(999, 'type'))

        # add two more different node ids, just to check...
        interpreter.worker_id = 33
        interpreter.employer_id = 3333
        expected_nodes = [19,888,999, 33, 3333]
        create_nodes(interpreter, graph)
        self.assertLessEqual(expected_nodes, graph.get_nodes())
        self.assertEquals('worker', graph.get_node_attribute(19, 'type'))
        self.assertEquals('employer', graph.get_node_attribute(888, 'type'))
        self.assertEquals('employer', graph.get_node_attribute(999, 'type'))
        self.assertEquals('worker', graph.get_node_attribute(33, 'type'))
        self.assertEquals('employer', graph.get_node_attribute(3333, 'type'))

    def test_create_edges(self):

        interpreter = FakeInterpreter()

        # graph with only nodes, no edges...
        graph = graph_manager.SnapManager()
        graph.add_node(19)
        graph.add_node(888)
        graph.add_node(123)
        graph.add_node(456)

        self.assertEquals(0, graph.get_edge_count())

        # just adding an edge
        interpreter.worker_id = 19
        interpreter.employer_id = 888
        create_edges(interpreter, graph)
        self.assertEquals(1, graph.get_edge_count())

        # same ids, we still add an edge!
        interpreter.worker_id = 19
        interpreter.employer_id = 888
        create_edges(interpreter, graph)
        self.assertEquals(2, graph.get_edge_count())

        # different ids, same thing...
        interpreter.worker_id = 123
        interpreter.employer_id = 456
        create_edges(interpreter, graph)
        self.assertEquals(3, graph.get_edge_count())

        # a bit fancy now...but same thing
        interpreter.worker_id = 19
        interpreter.employer_id = 456
        create_edges(interpreter, graph)
        self.assertEquals(4, graph.get_edge_count())

    def test_passes_filter(self):
        # no worker_id rule
        interpreter = FakeInterpreter()
        interpreter.worker_id = 33
        self.assertTrue(passes_filter(interpreter))

        interpreter.worker_id = -1
        self.assertFalse(passes_filter(interpreter))

        # no employer_id rule
        interpreter = FakeInterpreter()
        interpreter.worker_id = 222
        self.assertTrue(passes_filter(interpreter))

        interpreter.worker_id = -1
        self.assertFalse(passes_filter(interpreter))

        # no year rule
        interpreter = FakeInterpreter()
        interpreter.year = -1
        self.assertFalse(passes_filter(interpreter))

        interpreter.year = 1999
        self.assertTrue(passes_filter(interpreter))


# todo, make interpreter an ABC
class FakeInterpreter():
    ''' This class makes our lives easier when testing'''
    def __init__(self):
        self.year = 0
        self.admission_date = 0
        self.demission_date = 0
        self.worker_id = 0
        self.employer_id = 0
        self.time_at_employer = 0



if __name__ == "__main__":
    unittest.main()
