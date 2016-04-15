import unittest
import sys
import mock
import logging
sys.path.insert(0, '../src/')
from build_raw_graph import *
import data_parser
import graph_manager


class TestDataAnalysis(unittest.TestCase):
    # TODO: if there is enough time, decouple from other classes.

    def setUp(self):
        # logging.disable(logging.CRITICAL)
        pass

    @mock.patch('build_raw_graph.process_file')
    def test_process_files(self, process_file_mock):

        # should process 3 files
        src_folder = "./test_file_path_folder/"
        parser = data_parser.Pis12DataParser()
        interpreter_class = data_parser.Pis12DataInterpreter
        manager = graph_manager.SnapManager
        process_files(src_folder, parser, interpreter_class, manager)
        self.assertEquals(3, process_file_mock.call_count)

    def test_process_file(self):

        # should call process_line 30 times
        file_path = './test_data/raw_graph.csv'
        parser = data_parser.Pis12DataParser()
        interpreter_class = data_parser.Pis12DataInterpreter
        graph = graph_manager.SnapManager()
        process_file(file_path, parser, interpreter_class, graph)
        self.assertEquals(30, process_line_mock.call_count)

        # from raw_graph, we should see 3 worker nodes
        # we should also see 3 employer nodes.
        self.assertEquals(3, graph.node_count())


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




# todo, make interpreter an ABC
class FakeInterpreter():
    ''' This class makes our lives easier when testing'''
    def __init__(self):
        self.year = 0
        self.admission_date = 0
        self.demission_date = 0
        self.worker_id = 0
        self.employer_id = 0
        self.time_at_worker = 0



if __name__ == "__main__":
    unittest.main()
