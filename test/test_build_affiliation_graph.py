import unittest
import sys, os
import mock
import logging
sys.path.insert(0, '../src/')
from build_affiliation_graph import *
import data_parser
import graph_manager


class TestBuildAffiliationGraph(unittest.TestCase):
    # TODO: if there is enough time, decouple from other classes.

    def setUp(self):
        logging.disable(logging.CRITICAL)

    def test_process_file(self):

        # should call process_line 30 times
        file_path = './test_data/raw_graph.csv'
        parser = data_parser.Pis12DataParser()
        interpreter_class = data_parser.Pis12DataInterpreter
        graph = graph_manager.SnapManager()
        process_file(file_path, parser, interpreter_class, graph)

        # from affiliation_graph, we should see 5 worker nodes
        # we should also see 3 employer nodes.
        self.assertEquals(8, graph.get_node_count())

    @mock.patch('build_affiliation_graph.create_nodes')
    @mock.patch('build_affiliation_graph.create_edges')
    @mock.patch('build_affiliation_graph.passes_filter', return_value=True)
    def test_process_line_if_filter_is_ok(self, mock_filter,
                           mock_edges, mock_nodes):

        process_line(FakeInterpreter(), graph_manager.SnapManager())
        self.assertEquals(1, mock_nodes.call_count)
        self.assertEquals(1, mock_filter.call_count)
        self.assertEquals(1, mock_edges.call_count)

    @mock.patch('build_affiliation_graph.create_nodes')
    @mock.patch('build_affiliation_graph.create_edges')
    @mock.patch('build_affiliation_graph.passes_filter', return_value=False)
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
        interpreter.year = 2015
        interpreter.avg_wage = 12.34
        expected_nodes = [19,888]
        create_nodes(interpreter, graph)
        self.assertLessEqual(expected_nodes, graph.get_nodes())
        self.assertEqual(2, len(graph.get_node_attrs(19)))
        self.assertEqual(1, len(graph.get_node_attrs(888)))
        self.assertEquals('worker', graph.get_node_attr(19, 'type'))
        self.assertEquals('employer', graph.get_node_attr(888, 'type'))
        self.assertTrue('2015_aw' in  graph.get_node_attrs(19))
        self.assertEquals(12.34, graph.get_node_attrs(19)['2015_aw'])

        # same graph, should add just another employer node
        interpreter.worker_id = 19
        interpreter.employer_id = 999
        interpreter.year = 2015
        interpreter.avg_wage = 12.34
        expected_nodes = [19,888,999]
        create_nodes(interpreter, graph)
        self.assertLessEqual(expected_nodes, graph.get_nodes())
        self.assertEqual(2, len(graph.get_node_attrs(19)))
        self.assertEqual(1, len(graph.get_node_attrs(888)))
        self.assertEqual(1, len(graph.get_node_attrs(999)))
        self.assertEquals('worker', graph.get_node_attr(19, 'type'))
        self.assertEquals('employer', graph.get_node_attr(888, 'type'))
        self.assertEquals('employer', graph.get_node_attr(999, 'type'))
        self.assertTrue('2015_aw' in  graph.get_node_attrs(19))
        self.assertEquals(12.34, graph.get_node_attrs(19)['2015_aw'])

        # add an entry with same ids, don't change anything, except wage
        interpreter.worker_id = 19
        interpreter.employer_id = 999
        expected_nodes = [19,888,999]
        interpreter.year = 2015
        interpreter.avg_wage = 15.34
        create_nodes(interpreter, graph)
        self.assertLessEqual(expected_nodes, graph.get_nodes())
        self.assertEquals('worker', graph.get_node_attr(19, 'type'))
        self.assertEquals('employer', graph.get_node_attr(888, 'type'))
        self.assertEquals('employer', graph.get_node_attr(999, 'type'))
        self.assertTrue('2015_aw' in  graph.get_node_attrs(19))
        self.assertEquals(15.34, graph.get_node_attrs(19)['2015_aw'])

        # add two more different node ids, just to check...
        interpreter.worker_id = 33
        interpreter.employer_id = 3333
        interpreter.year = 2016
        expected_nodes = [19,888,999, 33, 3333]
        interpreter.avg_wage = 17.34
        create_nodes(interpreter, graph)
        self.assertLessEqual(expected_nodes, graph.get_nodes())
        self.assertEquals('worker', graph.get_node_attr(19, 'type'))
        self.assertEquals('employer', graph.get_node_attr(888, 'type'))
        self.assertEquals('employer', graph.get_node_attr(999, 'type'))
        self.assertEquals('worker', graph.get_node_attr(33, 'type'))
        self.assertEquals('employer', graph.get_node_attr(3333, 'type'))
        self.assertTrue('2015_aw' in  graph.get_node_attrs(19))
        self.assertEquals(15.34, graph.get_node_attrs(19)['2015_aw'])
        self.assertTrue('2016_aw' in  graph.get_node_attrs(33))
        self.assertEquals(17.34, graph.get_node_attrs(33)['2016_aw'])

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
        interpreter.avg_wage = 12.34
        interpreter.year = 2015
        create_edges(interpreter, graph)
        self.assertEquals(1, graph.get_edge_count())
        attributes = graph.get_edge_attrs(graph.get_edge_between(19,888))
        self.assertEqual(2, len(attributes))
        self.assertTrue('2015_ad' in  attributes)
        self.assertFalse('2014_ad' in  attributes)
        self.assertTrue('2015_de' in  attributes)
        self.assertFalse('2014_de' in  attributes)


        # same ids, we DON'T add an edge!
        # but we do add another attribute!
        interpreter.worker_id = 19
        interpreter.employer_id = 888
        interpreter.year = 2014
        interpreter.avg_wage = 1.14
        create_edges(interpreter, graph)
        self.assertEquals(1, graph.get_edge_count())
        attributes = graph.get_edge_attrs(graph.get_edge_between(19,888))
        self.assertTrue('2015_ad' in  attributes)
        self.assertTrue('2014_ad' in  attributes)

        # different ids, add an edge...
        interpreter.worker_id = 123
        interpreter.employer_id = 456
        interpreter.year = 2015
        create_edges(interpreter, graph)
        self.assertEquals(2, graph.get_edge_count())

        # a bit fancy now...but same thing
        interpreter.worker_id = 19
        interpreter.employer_id = 456
        interpreter.year = 2015
        create_edges(interpreter, graph)
        self.assertEquals(3, graph.get_edge_count())

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

        # single state rule
        interpreter = FakeInterpreter()
        interpreter.municipality = ''
        self.assertFalse(passes_filter(interpreter))

        # extra worker id filter rule
        allowed_ids = [33,44]
        interpreter = FakeInterpreter()
        interpreter.worker_id = 33
        self.assertTrue(passes_filter(interpreter, allowed_ids))

        interpreter = FakeInterpreter()
        interpreter.worker_id = 44
        self.assertTrue(passes_filter(interpreter, allowed_ids))

        interpreter = FakeInterpreter()
        interpreter.worker_id = 55
        self.assertFalse(passes_filter(interpreter, allowed_ids))

    def process_line(self):
        graph = graph_manager.SnapManager()

        interpreter = FakeInterpreter()
        interpreter.worker_id = 111
        interpreter.employer_id = 456
        interpreter.year = 2002
        interpreter.municipality = '231324'
        process_line(interpreter)
        self.assertTrue(0, graph.get_node_count())

        interpreter = FakeInterpreter()
        interpreter.worker_id = 111
        interpreter.employer_id = 456
        interpreter.year = 2002
        interpreter.municipality = '431490'
        process_line(interpreter)
        self.assertTrue(1, graph.get_node_count())

        interpreter = FakeInterpreter()
        interpreter.worker_id = 111
        interpreter.employer_id = 456
        interpreter.year = 2002
        interpreter.municipality = '431490'
        process_line(interpreter)
        self.assertTrue(1, graph.get_node_count())


# todo, make interpreter an ABC
class FakeInterpreter():
    ''' This class makes our lives easier when testing'''
    def __init__(self):
        self.year = 0
        self.admission_date = 0
        self.demission_date = 0
        self.admission_timestamp = 0
        self.demission_timestamp = 0
        self.worker_id = 3
        self.employer_id = 30
        self.time_at_employer = 0
        self.municipality = '431490' #some valid value
        self.cbo_group = 239
        self.avg_wage = 12.2


if __name__ == "__main__":
    unittest.main()
