import unittest
import sys
sys.path.insert(0, '../src/')
import mock
import graph_manager
import connect_workers


class ConnectWorkersTest(unittest.TestCase):

    @mock.patch('build_raw_graph.passes_filter', return_value=100)
    def test_a_few_simple_cases(self):
        manager = graph_manager.SnapManager()

        # add the first worker
        w1 = manager.add_node(1)
        manager.add_node_attr(w1, "type", "worker")
        manager.add_node_attr(w1, "type", "worker")



if __name__ == "__main__":
    unittest.main()
