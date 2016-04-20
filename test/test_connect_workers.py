import unittest
import sys
sys.path.insert(0, '../src/')
import mock
import graph_manager
import connect_workers


class ConnectWorkersTest(unittest.TestCase):

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






if __name__ == "__main__":
    unittest.main()
