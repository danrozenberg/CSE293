import sys, os
import unittest
sys.path.insert(0, '../src/')
import graph_plotter
import graph_manager

class GraphPlotterTest(unittest.TestCase):

    def test_draw(self):
        # Create an association graph
        manager = graph_manager.SnapManager()
        self.create_association_graph(manager)
        self.assertTrue(manager.get_node_count() > 0)

        # create plotter and change output folder
        plotter = graph_plotter.SnapPlotter(manager)
        plotter.output_folder = "./"

        # delete old file if needed
        file_name = "draw_test.png"
        file_path = plotter.output_folder + file_name
        if os.path.isfile(file_path):
            os.remove(file_path)

        # DO IT !11!!!!
        asd = plotter.draw(file_name, "My Test Graph")

        # file should exist
        self.assertTrue(os.path.isfile(file_path))


    def create_association_graph(self, manager):
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
        manager.add_node(10)
        manager.add_node(20)
        manager.add_node(30)
        manager.add_node_attr(10, "type", "employer")
        manager.add_node_attr(20, "type", "employer")
        manager.add_node_attr(30, "type", "employer")
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

def create_association_graph2(manager):
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
    manager.add_node(10)
    manager.add_node(20)
    manager.add_node(30)
    manager.add_node_attr(10, "type", "employer")
    manager.add_node_attr(20, "type", "employer")
    manager.add_node_attr(30, "type", "employer")
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
