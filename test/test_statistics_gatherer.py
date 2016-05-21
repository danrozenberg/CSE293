import os
import unittest
import sys
sys.path.insert(0, '../src/')
from statistics_gatherer import StatisticsGatherer
import graph_manager


class TestSnapStatisticsGatherer(unittest.TestCase):

    def test_get_node_sample(self):
        gatherer = StatisticsGatherer
        manager = graph_manager.SnapManager()
        manager.generate_random_graph(20,2,0.5)

        sample = gatherer.get_node_sample(manager, 5)
        self.assertEquals(5, len(sample))

        sample = gatherer.get_node_sample(manager, 999)
        self.assertEquals(20, len(sample))

    def test_build_ground_truth(self):
        # also tests save and load"

        gatherer = StatisticsGatherer
        load_folder = "./classification_files/"
        output_path = "./classification_files/ground_truth.p"

        # file gets built
        loader = gatherer.build_ground_truth(load_folder, output_path)

        # file is saved
        self.assertTrue(os.path.isfile(output_path))

        # file gets loaded
        loader2 = gatherer.load_ground_truth(output_path)

        # matchin a few things is enough, I trust python...
        self.assertEqual(loader.plant_id, loader2.plant_id)
        self.assertEqual(loader.firm_type, loader2.firm_type)

        # cleanup
        os.remove(output_path)



if __name__ == "__main__":
    unittest.main()
