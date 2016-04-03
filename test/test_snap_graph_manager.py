import os
import unittest
import sys
import snap
import config_manager
sys.path.append('..')

class TestDataParser(unittest.TestCase):

    def setUp(self):
        self.config = config_manager.Config()

    # should save a picture in the output folder
    # this test is useful to check if snap and graphviz are installed correctly.
    def test_drawing(self):

        output_file_path = self.config.get_image_output_path() + \
                           "my_test_file.png"

        # delete file first..
        if os.path.isfile(output_file_path):
            os.remove(output_file_path)

        network = snap.GenRndGnm(snap.PNEANet, 10, 50)
        labels = snap.TIntStrH()
        for node in network.Nodes():
            labels[node.GetId()] = str(node.GetId())
        snap.DrawGViz(network, snap.gvlDot, output_file_path, "MyNetwork ",labels)

        # test if file creation was successful
        self.assertTrue(os.path.isfile(output_file_path))

        # don't leave file hanging around
        os.remove(output_file_path)

if __name__ == "__main__":
    unittest.main()
