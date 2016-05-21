"""This file helps us compiling results for our project
as such, so save coding / plannign time, it will be super specific"""

import random
import cPickle as pickle
import numpy as np
import matplotlib.pyplot as plt
from graph_manager import SnapManager
from data_parser import ClassificationLoader


class StatisticsGatherer(object):

    @staticmethod
    def get_statistics(result_list, name=""):
        results = dict
        results["name"] = name
        results["naverage"] = np.average(result_list)
        results["nstddev"] = np.std(result_list)
        results["n10th percentile"] = np.percentile(result_list, 10)
        results["n25th percentile"] = np.percentile(result_list, 25)
        results["nmedian"] = np.median(result_list)
        results["n75th percentile"] = np.percentile(result_list, 75)
        results["n90th percentile"] = np.percentile(result_list, 90)
        results["nmax"] = max(result_list)
        results["nmin"] = min(result_list)
        return results

    @staticmethod
    def build_histogram(result_list, save_path, x_label="Value", y_label="Frequency", title=""):
        bin_num = len(set(result_list))
        plt.hist(result_list, bin_num, normed=1, facecolor='g', alpha=0.75)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        plt.title(title)
        plt.axis([min(result_list), max(result_list), 0, 1])

        # for degugging how it looks
        # plt.show()

        # save
        plt.savefig(save_path)

    @staticmethod
    def get_node_sample(graph_manager, sample_size):
        '''From a graph (at this point the connected worker graph)
        we get a list of nodes at random'''
        # don't "overdraw" the graph
        if graph_manager.get_node_count() < sample_size:
            return graph_manager.get_nodes()

        node_set = set()
        while len(node_set) < sample_size:
            node_set.add(graph_manager.get_random_node())
        return list(node_set)

    @staticmethod
    def calculate_node_specific_stats(sample, graph):
        # calculate node-specific metrics
        # it receives a sample of nodes, and will calculate
        # the statistics of the calculated metrics.

        # shorthand
        gatherer = StatisticsGatherer

        # shortests paths
        results = map(graph.get_shortest_path_size, sample)
        gatherer.print_statistics(results, "get_shortest_path_size")
        # TODO: do histogram data

        # node centrality
        results = map(graph.get_degree_centrality, sample)
        gatherer.print_statistics(results, "get_degree_centrality")

        results = map(graph.get_eccentricity, sample)
        gatherer.print_statistics(results, "get_eccentricity")

    @staticmethod
    def calculate_graph_specific_metrics(graph):

        # shorthand
        gatherer = StatisticsGatherer

        # node centrality
        gatherer.print_metric(graph.get_betweeness_centrality(),
                              "betweeness_centrality")

        gatherer.print_metric(graph.get_eigenvector_centrallity(),
                              "eigenvector_centrallity")

        # connected components
        gatherer.print_metric(graph.get_connected_components(),
                              "connected_components")

        # TRIADS AND CLUSTERING
        gatherer.print_metric(graph.get_clustering_coefficient(),
                              "get_clustering_coefficient")

    @staticmethod
    def build_ground_truth(load_folder, output_file_path):
        """ Builds ground truth from csv files,
            it also pickles everything so we can load it later"""
        loader = ClassificationLoader()
        for file_path in loader.find_files(load_folder, 0):
            for line in loader.lines_reader(file_path, 0):
                loader.parse_line(line)
        pickle.dump(loader, open(output_file_path, 'wb'))
        return loader

    @staticmethod
    def load_ground_truth(target_file):
        return pickle.load(open(target_file, 'rb'))

def run_script(load_path):
    gatherer = StatisticsGatherer
    graph = SnapManager().load_graph(load_path)
    sample = gatherer.get_node_sample(graph, 100)

    gatherer.calculate_node_specific_stats(sample, graph)






    # DO graph-as-a-whole stats


if __name__ == '__main__':
    load_path = "../output_graphs/rs_affiliation.graph"
    run_script(load_path)
