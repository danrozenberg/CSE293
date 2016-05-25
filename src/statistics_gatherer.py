"""This file helps us compiling results for our project
as such, so save coding / plannign time, it will be super specific"""

import random
import cPickle as pickle
import numpy as np
from graph_manager import SnapManager
from data_parser import ClassificationLoader


#TODO : we don't really need a class here...
class StatisticsGatherer(object):

    @staticmethod
    def get_valid_plants(ground_truth, affiliation_graph):
        """ depending on how we built affiliations graph
        it will contain only a subset of plants found in ground truth
        thus, we create a new ground trugh, containing only plants
        that actually exist in our affiliation graph.
        """
        valid_ground_data = {}
        for key in ground_truth.keys():
            if affiliation_graph.is_node(key):
                type = affiliation_graph.get_node_attr(key, "type")
                if type == "employer":
                    valid_ground_data[key] = ground_truth[key]

        return  valid_ground_data

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
    def get_plant_sample(ground_truth):
        """from ground truth, give me a sample dictionary with
        plant_id -> type"""

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
    def build_ground_truth(load_folder, output_file_path, save=True):
        """ Builds ground truth from csv files,
            it also pickles everything so we can load it later"""
        loader = ClassificationLoader()
        for file_path in loader.find_files(load_folder, 0):
            if "new25" in file_path:
                continue
            for line in loader.lines_reader(file_path, 0):
                loader.parse_line(line)
        if save:
            pickle.dump(loader.truth_data, open(output_file_path, 'wb'))
        return loader.truth_data

    @staticmethod
    def load_ground_truth(target_file):
        return pickle.load(open(target_file, 'rb'))

def affiliation_stats_script(load_path, output_file_path):
    ''' saves several properties of the affiliation graph'''
    graph = SnapManager().load_graph(load_path)
    graph.print_info(output_file_path,
                     "Affiliation Graph POA")

def run_script():
    gatherer = StatisticsGatherer

    ground_truth = gatherer.build_ground_truth("X:/state-csv/", "../output_stats/ground_truth_new_4d.p")





    # DO graph-as-a-whole stats


if __name__ == '__main__':
    run_script()

