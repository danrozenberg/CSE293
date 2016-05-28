"""This file helps us compiling results for our project
as such, so save coding / plannign time, it will be super specific"""

import random
import sys
import cPickle as pickle
import numpy as np
from graph_manager import SnapManager
from data_parser import ClassificationLoader
from datetime import datetime

#TODO : we don't really need a class here...
class StatisticsGatherer(object):

    @staticmethod
    def print_datetime(stream=sys.stdout):
        now = str(datetime.now())
        stream.write("\ndate;" + now + "\n")

    @staticmethod
    def build_ground_truth(load_folder, output_file_path, save=True):
        """ Builds ground truth from csv files,
            it also pickles everything so we can load it later"""
        loader = ClassificationLoader()
        for file_path in loader.find_files(load_folder, 0):
            for line in loader.lines_reader(file_path, 0):
                loader.parse_line(line)
        if save:
            pickle.dump(loader.truth_data, open(output_file_path, 'wb'))
        return loader.truth_data

    @staticmethod
    def load_ground_truth(target_file):
        return pickle.load(open(target_file, 'rb'))

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
        results = []
        results.append(("name",name))
        results.append(("average",np.average(result_list)))
        results.append(("stddev",np.std(result_list)))
        results.append(("10th percentile",np.percentile(result_list, 10)))
        results.append(("25th percentile",np.percentile(result_list, 25)))
        results.append(("median",np.median(result_list)))
        results.append(("75th percentile",np.percentile(result_list, 75)))
        results.append(("90th percentile",np.percentile(result_list, 90)))
        results.append(("max",max(result_list)))
        results.append(("min",min(result_list)))
        return results

    @staticmethod
    def print_statistics(statistics, stream):
        for stat in statistics:
            stream.write(stat[0] + ";" + str(stat[1]) +"\n")
        stream.write("\n")

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
    def print_sample_specific_stats(graph, sample=None, stream=sys.stdout):
        gatherer = StatisticsGatherer

        # use everything is we were not given a sample
        if sample is None:
            sample = graph.get_nodes()

        results = gatherer.get_statistics(
            [graph.get_shortest_path_size(n) for n in sample],
            "shortest_path")
        gatherer.print_statistics(results, stream)

        results = gatherer.get_statistics(
            [graph.get_node_degree(n) for n in sample],
            "node_degree")
        gatherer.print_statistics(results, stream)

        results = gatherer.get_statistics(
            [graph.get_clustering_coefficient(n) for n in sample],
            "clustering_coefficient")
        gatherer.print_statistics(results, stream)

        # TODO, use closeness maybe?
        results = gatherer.get_statistics(
            [graph.get_degree_centrality(n) for n in sample],
            "degree_centrality")
        gatherer.print_statistics(results, stream)

        # add more stuff here.

    @staticmethod
    def print_graph_specific_metrics(graph, stream=sys.stdout):
        stream.write("\n" + "nodes;" + str(graph.get_node_count()))
        stream.write("\n" + "edges;" + str(graph.get_edge_count()))
        stream.write("\n" + "diameter;" + str(graph.get_diameter()))

        component_sizes = list(graph.get_connected_components())
        stream.write("\n" + "connected components;" + str(len(component_sizes)))
        stream.write("\n" + "component sizes;" + str(component_sizes)[1:-1])

    @staticmethod
    def print_correl_info(x_axis_method, y_axis_method, sample=None, stream=sys.stdout):

        # use everything is we were not given a sample
        if sample is None:
            sample = graph.get_nodes()

        x_axis = [x_axis_method(n) for n in sample]
        y_axis = [y_axis_method(n) for n in sample]

        for i in xrange(len(x_axis)):
            stream.write("\n" + str(y_axis[i]) + ";" + str(x_axis[i]))

    @staticmethod
    def print_node_degree_dist(graph, stream=sys.stdout):
        dist = graph.get_degree_dist()
        for key in dist.keys():
            stream.write(str(key) + ";" + str(dist[key]) + "\n")

def run_script(graph, stream=sys.stdout):
    gatherer = StatisticsGatherer
    gatherer.print_datetime(stream)
    # gatherer.print_graph_specific_metrics(graph, stream)
    # gatherer.print_sample_specific_stats(graph, stream=stream)
    # gatherer.print_node_degree_dist(graph, stream)
    gatherer.print_correl_info(graph.get_eigenvector_centrality,
                               graph.get_node_degree)


if __name__ == '__main__':
    graph = SnapManager()
    graph.generate_random_graph(1000,4,0.3)
    run_script(graph)

    # with open("../output_stats/graph_summary.csv", 'wb') as f:
    #     run_script(graph; f)


