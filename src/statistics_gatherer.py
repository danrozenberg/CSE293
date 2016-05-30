"""This file helps us compiling results for our project
as such, so save coding / plannign time, it will be super specific"""

import random
import logging
import sys
import cPickle as pickle
import numpy as np
from graph_manager import SnapManager
from data_parser import ClassificationLoader
from datetime import datetime

def print_datetime(stream):
    now = str(datetime.now())
    stream.write("date;" + now)

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

def load_ground_truth(target_file):
    return pickle.load(open(target_file, 'rb'))

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

def get_statistics(result_list, name=""):
    results = list()
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

def print_statistics(statistics, stream):
    for stat in statistics:
        stream.write(stat[0] + ";" + str(stat[1]) +"\n")
    stream.write("\n")

def print_node_statistics(graph, sample, stream):
    logging.warn("Calculating node statistics:")

    logging.warn("shortest_paths...")
    results = get_statistics(
        [graph.get_shortest_path_size(n) for n in sample],
        "shortest_path")
    print_statistics(results, stream)

    logging.warn("node_degrees...")
    results = get_statistics(
        [graph.get_node_degree(n) for n in sample],
        "node_degree")
    print_statistics(results, stream)

    logging.warn("clustering_coefficient...")
    results = get_statistics(
        [graph.get_clustering_coefficient(n) for n in sample],
        "clustering_coefficient")
    print_statistics(results, stream)

    logging.warn("get_degree_centrality...")
    results = get_statistics(
        [graph.get_degree_centrality(n) for n in sample],
        "degree_centrality")
    print_statistics(results, stream)

    # add more stuff here.

def print_structure_metrics(graph, stream):
    logging.warn("Calculating structure metrics...")
    stream.write("\n" + "nodes;" + str(graph.get_node_count()))
    stream.write("\n" + "edges;" + str(graph.get_edge_count()))
    stream.write("\n" + "90th percentile diameter;" + str(graph.get_diameter()))

    component_sizes = list(graph.get_connected_components())
    stream.write("\n" + "connected components;" + str(len(component_sizes)))
    stream.write("\n" + "component sizes;" + str(component_sizes)[1:-1] + "\n")

def print_correl_info(x_axis_method, y_axis_method, sample):
    x_axis = [x_axis_method(n) for n in sample]
    y_axis = [y_axis_method(n) for n in sample]

    for i in xrange(len(x_axis)):
        stream.write("\n" + str(y_axis[i]) + ";" + str(x_axis[i]))

def print_correl_wages(x_axis_method, affiliation_graph, year, sample_size, stream):
    logging.warn("Start calculating wage correlation...")
    node_candidates = filter_nodes_with_wage_in_year(affiliation_graph,
                                                     year)
    sample = random.sample(node_candidates, sample_size)

    stream.write("\n wages correl\n")
    x_axis = [x_axis_method(n) for n in sample]
    y_axis = [affiliation_graph.get_wage(n, year) for n in sample]

    for i in xrange(len(x_axis)):
        stream.write("\n" + str(x_axis[i]) + ";" + str(y_axis[i]))

def print_node_degree_dist(graph, stream):
    logging.warn("Calculating degree distribution...")
    dist = graph.get_degree_dist()
    stream.write("\n degree distribution\n")
    for key in dist.keys():
        stream.write(str(key) + ";" + str(dist[key]) + "\n")

def filter_nodes_with_wage_in_year(graph, year):
    candidates = filter(lambda n: graph.has_wage(n, year),
                        graph.get_nodes())
    return candidates

def enable_logging(log_level):
    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=log_level)

    # so we know how long it took

if __name__ == '__main__':
    enable_logging(logging.WARNING)
    logging.warn("Started gathering statistics")

    target_name = "poa_directors"
    with open("../anonymized_results/" + target_name + "_summary.csv", 'wb') as f:

        # setup
        year = 2001
        stream = f   # stream = sys.stdout
        sample_size = 1000

        # set graphs
        affiliation_graph = SnapManager()
        affiliation_graph.load_graph_lite("X:/output_graphs/" + target_name + "_affiliation.graph")

        connected_graph = SnapManager()
        connected_graph.load_graph_lite("X:/output_graphs/" + target_name + "_connected_" + \
            str(year) + ".graph")

        # networks tructure as a whole
        print_structure_metrics(connected_graph, stream)

        # these take a sample explicitely
        sample = random.sample(connected_graph.get_nodes(), sample_size)
        print_node_statistics(connected_graph, sample, stream)

        # specific distributions
        print_node_degree_dist(connected_graph, stream)

        # wage vs centrality...gets its own sample inside
        print_correl_wages(connected_graph.get_eigenvector_centrality,
                           affiliation_graph,
                           year,
                           sample_size)

    logging.warn("Finished")


