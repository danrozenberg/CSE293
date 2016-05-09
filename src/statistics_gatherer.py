import numpy as np
import matplotlib.pyplot as plt
from graph_manager import SnapManager


class StatisticsGatherer(object):

    # generate a report on the graph.
    def report(self):
        pass


    @staticmethod
    def fill_results(node_list, graph_method):
        return map(graph_method, node_list)


    @staticmethod
    def print_statistics(result_list):
        result_string = "average = " + str(np.average(result_list))
        result_string += "\nstddev = " + str(np.std(result_list))
        result_string += "\n10th percentile = " + str(np.percentile(result_list, 10))
        result_string += "\n25th percentile = " + str(np.percentile(result_list, 25))
        result_string += "\nmedian = " + str(np.median(result_list))
        result_string += "\n75th percentile = " + str(np.percentile(result_list, 75))
        result_string += "\n90th percentile = " + str(np.percentile(result_list, 90))
        result_string += "\nmax = " + str(max(result_list))
        result_string += "\nmin = " + str(min(result_list))
        return result_string


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

    def save_report(self, output_path):
        # saves output in a csv, ready to be EXCELED!
        pass

    @staticmethod
    def get_node_sample(graph_manager, sample_size):
        # get a a sample of nodes for calculating statistics

        # don't "overdraw" the graph
        if graph_manager.get_node_count() < sample_size:
            return graph_manager.get_nodes()

        node_set = set()
        while len(node_set) < sample_size:
            node_set.add(graph_manager.get_random_node())
        return list(node_set)


# def run_script(load_path):
#     gatherer = StatisticsGatherer()
#     connected_graph = SnapManager().load_graph(load_path)
#
#
#     # shortests paths
#     method = connected_graph.get_shortest_path_size
#     results =  gatherer.fill_results(method)
#
#
#     # save everything.
#
# if __name__ == '__main__':
#     load_path = "../output_graphs/rs_affiliation.graph"
#     run_script(load_path)
