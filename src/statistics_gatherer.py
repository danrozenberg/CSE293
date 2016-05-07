import numpy as np
import matplotlib.pyplot as plt

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
