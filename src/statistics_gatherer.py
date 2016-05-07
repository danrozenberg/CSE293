import math
import matplotlib.pyplot as plt

class StatisticsGatherer(object):

    # generate a report on the graph.
    def report(self):
        pass


    @staticmethod
    def fill_results(node_list, graph_method):
        return map(graph_method, node_list)


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

