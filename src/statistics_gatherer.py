# gathers and prints statistics

class SnapStatisticsGatherer(object):
    def __init__(self, graph):
        self.graph = graph

    # generate a report on the graph.
    def report(self):
        pass

    def fill_results(self, node_list, graph_method):
        print "asd"
        statistics = []
        for node in node_list:
            statistics.append(graph_method(node))
            
        return statistics

    def save_report(self, output_path):
        # saves output in a csv, ready to be EXCELED!
        pass

