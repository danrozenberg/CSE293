
class SnapStatisticsGatherer(object):

    # generate a report on the graph.
    def report(self):
        pass


    @staticmethod
    def fill_results(node_list, graph_method):
        return map(graph_method, node_list)

    def save_report(self, output_path):
        # saves output in a csv, ready to be EXCELED!
        pass

