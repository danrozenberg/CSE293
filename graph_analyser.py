import networkx as nx

# given a graph, run a lot of analysis on it.

class GraphAnalyser(object):

    def __init__(self, graph):
        self.graph = graph

    def draw(self, color_map = None):
        plt = self.check_matplotlib()

        # TODO: make this code better
        if plt is not None:
            if color_map is not None:
                nx.draw(self.graph, node_color=[color_map[self.graph.node[node]['category']] for node in self.graph])
            else:
                nx.draw(self.graph)

            plt.show()


    @staticmethod
    def check_matplotlib():
        try:
            import matplotlib.pyplot as plt
            return  plt
        except ImportError:
            print("Matplotlib needed for drawing. Skipping")
            return None

