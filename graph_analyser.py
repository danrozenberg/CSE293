import snap

# given a graph, run a lot of analysis on it.

class GraphAnalyser(object):

    def __init__(self, graph):
        self.graph = graph

    def draw(self):
        # network = snap.GenRndGnm(snap.PNEANet, 10, 50)
        #
        # labels = snap.TIntStrH()
        # for node in network.Nodes():
        #     labels[node.GetId()] = str(node.GetId())
        # snap.DrawGViz(network, snap.gvlDot, "../output_images/gvlDot.png", "MyNetwork ",labels)
