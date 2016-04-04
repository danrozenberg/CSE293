import snap

class AbstractGraphGenerator(object):

    """ Abstract class | This is here to implement the strategy pattern that
    returns a usable graph.
    """

    def random_graph(self):
        """
        :return: some random graph, mostly for testing purposes
        """
        raise NotImplementedError("Method cannot be called from abstract base class.")

    def add_node(self):
        raise NotImplementedError("Method cannot be called from abstract base class.")

    def add_edge(self, node1, node2, information = None):
        raise NotImplementedError("Method cannot be called from abstract base class.")


class SnapGenerator(AbstractGraphGenerator):

    def add_node(self):
        pass

    def add_edge(self, node1, node2, information=None):
        pass

    def random_graph(self):
        # TODO: make it not save the random graph.
        FOut = snap.TFOut("test.graph")
        rand_graph = snap.GenRndDegK(100, 12)
        rand_graph.Save(FOut)
        FOut.Flush()
        return rand_graph

