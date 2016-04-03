import snap

class GraphGenerator(object):
    """ Abstract class | This is here to implement the strategy pattern that
    returns a usable graph.
    """

    def get_graph(self):
        raise NotImplementedError("Method cannot be called from abstract base class.")


class SnapRandomGraphGenerator(GraphGenerator):

    def get_graph(self):
        FOut = snap.TFOut("test.graph")
        asd = snap.GenRndDegK(100, 12)
        asd.Save(FOut)
        FOut.Flush()
        return asd

