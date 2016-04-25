import snap

class SnapPlotter():
    def __init__(self, graph_manager):
        self.manager = graph_manager
        self.network = graph_manager.network
        self.output_folder = "../output_images/"


    def draw(self, file_name, description = " ", labels = None):

        if labels is None:
            labels = snap.TIntStrH()
            for NI in self.network.Nodes():
                labels[NI.GetId()] = str(self.manager.id_from_NId[NI.GetId()])

        snap.DrawGViz(self.network,
                      snap.gvlDot,      # gvlCirco is nice too
                      self.output_folder + file_name,
                      description,
                      labels)


