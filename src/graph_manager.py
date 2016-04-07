import snap
import logging

class SnapManager(object):
    """
    This implementation deals with SNAP networks.
    """

    def __init__(self):
        self.network = snap.TNEANet()

    def add_node(self, NId=-1):
        """
        Adds a node to the network, if it doesn't exist yet.
        Note that: Accessing arbitrary node takes constant time.
        Note that: Adding a node takes constant time.
        """
        if NId <> -1 and self.network.IsNode(NId):
            logging.warning("Could not add node with id = "
                            + str(NId) + " because it already existed" )
        else:
            self.network.AddNode(NId)

    def delete_node(self, NId):
        """
        Deletes a node, but only if it exists.
        Note that: Accessing arbitrary node takes constant time.
        :param NId: the id of the node to delete
        """
        if self.network.IsNode(NId):
            self.network.DelNode(NId)
        else:
            logging.warning("Could not delete node with id = "
                            + str(NId) + " because it doesn't exist" )

    def add_edge(self, src_node, dest_node, EId=-1):

        if EId <> -1 and self.network.IsEdge(EId):
            logging.warning("Could add edge with EId = "
                            + str(EId) + " because it already exist")

        if self.network.IsNode(src_node):
            logging.warning("Could add edge from node "
                            + str(src_node) + " because it doesn't exist")
            return

        if self.network.IsNode(dest_node):
            logging.warning("Could add edge to node "
                            + str(dest_node) + " because it doesn't exist")
            return

        return self.network.AddEdge(src_node, dest_node)

    def get_edges(self):
        return self.network.GetEdges()

    def get_edge(self, EId):
        return self.network.getEI(EId)

    def get_nodes(self):
        return self.network.GetNodes()

    def get_node(self, NId):
        return self.network.GetNI(NId)

    def node_count(self):
        return self.network.GetNodes()

    def add_node_attribute(self, NId, name, value):

        node = self.get_node(NId)
        if isinstance(value, int):
            self.network.AddIntAttrDatN(node, value, name)
        elif isinstance(value, float):
            self.network.AddIntAttrDatN(node, value, name)
        elif isinstance(value, str):
            self.network.AddStrAttrDatN(node, value, name)
        else:
            raise Exception('Invalid data type')


    def add_edge_attribute(self, EId, name, value):

        edge = self.get_edge(EId)
        if isinstance(value, int):
            self.network.AddIntAttrDatE(edge, value, name)
        elif isinstance(value, float):
            self.network.AddIntAttrDatE(edge, value, name)
        elif isinstance(value, str):
            self.network.AddStrAttrDatE(edge, value, name)
        else:
            raise Exception('Invalid data type')




