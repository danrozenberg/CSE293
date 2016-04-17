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
        Note: id is a c++ (32 bit) integer, so be careful not to overflow.
        """
        if NId <> -1 and self.network.IsNode(NId):
            logging.debug("Could not add node with id = "
                            + str(NId) + " because it already existed" )
            return NId
        else:
            return self.network.AddNode(NId)

    def delete_node(self, NId):
        """
        Deletes a node, but only if it exists.
        Note that: Accessing arbitrary node takes constant time.
        :param NId: the id of the node to delete
        """
        if self.network.IsNode(NId):
            self.network.DelNode(NId)
        else:
            logging.debug("Could not delete node with id = "
                            + str(NId) + " because it doesn't exist" )

    def add_edge(self, src_node, dest_node, EId=-1):

        if EId <> -1 and self.network.IsEdge(EId):
            logging.debug("Couldn't add edge with EId = "
                            + str(EId) + " because it already exist")
            return EId

        if not self.is_node(src_node):
            logging.debug("Couldn't add edge from node "
                            + str(src_node) + " because such node doesn't exist")
            return None

        if not self.is_node(dest_node):
            logging.debug("Couldn't add edge to node "
                            + str(dest_node) + " because such node doesn't exist")
            return None

        return self.network.AddEdge(src_node, dest_node, EId)

    def get_edges(self):
        raise NotImplementedError

    def get_edge(self, EId):
        return self.network.GetEI(EId)

    def get_nodes(self):
        # TODO: make my own node iterator, since snap doesnt give us a REAL one.
        if self.get_node_count() == 0:
            return []
        else:
            nodes = []
            node_iterator = self.network.BegNI()
            nodes.append(node_iterator.GetId())

            # note, the Nodes() method does not work in SNAP, for some reason...
            # actually, its missing a lot of useful stuff.
            # gotta do it like this:  =(
            while node_iterator.Next():
                try:
                    nodes.append(node_iterator.GetId())
                except RuntimeError:
                    return nodes

    def get_node(self, NId):
        return self.network.GetNI(NId)

    def get_node_attributes(self, NId):
        """
        :param NId: the node to retrieve attributes from
        :return: a dictionary with 'attr name' - 'attr value' pairs
        """
        names = snap.TStrV()
        values = snap.TStrV()
        converted_values = []
        self.network.AttrNameNI(NId, names)
        self.network.AttrValueNI(NId, values)

        for value in values:
            # Due to a SNAP bug we are forced to convert attributes
            #   back to their original type ourselves ;(
            converted_values.append(self.__convert(value))

        return dict(zip(names, converted_values))

    def get_node_attribute(self, NId, attr_name):
        # Probably not the most efficient way of doing this...
        attributes = self.get_node_attributes(NId)

        try:
            return attributes[attr_name]
        except KeyError:
            raise RuntimeError("Node " + str(NId) + " does not have attribute '" +
                                               attr_name + "'")

    def get_edge_attributes(self, EId):
        """
        :param EId: the edge to retrieve attributes from
        :return: a dictionary with 'attr name' - 'attr value' pairs
        """
        names = snap.TStrV()
        values = snap.TStrV()
        converted_values = []
        self.network.AttrNameEI(EId, names)
        self.network.AttrValueEI(EId, values)

        for value in values:
            # Due to a SNAP bug we are forced to convert attributes
            #   back to their original type ourselves ;(
            converted_values.append(self.__convert(value))

        return dict(zip(names, converted_values))

    def get_edge_attribute(self, EId, attr_name):
        # Probably not the most efficient way of doing this...
        attributes = self.get_edge_attributes(EId)

        try:
            return attributes[attr_name]
        except KeyError:
            raise RuntimeError("Edge " + str(EId) + " does not have attribute '" +
                                               attr_name + "'")

    def get_node_count(self):
        return self.network.GetNodes()

    def get_edge_count(self):
        return self.network.GetEdges()

    def add_node_attr(self, NId, name, value):

        node = self.get_node(NId)
        if isinstance(value, int):
            self.network.AddIntAttrDatN(node, value, name)
        elif isinstance(value, float):
            self.network.AddFltAttrDatN(node, value, name)
        elif isinstance(value, str):
            self.network.AddStrAttrDatN(node, value, name)
        else:
            raise Exception('Invalid data type')

    def add_edge_attr(self, EId, name, value):

        edge = self.get_edge(EId)
        if isinstance(value, int):
            self.network.AddIntAttrDatE(edge, value, name)
        elif isinstance(value, float):
            self.network.AddFltAttrDatE(edge, value, name)
        elif isinstance(value, str):
            self.network.AddStrAttrDatE(edge, value, name)
        else:
            raise Exception('Invalid data type')

    def is_node(self, NId):
        return self.network.IsNode(NId)

    def save_graph(self, file_path):
        FOut = snap.TFOut(file_path)
        self.network.Save(FOut)
        FOut.Flush()

    def load_graph(self, file_path):
        FIn = snap.TFIn(file_path)
        self.network = snap.TNEANet.Load(FIn)
        return self

    # noinspection PyMethodMayBeStatic
    def __convert(self, value):
        # TODO: use SNAP verifications instead because it is
        # probably faster.

        try:
            return float(value) if '.' in value else int(value)
        except ValueError:
            return value
