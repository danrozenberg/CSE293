import snap
import logging

class SnapManager(object):
    """
    This implementation deals with SNAP networks.
    """

    def __init__(self):
        self.network = snap.TNEANet()

        # The following dictionaries basically transform a CEI or CNJP into
        #   values that fit a C++ integers. We want CEI and CNPJ to be
        #   our IDs, but their values do not fit SNAP integers which are
        #   used as node ids :(
        # This is the reason you can see the use of the following dictionaries:
        self.NId_from_id = {}
        self.id_from_NId = {}
        # TODO: try using compiled SNAP hashes instead here.

    def add_node(self, node_id):
        """
        Adds a node to the network, if it doesn't exist yet.
        Note that: Accessing arbitrary node takes constant time.
        Note that: Adding a node takes constant time.
        Note: id is a c++ (32 bit) integer, so be careful not to overflow.
        """
        if self.is_node(node_id):
            # no need to change any state...
            logging.debug("Could not add node with id = "
                            + str(node_id) + " because it already existed" )
        else:
            new_NId = self.network.AddNode(-1)
            self.NId_from_id[node_id] = new_NId
            self.id_from_NId[new_NId] = node_id

        return node_id

    def delete_node(self, node_id):
        """
        Deletes a node, but only if it exists.
        Note that: Accessing arbitrary node takes constant time.
        :param NId: the id of the node to delete
        """
        NId = self.NId_from_id[node_id]

        if self.network.IsNode(NId):
            self.network.DelNode(NId)
        else:
            logging.debug("Could not delete node with id = "
                            + str(NId) + " because it doesn't exist" )

    def add_edge(self, src_id, dest_id, EId=-1):

        if EId <> -1 and self.network.IsEdge(EId):
            logging.info("Couldn't add edge with EId = "
                            + str(EId) + " because it already exist")
            return EId

        if not self.is_node(src_id):
            logging.info("Couldn't add edge from node "
                            + str(src_id) + " because such node doesn't exist")
            return None

        if not self.is_node(dest_id):
            logging.info("Couldn't add edge to node "
                            + str(dest_id) + " because such node doesn't exist")
            return None

        src_NId = self.NId_from_id[src_id]
        dest_NId = self.NId_from_id[dest_id]
        return self.network.AddEdge(src_NId, dest_NId, EId)

    def get_edges_between(self, node1, node2):
        # TODO: make this less aweful, maybe using GetInEdges or something.
        # Again, there are important methods missing from SNAP.py
        # The manual says its there, but LIES! ;(
        # For this reason this method is super-funky!
        NId1 = self.NId_from_id[node1]
        NId2 = self.NId_from_id[node2]
        edges = []
        try:
            edge_iterator = self.network.GetEI(NId1, NId2)
            # iterator may 'overflow' to next set o nodes
            if edge_iterator.GetSrcNId() == NId1 and \
                edge_iterator.GetDstNId() == NId2:
                    edges.append(edge_iterator.GetId())
            while edge_iterator.Next():
                # iterator may 'overflow' to next set o nodes
                if edge_iterator.GetSrcNId() == NId1 and \
                    edge_iterator.GetDstNId() == NId2:
                    edges.append(edge_iterator.GetId())
        except RuntimeError:
            return edges

        # in case we get this far...
        return edges

    def get_edge(self, EId):
        return self.network.GetEI(EId)

    def get_nodes(self):
        return list(self.get_node_iterator())

    def get_node_iterator(self):
        """
        this returns a python generator, as evidenced by the yield command
        also, SNAP doesn't give us a decent note iterator, so we have to have
        this funky code instead.
        :return: A generator that traverses nodes, returning their ids.
        """

        if self.get_node_count() == 0:
            # got nothing, return empty iterator
            return
        else:
            node_iterator = self.network.BegNI()
            found_NId = self.id_from_NId[node_iterator.GetId()]
            yield found_NId

            # note, the Nodes() method does not work in SNAP, for some reason...
            # actually, its missing a lot of useful stuff.
            # gotta do it like this:  =(
            while node_iterator.Next():
                try:
                    found_NId = self.id_from_NId[node_iterator.GetId()]
                    yield found_NId
                except RuntimeError:
                    break

    def get_node_attributes(self, node_id):
        """
        :param NId: the node to retrieve attributes from
        :return: a dictionary with 'attr name' - 'attr value' pairs
        """
        NId = self.NId_from_id[node_id]

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

    def get_node_attribute(self, node_id, attr_name):
        # Probably not the most efficient way of doing this...
        attributes = self.get_node_attributes(node_id)

        try:
            return attributes[attr_name]
        except KeyError:
            raise RuntimeError("Node " + str(node_id) + " does not have attribute '" +
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

    def get_neighboring_nodes(self, node_id):
        """

        :param node_id: the id of the node in question.
        :return: all nodes connected by an edge to the node in question.
        """
        NId = self.NId_from_id[node_id]
        nodeI = self.network.GetNI(NId)
        num_neighbours = nodeI.GetOutDeg() + nodeI.GetInDeg()

        neighboring_nodes = []
        for i in xrange(num_neighbours):
            neighboring_nodes.append(self.id_from_NId[nodeI.GetNbrNId(i)])


        return neighboring_nodes

    def add_node_attr(self, node_id, name, value):
        NId = self.NId_from_id[node_id]

        node = self.network.GetNI(NId)
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

    def is_node(self, node_id):
        if node_id in self.NId_from_id:
            NId = self.NId_from_id[node_id]

            # should be always True at this point
            # but its good to have it here for test purposes.
            return self.network.IsNode(NId)
        else:
            return False

    def is_edge(self, edge_id):
        try:
            # if we got an iterator, then edge exists
            self.network.GetEI(edge_id)
            return True
        except RuntimeError:
            # couldn't get iterator means that edge doesn't exist
            return False

    def is_edge_between(self, src_id, dest_id):
        src_NId = self.NId_from_id[src_id]
        dest_NId = self.NId_from_id[dest_id]

        answer =  self.network.IsEdge(src_NId, dest_NId) or \
                  self.network.IsEdge(dest_NId, src_NId)

        return answer

    def save_graph(self, file_path):
        FOut = snap.TFOut(file_path)
        self.network.Save(FOut)
        FOut.Flush()

    def load_graph(self, file_path):
        FIn = snap.TFIn(file_path)
        self.network = snap.TNEANet.Load(FIn)
        return self

    def copy_node(self, node_id, dst_graph):
        """
        :param node_id: id of the node to be copied into dst_graph
        :param dst_graph: a different graph manager object, where
         we will create deep copy a new node, mirroring all characteristics
         of the node with id = node_id, which is present in this object.
        :return whether we suceeded or not.
        """
        # can't copy what doesn't exist
        if not self.is_node(node_id):
            return False

        # can't copy if node already exist at target
        if dst_graph.is_node(node_id):
            return False

        dst_graph.add_node(node_id)
        attr_dictionary = self.get_node_attributes(node_id)
        for attr_name in attr_dictionary:
            attr_value = attr_dictionary[attr_name]
            dst_graph.add_node_attr(node_id, attr_name, attr_value)

        return True

    # noinspection PyMethodMayBeStatic
    def __convert(self, value):
        # TODO: use SNAP verifications instead because it is
        # probably faster.

        try:
            return float(value) if '.' in value else int(value)
        except ValueError:
            return value
