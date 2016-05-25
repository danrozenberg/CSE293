import cPickle as pickle
import snap
import logging
from collections import defaultdict

class SnapManager(object):
    """ This implementation deals with SNAP networks. """

    def __init__(self):
        """The following dictionaries basically transform a CEI or CNJP into
        values that fit a C++ integers. We want CEI and CNPJ to be
        our IDs, but their values do not fit SNAP integers which are
        used as node ids :(
        This is the reason you can see the use of the following dictionaries:"""
        self.network = snap.TNEANet().New()
        self.NId_from_id = {}
        self.id_from_NId = {}
        self.edge_from_tuple = defaultdict(_get_defaults_dict)
        self.attrs_from_edge = defaultdict(_get_defaults_dict)

    def add_node(self, node_id):
        """
        Adds a node to the network, if it doesn't exist yet.
        Note that: Accessing arbitrary node takes constant time.
        Note that: Adding a node takes constant time.
        Note: id is a c++ (32 bit) integer, so be careful not to overflow.
        """
        if not self.is_node(node_id):
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

    def add_edge(self, id_1, id_2, EId=-1):

        if not self.is_node(id_1):
            logging.info("Couldn't add edge from node "
                            + str(id_1) + " because such node doesn't exist")
            return None

        if not self.is_node(id_2):
            logging.info("Couldn't add edge to node "
                            + str(id_2) + " because such node doesn't exist")
            return None

        NId1 = self.NId_from_id[id_1]
        NId2 = self.NId_from_id[id_2]
        # we only add (x, y) edges where x <= y
        if NId2 < NId1:
            temp = NId1
            NId1 = NId2
            NId2 = temp

        found_edge = self.edge_from_tuple[(NId1, NId2)]
        if found_edge is not None:
            return found_edge
        else:
            new_edge = self.network.AddEdge(NId1, NId2, EId)
            self.edge_from_tuple[(NId1, NId2)] = new_edge
            return new_edge

    def get_edges(self, node_id):
        """
        Note that this only returns the first edge ever added between
        node and its neighbors. So this fails if there is more than
        1 edge between 2 nodes... This is a current SNAP limitation.
        :param node_id: the node in question
        :return: all ids of all edges connected to the node in question.
        """
        NId = self.NId_from_id[node_id]
        nodeI = self.network.GetNI(NId)
        edges = set()

        for x in xrange(nodeI.GetOutDeg()):
            neighbor = nodeI.GetOutNId(x)
            edges.add(self.network.GetEI(NId, neighbor).GetId())

        for x in xrange(nodeI.GetInDeg()):
            neighbor = nodeI.GetInNId(x)
            edges.add(self.network.GetEI(neighbor, NId).GetId())

        return list(edges)

    def get_edge_between(self, node1, node2):
        """This only returns the FIRST edge ever added between
        node 1 and node 2"""
        NId1 = self.NId_from_id[node1]
        NId2 = self.NId_from_id[node2]

        if NId1 < NId2:
            return self.edge_from_tuple[NId1, NId2]
        else:
            return self.edge_from_tuple[NId2, NId1]

    def get_nodes(self):
        return list(self.get_node_iterator())

    def get_node_iterator(self):
        """
        this returns a python generator, as evidenced by the yield command
        also, SNAP doesn't give us a decent note iterator, so we have to have
        this funky code instead.
        :return: A generator that traverses nodes, returning their ids.
        """

        # There is yet another SNAP bug here to beware
        # If we load a saved TNEATNet (from disk)
        # it will mess up the itarator (goes out of bounds)
        # we can control it with this variable
        total_to_find = self.get_node_count()

        if self.get_node_count() == 0:
            # got nothing, return empty iterator
            return
        else:
            node_iterator = self.network.BegNI()
            found_id = self.id_from_NId[node_iterator.GetId()]
            total_found = 1
            yield found_id

            # note, the Nodes() method does not work in SNAP, for some reason...
            # so gotta do it like this:  =(
            while node_iterator.Next() and total_found < total_to_find:
                try:
                    found_id = self.id_from_NId[node_iterator.GetId()]
                    yield found_id
                    total_found += 1
                except RuntimeError:
                    break

    def get_node_attrs(self, node_id):
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

    def get_node_attr(self, node_id, attr_name):

        NId = self.NId_from_id[node_id]
        names = snap.TStrV()
        values = snap.TStrV()
        self.network.AttrNameNI(NId, names)

        index = -1
        for i in xrange(len(names)):
            if names[i] == attr_name:
                index = i
                break

        # if we found anything..
        if index == -1:
            raise RuntimeError("Node does not have attribute" + attr_name )
        else:
            self.network.AttrValueNI(NId, values)
            return self.__convert(values[index])

    def get_edge_attrs(self, EId):
        """
        :param EId: the edge to retrieve attributes from
        :return: a dictionary with 'attr name' - 'attr value' pairs
        """
        if self.attrs_from_edge[EId] is None:
            names = snap.TStrV()
            values = snap.TStrV()
            converted_values = []
            self.network.AttrNameEI(EId, names)
            self.network.AttrValueEI(EId, values)

            for value in values:
                # Due to a SNAP bug we are forced to convert attributes
                #   back to their original type ourselves ;(
                converted_values.append(self.__convert(value))

            self.attrs_from_edge[EId] = dict(zip(names, converted_values))

        return self.attrs_from_edge[EId]

    def get_edge_attr(self, EId, attr_name):

        names = snap.TStrV()
        values = snap.TStrV()
        self.network.AttrNameEI(EId, names)

        index = -1
        for i in xrange(len(names)):
            if names[i] == attr_name:
                index = i
                break

        # if we found anything..
        if index == -1:
            raise RuntimeError("Edge does not have attribute" + attr_name )
        else:
            self.network.AttrValueEI(EId, values)
            return self.__convert(values[index])

    def get_node_count(self):
        return self.network.GetNodes()

    def get_edge_count(self):
        return self.network.GetEdges()

    def get_neighboring_nodes(self, node_id):
        """

        :param node_id: the id of the node in question.
        :return: set of nodes connected by an edge to the node in question.
        """
        NId = self.NId_from_id[node_id]
        nodeI = self.network.GetNI(NId)
        num_neighbours = nodeI.GetOutDeg() + nodeI.GetInDeg()

        neighboring_nodes = set()
        for i in xrange(num_neighbours):
            neighboring_nodes.add(self.id_from_NId[nodeI.GetNbrNId(i)])


        return list(neighboring_nodes)

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

        # reset anything we knew about edge attrs
        self.attrs_from_edge[EId] = None

        edge = self.network.GetEI(EId)
        if isinstance(value, int):
            self.network.AddIntAttrDatE(edge, value, name)
        elif isinstance(value, float):
            self.network.AddFltAttrDatE(edge, value, name)
        elif isinstance(value, str):
            self.network.AddStrAttrDatE(edge, value, name)
        else:
            raise Exception('Invalid data type')

    def is_node(self, node_id):
        # this will give bad results if we mess with self.NId_from_id
        # from outside.
        return node_id in self.NId_from_id

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

        # direction 1
        edge1 = self.edge_from_tuple[src_NId, dest_NId]

        # direction 2
        edge2 = self.edge_from_tuple[dest_NId, src_NId]

        return (edge1 is not None) or (edge2 is not None)

    def save_graph(self, file_path):
        FOut = snap.TFOut(file_path)
        self.network.Save(FOut)
        FOut.Flush()

        # save dictionaries too!
        pickle.dump(self.NId_from_id,
                    open(file_path.replace(".graph", "_nid_from_id.p"), 'wb'))
        pickle.dump(self.id_from_NId,
                    open(file_path.replace(".graph", "_id_from_nid.p"), 'wb'))
        pickle.dump(self.edge_from_tuple,
                    open(file_path.replace(".graph", "_edge_from_tuple.p"), 'wb'))
        pickle.dump(self.attrs_from_edge,
                    open(file_path.replace(".graph", "_attrs_from_edge.p"), 'wb'))

    def load_graph(self, file_path, graph_type=snap.TNEANet):
        FIn = snap.TFIn(file_path)

        self.network = graph_type.Load(FIn)

        # grab dictionaries too!
        self.NId_from_id =\
            pickle.load(open(file_path.replace(".graph", "_nid_from_id.p"), 'rb'))

        self.id_from_NId =\
            pickle.load(open(file_path.replace(".graph", "_id_from_nid.p"), 'rb'))

        self.edge_from_tuple = \
            pickle.load(open(file_path.replace(".graph", "_edge_from_tuple.p"), 'rb'))

        self.attrs_from_edge = \
            pickle.load(open(file_path.replace(".graph", "_attrs_from_edge.p"), 'rb'))

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
        attr_dictionary = self.get_node_attrs(node_id)
        for attr_name in attr_dictionary:
            attr_value = attr_dictionary[attr_name]
            dst_graph.add_node_attr(node_id, attr_name, attr_value)
        return True

    def get_random_node(self):
        NId = self.network.GetRndNId()
        return self.id_from_NId[NId]

    def generate_random_graph(self, node_num, node_out_deg, rewire_prob):
        # this substitutes the old graph, so beware
        # from: https://snap.stanford.edu/snappy/doc/reference/GenSmallWorld.html?highlight=generate%20random%20network
        self.network = snap.GenSmallWorld(node_num,
                                          node_out_deg,
                                          rewire_prob,
                                          snap.TRnd(1,0))
        # gotta jig the dictionary, unfortunatelly
        self.jig_dictionary()

    # GRAPH PATH LENGHTS
    def get_shortest_path_size(self, node_id):
        """Returns the length of the shortest path from node SrcNId to
         all other nodes in the network."""
        NId = self.NId_from_id[node_id]

        NIdToDistH = snap.TIntH() #saves all distanecs FROM *NId*
        return snap.GetShortPath(self.network,
                                NId,
                                NIdToDistH)

    # NODE CENTRALITY
    def get_degree_centrality(self, node_id):
        """Returns degree centrality of a given node NId in Graph.
        Degree centrality of a node is defined as its degree/(N-1),
        where N is the number of nodes in the network."""

        NId = self.NId_from_id[node_id]
        return snap.GetDegreeCentr(self.network, NId)

    def get_eccentricity(self, node_id):
        NId = self.NId_from_id[node_id]
        return snap.GetNodeEcc(self.network,NId)

    def get_betweeness_centrality(self, fraction=1.0):
        """Computes (approximate) Node and Edge Betweenness Centrality based
        on a sample of NodeFrac nodes.
        It does so for all all nodes in the graph. Returns a HashMap"""
        Nodes = snap.TIntFltH()
        Edges = snap.TIntPrFltH()
        snap.GetBetweennessCentr(self.network, Nodes, Edges, fraction)
        return Nodes

    def get_eigenvector_centrallity(self):
        """  :return: hash from Id to centrality """
        NIdEigenH = snap.TIntFltH()
        snap.GetEigenVectorCentr(self.network, NIdEigenH)
        return NIdEigenH

    # CONNECTED COMPONENTS
    def get_connected_components(self):
        """Returns all weakly connected components in Graph.
        :returns list of component sizes [s1, s2, ... , sn]"""
        components = snap.TCnComV()
        snap.GetWccs(self.network, components)
        sizes = []
        for component in components:
            sizes.append(component.Len())
        return sizes

    # TRIADS AND CLUSTERING
    def get_clustering_coefficient(self):
        """Computes the average clustering coefficient as
        defined in  Watts and Strogatz, Collective dynamics of
        small-world networks"""
        return snap.GetClustCf(self.network, -1)

    # Print general graph information
    def print_info(self, file_path, description):
        snap.PrintInfo(self.network,
                       description,
                       file_path,
                       False)

    def jig_dictionary(self, NId_from_id=None, id_from_NId=None ):
        # TODO: apologize for having to do this.

        node_num = self.get_node_count()
        self.id_from_NId = {}
        self.NId_from_id = {}

        #=====NID from ID====
        if NId_from_id is not None:
            self.NId_from_id = NId_from_id
        else:
            for x in xrange(node_num):
                self.NId_from_id[x] = x

        #=====ID from NID====
        if id_from_NId is not None:
            self.id_from_NId = id_from_NId
        else:
            for x in xrange(node_num):
                self.id_from_NId[x] = x

    # noinspection PyMethodMayBeStatic
    def __convert(self, value):
        # TODO: use SNAP verifications instead because it is
        # probably faster.

        try:
            return float(value) if '.' in value else int(value)
        except ValueError:
            return value

# so we can pickle default dict
def _get_defaults_dict():
    return None
