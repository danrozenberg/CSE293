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

    def quick_add_edge(self, id_1, id_2, EId=-1):
        # only do it if you knoe nodes exist, edge doesnt exist
        NId1 = self.NId_from_id[id_1]
        NId2 = self.NId_from_id[id_2]
        return self.network.AddEdge(NId1, NId2, EId)

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

        if not self.is_edge_between(id_1, id_2):
            new_edge = self.network.AddEdge(NId1, NId2, EId)
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

    def fast_get_edge_between(self, src, target):
        # assumes that edge exist with correct orientation
        # remember, currently src is worker!
        NId1 = self.NId_from_id[src]
        NId2 = self.NId_from_id[target]
        return self.network.GetEI(NId1, NId2).GetId()

    def get_edge_between(self, node1, node2):
        """This only returns the FIRST edge ever added between
        node 1 and node 2"""
        NId1 = self.NId_from_id[node1]
        NId2 = self.NId_from_id[node2]

        if self.network.IsEdge(NId1, NId2):
            return self.network.GetEI(NId1, NId2).GetId()
        elif self.network.IsEdge(NId2, NId1):
            return self.network.GetEI(NId2, NId1).GetId()
        else:
            return None

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
        self.network.AttrNameNI(NId, names)
        self.network.AttrValueNI(NId, values)
        converted_values = [self.__convert(value) for value in values]
        return dict(zip(names, converted_values))

    def get_fast_node_attrs(self, node_id):
        """
        :param NId: the node to retrieve attributes from
        :return: a dictionary with 'attr name' - 'attr value' pairs
        """
        NId = self.NId_from_id[node_id]
        names = snap.TStrV()
        values = snap.TFltV()
        self.network.FltAttrNameNI(NId, names)
        self.network.FltAttrValueNI(NId, values)
        converted_values = [float(x) for x in values]
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
        names = snap.TStrV()
        values = snap.TStrV()
        self.network.AttrNameEI(EId, names)
        self.network.AttrValueEI(EId, values)
        converted_values = [self.__convert(value) for value in values]

        return dict(zip(names, converted_values))

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
        if isinstance(value, float) or isinstance(value, int):
            self.network.AddFltAttrDatN(node, value, name)
        elif isinstance(value, str):
            self.network.AddStrAttrDatN(node, value, name)
        else:
            raise Exception('Invalid data type')

    def add_edge_attr(self, EId, name, value):

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

        return self.network.IsEdge(src_NId, dest_NId) or \
         self.network.IsEdge(dest_NId, src_NId)

    def save_graph(self, file_path):
        FOut = snap.TFOut(file_path)
        self.network.Save(FOut)
        FOut.Flush()

        # save dictionaries too!
        pickle.dump(self.NId_from_id,
                    open(file_path.replace(".graph", "_nid_from_id.p"), 'wb'))
        pickle.dump(self.id_from_NId,
                    open(file_path.replace(".graph", "_id_from_nid.p"), 'wb'))

    def load_graph(self, file_path, graph_type=snap.TNEANet):
        FIn = snap.TFIn(file_path)

        self.network = graph_type.Load(FIn)

        # grab dictionaries too!
        self.NId_from_id =\
            pickle.load(open(file_path.replace(".graph", "_nid_from_id.p"), 'rb'))

        self.id_from_NId =\
            pickle.load(open(file_path.replace(".graph", "_id_from_nid.p"), 'rb'))



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

    def get_possible_coworkers(self, src_id):
        # just make sure src_id as worker node, ok?
        NId = self.NId_from_id[src_id]
        NodeVec = snap.TIntV()
        snap.GetNodesAtHop(self.network, NId, 2, NodeVec, False)

        return [self.id_from_NId[x] for x in NodeVec]

    def get_connected(self, src_id):
        # return connected by 1 edge
        NId = self.NId_from_id[src_id]
        NodeVec = snap.TIntV()
        snap.GetNodesAtHop(self.network, NId, 1, NodeVec, False)

        return  [self.id_from_NId[x] for x in NodeVec]

    def get_employees(self, src_id):
        # just make sure src_id as employer node, ok?
        NId = self.NId_from_id[src_id]
        NodeVec = snap.TIntV()
        snap.GetNodesAtHop(self.network, NId, 1, NodeVec, False)

        return  [self.id_from_NId[x] for x in NodeVec]

    def get_employers(self, src_id):
        # just make sure src_id as worker node, ok?
        NId = self.NId_from_id[src_id]
        NodeVec = snap.TIntV()
        snap.GetNodesAtHop(self.network, NId, 1, NodeVec, False)

        return  [self.id_from_NId[x] for x in NodeVec]

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
