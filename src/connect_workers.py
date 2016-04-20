def is_valid_node(node):
    raise NotImplementedError


def get_worker_iterator(association_graph):
    node_iterator = association_graph.get_node_iterator()
    for node in node_iterator:
        node_type = association_graph.get_node_attribute(node, "type")
        if node_type == "worker":
            yield node

def get_employers(worker):
    raise NotImplementedError


def remove_worker_edges(employer_out_edges, worker):
    raise NotImplementedError


def get_employer_out_edges(employer):
    raise NotImplementedError


def delete_edges_from_node(worker):
    raise NotImplementedError


def connect_workers(association_graph, graph_manager):
    """
    :param raw_graph: an association graph between workers-employers.
    :return: a new graph, created with the graph manager
    """
    new_graph = graph_manager()

    # we get a special worker iterator
    worker_iterator = get_worker_iterator(association_graph)

    # should only consider 'worker' nodes
    for worker in worker_iterator:

        # if it passes the filter
        if is_valid_node(worker):

            # should be easy to get companies, since its an association graph.
            worker_out_edges = association_graph.get_out_edges(worker)
            employer_nodes = get_employers(worker)

            # get all edges coming out of the employer nodes
            employer_out_edges = []
            for employer in employer_nodes:
                employer_out_edges.append(get_employer_out_edges(employer))

            # remove all edges that refer to the worker
            remove_worker_edges(employer_out_edges, worker)

            # we don't need to consider the worker node or its edges again
            # kind of a bad vibe to delete the node inside a node iterator
            # let's delete only the edges, which should work just as fine.
            delete_edges_from_node(worker)
    return new_graph



