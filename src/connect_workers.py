def get_worker_iterator(association_graph):
    node_iterator = association_graph.get_node_iterator()
    for node in node_iterator:
        node_type = association_graph.get_node_attribute(node, "type")
        if node_type == "worker":
            yield node

def connect_workers(association_graph, new_graph):
    """
    Connects workers as long as they have worked in the same company
      at any given point, including if they never worked together
      during the same period of time. This is the most simple way to
      connect the workers.

    :param association_graph: an association graph between workers-plants.
    :param graph_manager: a graph manager that we will use to create a
     new graph.
    :return: the new graph, with only workers and edges between them.
    """

    # we get a special worker iterator
    for worker in get_worker_iterator(association_graph):

        # add this worker to the new graph
        association_graph.copy_node(worker, new_graph)

        # In an association graph, we can get the employers just by
        # following the edges from worker and retrieving the neighbors.
        employer_nodes = association_graph.get_neighboring_nodes(worker)

        # We don't need to consider the worker node or its edges again.
        #  so we could delete it...
        # Its kind of a bad vibe to delete the node inside a node iterator
        #   just make sure that iterator can deal with it and that
        #   when you delete a node the manager will delete the edges..
        association_graph.delete_node(worker)

        for employer in employer_nodes:
            for coworker in association_graph.get_neighboring_nodes(employer):
                new_graph.add_node(coworker)

                # don't want repeated edges. This could happen if
                # worker and coworker worked together in 2 different plants.
                if not new_graph.is_edge_between(worker, coworker):
                    new_graph.add_edge(worker, coworker)

    return new_graph



