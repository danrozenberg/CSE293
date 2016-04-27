from datetime import datetime
from collections import namedtuple

def get_worker_iterator(association_graph):
    node_iterator = association_graph.get_node_iterator()
    for node in node_iterator:
        node_type = association_graph.get_node_attribute(node, "type")
        if node_type == "worker":
            yield node

def connect_workers(association_graph, new_graph, connector):
    """
    Connects workers as long as they have worked in the same company
      at any given point, including if they never worked together
      during the same period of time. This is the most simple way to
      connect the workers.

    :param association_graph: an association graph between workers-plants.
    We expect this association graph to have ONLY ONE EDGE BETWEEN ANY
     PAIR OF NODES.

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

def get_overlapping_time(start_1, end_1, start_2, end_2):
    # from https://stackoverflow.com/questions/9044084/efficient-date-range-overlap-calculation-in-python
    Range = namedtuple('Range', ['start', 'end'])
    r1 = Range(start=start_1, end=end_1)
    r2 = Range(start=start_2, end=end_2)
    latest_start = max(r1.start, r2.start)
    earliest_end = min(r1.end, r2.end)
    return max((earliest_end - latest_start).days + 1, 0)

class WorkerConnector():
    def should_connect(self, worker, coworker):
        raise NotImplementedError


class SimpleConnector(WorkerConnector):
    """ Connects workers if they have worked together
    at any point in time.
    """
    def __init__(self, graph):
        self.graph = graph

    def should_connect(self, worker, coworker):
        """
        :param worker: id of a worker
        :param coworker: id of another worker
        :return: wether we should add an edge between them.
        """
        pass



    def _get_time_at_worker_attrs(self, node_id):
        pass







