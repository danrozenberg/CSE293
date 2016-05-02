from datetime import datetime
from collections import namedtuple

def get_worker_iterator(association_graph):
    node_iterator = association_graph.get_node_iterator()
    for node in node_iterator:
        node_type = association_graph.get_node_attr(node, "type")
        if node_type == "worker":
            yield node

def connect_workers(association_graph, new_graph):
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


def get_time_together(worker_edge, coworker_edge, graph):
    time_together = 0
    worker_edge_attrs = graph.get_edge_attrs(worker_edge)
    coworker_edge_attrs = graph.get_edge_attrs(coworker_edge)
    for year in xrange(1980, 2030):
        admission_string = str(year) + "_admission_date"
        demission_string = str(year) + "_demission_date"

        if (admission_string in worker_edge_attrs) and \
           (admission_string in coworker_edge_attrs):
            time_together += get_overlapping_days(
                worker_edge_attrs[admission_string],
                worker_edge_attrs[demission_string],
                coworker_edge_attrs[admission_string],
                coworker_edge_attrs[demission_string])
    return time_together


class SimpleConnector(object):
    """ Connects workers if they have worked together
    at any point in time.
    """

    def connect_workers(self, association_graph, new_graph):
        # we get a special worker iterator
        for worker in get_worker_iterator(association_graph):

            # add this worker to the new graph, if necessary
            association_graph.copy_node(worker, new_graph)

            # In an association graph, we can get the employers just by
            # following the edges from worker and retrieving the neighbors.
            employer_nodes = association_graph.get_neighboring_nodes(worker)

            for employer in employer_nodes:
                worker_edge = association_graph.get_edge_between(worker, employer)

                for coworker in association_graph.get_neighboring_nodes(employer):
                    # skip if worker and coworkers are the same

                    if worker == coworker:
                        continue

                    coworker_edge = association_graph.get_edge_between(coworker, employer)

                    if worker == 2 and  coworker == 9:
                        print coworker_edge
                        print self.should_connect(worker_edge, coworker_edge, association_graph)

                    if self.should_connect(worker_edge, coworker_edge, association_graph):
                        new_graph.add_node(coworker)

                        if not new_graph.is_edge_between(worker, coworker):
                            new_graph.add_edge(worker, coworker)
                            # TODO: maybe put time together in the attr?
                            # TODO: maybe put in some other attrs?

        return new_graph



    def should_connect(self, worker_edge, coworker_edge, graph):
        """
        :param worker_edge: id edge connecting worker to employee
        :param coworker_edge: id of edge connecting employee to a
            different worker.
        :return: wether we should add an edge between them.
        """

        time_together = get_time_together(worker_edge,
                                               coworker_edge,
                                               graph)
        return time_together > 1


def get_overlapping_days(start_1, end_1, start_2, end_2):
    # from https://stackoverflow.com/questions/9044084/efficient-date-range-overlap-calculation-in-python

    # convert to datetime if we received timestamp
    # also, assumes all parameters are in the same format
    if type(start_1) == float:
        fromtimestamp = datetime.fromtimestamp
        start_1 = fromtimestamp(start_1)
        end_1 = fromtimestamp(end_1)
        start_2 = fromtimestamp(start_2)
        end_2 = fromtimestamp(end_2)

    Range = namedtuple('Range', ['start', 'end'])
    r1 = Range(start=start_1, end=end_1)
    r2 = Range(start=start_2, end=end_2)
    latest_start = max(r1.start, r2.start)
    earliest_end = min(r1.end, r2.end)
    return max((earliest_end - latest_start).days + 1, 0)




