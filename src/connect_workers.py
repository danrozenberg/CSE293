import datetime
from collections import namedtuple
import logging
from graph_manager import SnapManager

def get_worker_iterator(association_graph):
    node_iterator = association_graph.get_node_iterator()
    for node in node_iterator:
        node_type = association_graph.get_node_attr(node, "type")
        if node_type == "worker":
            yield node

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

def get_overlapping_days(start_1, end_1, start_2, end_2):
    # from https://stackoverflow.com/questions/9044084/efficient-date-range-overlap-calculation-in-python

    # convert to datetime if we received timestamp
    # also, assumes all parameters are in the same format
    if type(start_1) == float:
        # windows cant handle negative timestamps..
        start_1 = from_timestamp(start_1)
        end_1 = from_timestamp(end_1)
        start_2 = from_timestamp(start_2)
        end_2 = from_timestamp(end_2)

    Range = namedtuple('Range', ['start', 'end'])
    r1 = Range(start=start_1, end=end_1)
    r2 = Range(start=start_2, end=end_2)
    latest_start = max(r1.start, r2.start)
    earliest_end = min(r1.end, r2.end)
    return max((earliest_end - latest_start).days + 1, 0)

def from_timestamp(timestamp):
    # windows cant handle negative timestamps
    # we need to do this weird conversion here...
    # there is some gmt weirdness python introduces...
    # no big deal, though...
    converted = datetime.datetime(1970,1,1) + \
        datetime.timedelta(seconds=(timestamp))
    return converted


class WorkerConnector(object):
    def __init__(self):
        # defaults allows workers with 0 days in common to be connected.
        self.min_days_together = -1

        # TODO: min_firm_size  ?
        # TODO: remove state owned firms?

    def connect_workers(self, affiliation_graph, new_graph):
        # we get a special worker iterator
        for worker in get_worker_iterator(affiliation_graph):
            # add this worker to the new graph, if necessary
            logging.warn("Processing worker " + str(worker))
            affiliation_graph.copy_node(worker, new_graph)

            # In an association graph, we can get the employers just by
            # following the edges from worker and retrieving the neighbors.
            employer_nodes = affiliation_graph.get_neighboring_nodes(worker)

            for employer in employer_nodes:
                worker_edge = affiliation_graph.get_edge_between(worker, employer)

                for coworker in affiliation_graph.get_neighboring_nodes(employer):

                    # no need to connect someone with oneself...
                    if worker == coworker:
                        continue

                    coworker_edge = affiliation_graph.get_edge_between(coworker, employer)

                    if self.should_connect(worker_edge, coworker_edge, affiliation_graph):
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

        # add more checks here, as needed.
        return time_together >= self.min_days_together

def enable_logging(log_level):
    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=log_level)

def run_script(load_path, save_path, min_days):
    # load affiliation
    affiliation_graph = SnapManager()
    logging.warn("Beggining to load graph...")
    affiliation_graph.load_graph(load_path)
    logging.warn("Loaded!")

    # connect workers
    connected_graph = SnapManager()
    connector = WorkerConnector()
    connector.min_days_together = min_days
    connector.connect_workers(affiliation_graph, connected_graph)

    # save it
    connected_graph.save_graph(save_path)

if __name__ == '__main__':
    enable_logging(logging.WARNING)
    load_path = "../output_graphs/rs_affiliation.graph"
    save_path = "../output_graphs/rs_connected.graph"
    min_days = 182
    logging.warn("Started!")
    run_script(load_path, save_path, min_days)
    logging.warn("Finished!")
