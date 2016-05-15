import datetime
import logging
from graph_manager import SnapManager

def get_worker_iterator(affiliation_graph):
    node_iterator = affiliation_graph.get_node_iterator()
    for node in node_iterator:
        node_type = affiliation_graph.get_node_attr(node, "type")
        if node_type == "worker":
            yield node


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

    latest_start = max(start_1, start_2)
    earliest_end = min(end_1, end_2)
    return max((earliest_end - latest_start).days + 1, 0)

def from_timestamp(timestamp):
    # windows cant handle negative timestamps
    # we need to do this weird conversion here...
    # there is some gmt weirdness python introduces...
    # no big deal, though...
    converted = datetime.datetime(1970,1,1) + \
        datetime.timedelta(seconds=timestamp)
    return converted


def should_skip(worker, coworker, new_graph):
    # no need to connect someone with oneself...
    if worker == coworker:
        return True

    # no need to connect with someone already connected...
    if new_graph.is_node(coworker) and \
        new_graph.is_edge_between(worker, coworker):
        return True

    return  False


class WorkerConnector(object):
    def __init__(self):
        # defaults allows workers with 0 days in common to be connected.
        self.min_days_together = -1

        # TODO: min_firm_size  ?
        # TODO: remove state owned firms?

        # To avoid concatenating strings too many times, we take a shortcut here
        # this will be used in the get_time_together method
        # TODO: extract this sometime.
        self.admission_strings = []
        self.demission_strings = []
        for year in xrange(0, 2030):
            self.admission_strings.append(str(year) + "_admission_date")
            self.demission_strings.append(str(year) + "_demission_date")

    def connect_workers(self, affiliation_graph, new_graph):

        progress_counter = -1
        for worker in get_worker_iterator(affiliation_graph):

            # log every once in a while
            progress_counter += 1
            if progress_counter % 1000 == 0:
                logging.warn("Processeced " + str(progress_counter) + " workers.")

            # add this worker to the new graph, if necessary
            affiliation_graph.copy_node(worker, new_graph)

            # In an affiliation graph, we can get the employers just by
            # following the edges from worker and retrieving the neighbors.
            employer_nodes = affiliation_graph.get_neighboring_nodes(worker)

            for employer in employer_nodes:
                worker_edge = affiliation_graph.get_edge_between(worker, employer)
                worker_edge_attrs = affiliation_graph.get_edge_attrs(worker_edge)

                for coworker in affiliation_graph.get_neighboring_nodes(employer):

                    # sometimes, we can just skip a step in the algorithm...
                    if should_skip(worker, coworker, new_graph):
                        continue

                    coworker_edge = affiliation_graph.get_edge_between(coworker, employer)
                    coworker_edge_attrs = affiliation_graph.get_edge_attrs(coworker_edge)

                    if self.should_connect(worker_edge_attrs, coworker_edge_attrs):
                        new_graph.add_node(coworker)
                        new_graph.add_edge(worker, coworker)
                        # TODO: maybe put time together in the attr?
                        # TODO: maybe put in some other attrs?

        return new_graph

    def should_connect(self, worker_edge_attrs, coworker_edge_attrs):
        # although less general, receiving attributes as parameters
        # allows us to call get_edge_attrs almost half the number of times...
        # TODO, make worker_edge attrs a class property or something...

        time_together = self.get_time_together(worker_edge_attrs, coworker_edge_attrs)

        # add more checks here, as needed.
        return time_together >= self.min_days_together

    def get_time_together(self, worker_edge_attrs, coworker_edge_attrs):
        # although less general, receiving attributes as parameters
        # allows us to call get_edge_attrs almost half the number of times...
        time_together = 0

        # we did some string concatenation in the class init method
        # lets reference to it here.
        admission_strings = self.admission_strings
        demission_strings = self.demission_strings

        for year in xrange(1980, 2016):

            if (admission_strings[year] in worker_edge_attrs) and \
               (admission_strings[year] in coworker_edge_attrs):
                time_together += get_overlapping_days(
                    worker_edge_attrs[admission_strings[year]],
                    worker_edge_attrs[demission_strings[year]],
                    coworker_edge_attrs[admission_strings[year]],
                    coworker_edge_attrs[demission_strings[year]])
        return time_together

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
    min_days = 182
    load_path = "../output_graphs/rs_affiliation.graph"
    save_path = "../output_graphs/rs_connected" + str(min_days) + "_days.graph"
    logging.warn("Started!")
    run_script(load_path, save_path, min_days)
    logging.warn("Finished!")
