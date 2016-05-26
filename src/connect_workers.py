import logging
import sys
from graph_manager import SnapManager

def get_worker_iterator(affiliation_graph):
    node_iterator = affiliation_graph.get_node_iterator()
    for node in node_iterator:
        node_type = affiliation_graph.get_node_attr(node, "type")
        if node_type == "worker":
            yield node

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

        # final year: this represents the year (including it up to Dec-31st)...
        # We will ignore any edges that represents date after that
        # as such, we can create a snapshot at the end of that year.
        self.max_year = 2016
        self.proc_num = sys.argv[1]

    def start_connect_worker_proc(self):
        """ This is what gets called by Popen, so it is the
         entry point of processing, if called by the script start.py
        """
        # year from argvs
        year = int(sys.argv[2])
        self.max_year = year
        graph_load_path = "../output_graphs/cds_affiliation.graph"

        # load affiliation
        affiliation_graph = SnapManager()
        logging.warn("proc " + self.proc_num + ": Beginning to load graph...")
        affiliation_graph.load_graph(graph_load_path)
        logging.warn("proc " + self.proc_num + ": Affiliations graph loaded!")

        # Get this process' work load
        connected_graph = SnapManager()
        self.connect_workers(affiliation_graph, connected_graph)
        logging.warn("proc " + self.proc_num + ": Finished!")

        # save connected graph
        graph_save_path = "../output_graphs/cds_connected_" + str(year) + ".graph"
        connected_graph.save_graph(graph_save_path)

    def connect_workers(self, affiliation_graph, new_graph):

        # get method addresses for performance reasonts
        get_node_attrs = affiliation_graph.get_node_attrs
        should_connect = self.should_connect

        progress_counter = -1
        for worker in get_worker_iterator(affiliation_graph):

            # log every once in a while
            progress_counter += 1
            if progress_counter % 1000 == 0:
                logging.warn("proc " + self.proc_num +
                ": processed " + str(progress_counter) +
                " workers.")

            # in the future we might copy if needed instead
            new_graph.add_node(worker)
            currently_connected = new_graph.get_connected(worker)
            worker_attrs = get_node_attrs(worker)

            # In an affiliation graph, we can get the employers just by
            # following the edges from worker and retrieving the neighbors.
            for employer in affiliation_graph.get_employers(worker):
                for coworker in affiliation_graph.get_employees(employer):
                    # no need to connect with someone already connected...
                    if worker == coworker:
                        continue
                    if coworker in currently_connected:
                        continue
                    coworker_attrs = get_node_attrs(coworker)

                    if should_connect(worker_attrs, coworker_attrs):
                        new_graph.add_node(coworker)
                        new_graph.quick_add_edge(worker, coworker)
                        # TODO: maybe put time together in the attr?
                        # TODO: maybe put in some other attrs?


        return new_graph

    def should_connect(self, worker_node_attrs, coworker_node_attr):
        # although less general, receiving attributes as parameters
        # allows us to call get_edge_attrs almost half the number of times...
        # TODO, make worker_edge attrs a class property or something...

        time_together = self.get_time_together(worker_node_attrs,
                                               coworker_node_attr,
                                               self.min_days_together)

        # add more checks here, as needed.
        return time_together >= self.min_days_together

    def get_time_together(self, worker_attrs, coworker_attrs, min_days = None):
        # although less general, receiving attributes as parameters
        # allows us to call get_edge_attrs almost half the number of times...
        time_together = 0
        max_year = self.max_year

        removal_set = set()
        removal_set.add("type")
        worker_set = set(worker_attrs.keys())
        coworker_set = set(coworker_attrs.keys())
        valid_set = sorted((worker_set & coworker_set)-removal_set)

        i = 0
        while i < (len(valid_set)):
            year = int(valid_set[i][:4])
            if year > max_year:
                i += 2
                continue
            worker_admission = worker_attrs[valid_set[i]]
            coworker_admission = coworker_attrs[valid_set[i]]
            worker_demission = worker_attrs[valid_set[i+1]]
            coworker_demission = coworker_attrs[valid_set[i+1]]

            i += 2

            latest_start = max(worker_admission,
                               coworker_admission)
            earliest_end = min(worker_demission,
                               coworker_demission)

            # timestamps
            time_together += round(max(((earliest_end - latest_start)/60/60/24) + 1, 0))

            # we can stop if we were given a min_days, and
            # if that min time has been reached
            if min_days is not None and time_together > min_days:
                return time_together

        return time_together

def enable_logging(log_level):
    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=log_level)

if __name__ == '__main__':
    enable_logging(logging.WARNING)
    connector = WorkerConnector()
    connector.min_days_together = 90
    connector.start_connect_worker_proc()

