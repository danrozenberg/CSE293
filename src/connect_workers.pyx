import logging
from graph_manager import SnapManager


cdef extern from "math.h" nogil:
    double round(double x)
    double fmax(double x, double y)
    double fmin(double x, double y)
    double fmod(double x, double y)


def get_worker_iterator(affiliation_graph):
    node_iterator = affiliation_graph.get_node_iterator()
    for node in node_iterator:
        node_type = affiliation_graph.get_node_attr(node, "type")
        if node_type == "worker":
            yield node

class WorkerConnector(object):
    def __init__(self, max_year = 2016):
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
        self.max_year = max_year

        for year in xrange(0, self.max_year+1):
            self.admission_strings.append(str(year) + "_admission_date")
            self.demission_strings.append(str(year) + "_demission_date")

    def start_connect_worker_proc(self):
        """ This is what gets called by Popen, so it is the
         entry point of processing, if called by the script start.py
        """
        # year from argvs
        graph_load_path = "../output_graphs/test_affiliation.graph"

        # load affiliation
        affiliation_graph = SnapManager()
        logging.warn("proc " + str(self.max_year) + ": Beginning to load graph...")
        affiliation_graph.load_graph(graph_load_path)
        logging.warn("proc " + str(self.max_year )+ ": Affiliations graph loaded!")

        # Get this process' work load
        connected_graph = SnapManager()
        self.connect_workers(affiliation_graph, connected_graph)
        logging.warn("proc " + str(self.max_year) + ": Finished!")

        # save connected graph
        graph_save_path = "../output_graphs/test_connected_" + \
                          str(self.max_year) + ".graph"
        connected_graph.save_graph(graph_save_path)

    def connect_workers(self, affiliation_graph, new_graph):

        # get method addresses for performance reasonts
        get_neighboring_nodes = affiliation_graph.get_neighboring_nodes
        get_edge_between = affiliation_graph.get_edge_between
        get_edge_attrs = affiliation_graph.get_edge_attrs
        attrs_from_edge = affiliation_graph.attrs_from_edge
        should_connect = self.should_connect

        cdef double progress_counter
        cdef long long worker, coworker, employer
        cdef int worker_edge, coworker_edge

        progress_counter = -1
        for worker in get_worker_iterator(affiliation_graph):

            # log every once in a while
            progress_counter += 1
            if fmod(progress_counter,1000) == 0:
                logging.warn("proc " + str(self.max_year) +
                ": processed " + str(progress_counter) +
                " workers.")

            # add this worker to the new graph, if necessary
            affiliation_graph.copy_node(worker, new_graph)

            # In an affiliation graph, we can get the employers just by
            # following the edges from worker and retrieving the neighbors.
            employer_nodes = get_neighboring_nodes(worker)

            for employer in employer_nodes:
                worker_edge = get_edge_between(worker, employer)
                worker_edge_attrs = get_edge_attrs(worker_edge)

                for coworker in get_neighboring_nodes(employer):

                    # should skip1
                    if worker == coworker:
                        continue

                    # should skip2
                    if new_graph.is_node(coworker) and \
                        new_graph.is_edge_between(worker, coworker):
                        continue

                    coworker_edge = get_edge_between(coworker, employer)
                    coworker_edge_attrs = get_edge_attrs(coworker_edge)

                    if should_connect(worker_edge_attrs, coworker_edge_attrs):
                        new_graph.add_node(coworker)
                        new_graph.add_edge(worker, coworker)
                        # TODO: maybe put time together in the attr?
                        # TODO: maybe put in some other attrs?

                # this worker-employer edge will never be examined again
                # and we are running out of memory, so we might as well
                # nuke the worker_edge_attr from memory. We just set it to None..
                del attrs_from_edge[worker_edge]

        return new_graph

    def should_connect(self, worker_edge_attrs, coworker_edge_attrs):
        # although less general, receiving attributes as parameters
        # allows us to call get_edge_attrs almost half the number of times...
        # TODO, make worker_edge attrs a class property or something...

        cdef double time_together
        time_together = self.get_time_together(worker_edge_attrs,
                                               coworker_edge_attrs,
                                               self.min_days_together)

        # add more checks here, as needed.
        return time_together >= self.min_days_together

    def get_time_together(self, worker_edge_attrs, coworker_edge_attrs,min_days = None):
        # although less general, receiving attributes as parameters
        # allows us to call get_edge_attrs almost half the number of times...
        cdef double time_together = 0
        cdef int year = 0
        cdef float latest_start = 0
        cdef float earliest_end = 0
        cdef char* admission_string = ''
        cdef char* demission_string = ''

        # we did some string concatenation in the class init method
        # lets reference to it here.
        admission_strings = self.admission_strings
        demission_strings = self.demission_strings

        for year in xrange(self.max_year, 1980, -1):
            admission_string = admission_strings[year]
            demission_string = demission_strings[year]
            if (admission_string in worker_edge_attrs) and \
               (admission_string in coworker_edge_attrs):
                    latest_start = fmax(worker_edge_attrs[admission_string],
                                       coworker_edge_attrs[admission_string])
                    earliest_end = fmin(worker_edge_attrs[demission_string],
                                       coworker_edge_attrs[demission_string])

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



