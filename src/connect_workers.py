import logging
import sys
from graph_manager import SnapManager

def get_worker_iterator(affiliation_graph):
    node_iterator = affiliation_graph.get_node_iterator()
    for node in node_iterator:
        node_type = affiliation_graph.get_node_attr(node, "type")
        if node_type == "worker":
            yield node

def get_worker_list(affiliation_graph):
    list = []
    node_iterator = affiliation_graph.get_node_iterator()
    for node in node_iterator:
        node_type = affiliation_graph.get_node_attr(node, "type")
        if node_type == "worker":
            list.append(node)
    return list

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

        # final year: this represents the year (including it up to Dec-31st)...
        # We will ignore any edges that represents date after that
        # as such, we can create a snapshot at the end of that year.
        self.max_year = 2016

        for year in xrange(0, self.max_year+1):
            self.admission_strings.append(str(year) + "_admission_date")
            self.demission_strings.append(str(year) + "_demission_date")

        self.proc_num = sys.argv[1]
        self.progress_counter = -1

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

    def add_edges_between(self, coworker, employer, get_edge_attrs, get_edge_between,
                    new_graph, should_connect, worker, worker_edge_attrs):
        # sometimes, we can just skip a step in the algorithm...
        if not should_skip(worker, coworker, new_graph):
            coworker_edge = get_edge_between(coworker, employer)
            coworker_edge_attrs = get_edge_attrs(coworker_edge)

            if should_connect(worker_edge_attrs, coworker_edge_attrs):
                new_graph.add_node(coworker)
                new_graph.add_edge(worker, coworker)

    def for_each_employer(self, attrs_from_edge, employer, get_edge_attrs,
                          get_edge_between, get_neighboring_nodes, worker, new_graph,
                          should_connect):
        worker_edge = get_edge_between(worker, employer)
        worker_edge_attrs = get_edge_attrs(worker_edge)
        map(lambda x: self.add_edges_between(x, employer, get_edge_attrs,
                                             get_edge_between, new_graph,
                                             should_connect, worker,
                                             worker_edge_attrs),
            get_neighboring_nodes(employer))
        # this worker-employer edge will never be examined again
        # and we are running out of memory, so we might as well
        # nuke the worker_edge_attr from memory. We just set it to None..
        del attrs_from_edge[worker_edge]

    def for_each_worker(self, affiliation_graph, get_neighboring_nodes,
                        new_graph, worker, attrs_from_edge,
                        get_edge_attrs, get_edge_between, should_connect):
        # log every once in a while
        self.progress_counter += 1
        if self.progress_counter % 1000 == 0:
            logging.warn("proc " + self.proc_num +
                         ": processed " + str(self.progress_counter) +
                         " workers.")

        # add this worker to the new graph, if necessary
        affiliation_graph.copy_node(worker, new_graph)
        # In an affiliation graph, we can get the employers just by
        # following the edges from worker and retrieving the neighbors.
        employer_nodes = get_neighboring_nodes(worker)
        map(lambda x: self.for_each_employer(attrs_from_edge, x,
                                             get_edge_attrs, get_edge_between,
                                             get_neighboring_nodes, worker,
                                             new_graph, should_connect),
            employer_nodes)

    def connect_workers(self, affiliation_graph, new_graph):

        # get method addresses for performance reasonts
        get_neighboring_nodes = affiliation_graph.get_neighboring_nodes
        get_edge_between = affiliation_graph.get_edge_between
        get_edge_attrs = affiliation_graph.get_edge_attrs
        attrs_from_edge = affiliation_graph.attrs_from_edge
        should_connect = self.should_connect

        map(lambda x: self.for_each_worker(affiliation_graph, get_neighboring_nodes,
                                 new_graph, x, attrs_from_edge,
                                 get_edge_attrs, get_edge_between, should_connect),
            get_worker_iterator(affiliation_graph))

        return new_graph

    def should_connect(self, worker_edge_attrs, coworker_edge_attrs):
        # although less general, receiving attributes as parameters
        # allows us to call get_edge_attrs almost half the number of times...
        # TODO, make worker_edge attrs a class property or something...

        time_together = self.get_time_together(worker_edge_attrs,
                                               coworker_edge_attrs,
                                               self.min_days_together)

        # add more checks here, as needed.
        return time_together >= self.min_days_together

    def time_together_for_year(self, admission_strings, coworker_edge_attrs,
                               demission_strings, worker_edge_attrs, year):

        admission_string = admission_strings[year]
        demission_string = demission_strings[year]
        if (admission_string in worker_edge_attrs) and \
            (admission_string in coworker_edge_attrs):
            latest_start = max(worker_edge_attrs[admission_string],
                               coworker_edge_attrs[admission_string])
            earliest_end = min(worker_edge_attrs[demission_string],
                               coworker_edge_attrs[demission_string])

            # timestamps
            return  round( max(((earliest_end - latest_start) / 60 / 60 / 24) + 1, 0))
        else:
            return 0


    def get_time_together(self, worker_edge_attrs, coworker_edge_attrs, min_days = None):
        # although less general, receiving attributes as parameters
        # allows us to call get_edge_attrs almost half the number of times...

        # we did some string concatenation in the class init method
        # lets reference to it here.
        admission_strings = self.admission_strings
        demission_strings = self.demission_strings

        list_of_time_together = map(lambda x :self.time_together_for_year(admission_strings,
                                                        coworker_edge_attrs,
                                                        demission_strings,
                                                        worker_edge_attrs, x),
            xrange(self.max_year, 1980, -1))


        time_together = sum(list_of_time_together)
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

