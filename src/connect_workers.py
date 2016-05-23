import datetime
import logging
import sys
import pickle
import os
from graph_manager import SnapManager

def get_worker_iterator(affiliation_graph):
    node_iterator = affiliation_graph.get_node_iterator()
    for node in node_iterator:
        node_type = affiliation_graph.get_node_attr(node, "type")
        if node_type == "worker":
            yield node

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

        self.proc_num = sys.argv[1]


    def start_connect_worker_proc(self):
        """ This is what gets called by Popen, so it is the
         entry point of processing, if called by the script start.py
        """
        # year from argvs
        year = sys.argv[2]
        graph_load_path = "../output_graphs/cds_affiliation.graph"

        # load affiliation
        affiliation_graph = SnapManager()
        logging.warn("proc " + self.proc_num + ": Beggining to load graph...")
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

        progress_counter = -1
        for worker in get_worker_iterator(affiliation_graph):

            # log every once in a while
            progress_counter += 1
            if progress_counter % 1000 == 0:
                logging.warn("Processed " + str(progress_counter) + " workers.")

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

                # this worker-employer edge will never be examined again
                # and we are running out of memory, so we might as well
                # nuke the worker_edge_attr from memory. We just set it to None..
                affiliation_graph.attrs_from_edge[worker_edge] = None

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

    def get_time_together(self, worker_edge_attrs, coworker_edge_attrs, min_days = None):
        # although less general, receiving attributes as parameters
        # allows us to call get_edge_attrs almost half the number of times...
        time_together = 0

        # we did some string concatenation in the class init method
        # lets reference to it here.
        admission_strings = self.admission_strings
        demission_strings = self.demission_strings

        for year in xrange(2016, 1980, -1):
            admission_string = admission_strings[year]
            demission_string = demission_strings[year]
            if (admission_string in worker_edge_attrs) and \
               (admission_string in coworker_edge_attrs):
                    latest_start = max(worker_edge_attrs[admission_string],
                                       coworker_edge_attrs[admission_string])
                    earliest_end = min(worker_edge_attrs[demission_string],
                                       coworker_edge_attrs[demission_string])

                    # timestamps
                    if type(latest_start) == float and type(earliest_end) == float:
                        time_together += round(max(((earliest_end - latest_start)/60/60/24) + 1, 0))
                    else:
                        logging.warn("Weird type in one of the types: worker_admission: " +
                                     str(worker_edge_attrs[admission_string]) + " | worker_demission: " +
                                     str(worker_edge_attrs[demission_string]) + " | coworker_admission: " +
                                     str(coworker_edge_attrs[admission_string]) + " | coworker_demission: " +
                                     str(coworker_edge_attrs[demission_string]))

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

