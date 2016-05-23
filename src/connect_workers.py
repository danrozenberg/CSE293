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

def delete_files(folder_path):
    found_files = os.listdir(folder_path)
    for subfile in found_files:
        if os.path.isfile(folder_path + "/" +  subfile):
            file_path = folder_path + subfile
            os.remove(file_path)

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


    def start_get_edges_worker(self, graph_load_path):
        """ This is what gets called by Popen, so it is the
         entry point of processing, if called by the script start.py
        """
        # load affiliation
        affiliation_graph = SnapManager()
        proc_num = sys.argv[1]
        logging.warn("proc " + proc_num + ": Beggining to load graph...")
        affiliation_graph.load_graph(graph_load_path)
        logging.warn("proc " + proc_num + ": Affiliations graph loaded!")

        # Get this process' work load
        node_slice = get_node_slice(affiliation_graph)
        results = self.get_edges(affiliation_graph, node_slice)
        logging.warn("proc " + proc_num + ": Finished!")

        # save edge output
        output_folder = "../output_edges/"
        save_edges_to_disk(output_folder, results, proc_num)

    def get_edges(self, affiliation_graph, node_slice):
        """
        :param affiliation_graph: The source affiliation graph
        :param node_slice: a list of nodes for which we will calculate
            which edges should be attached to them.
        :return: a set of tuples.
        """

        # my answer will be a set of tuples
        edges_to_add = set()

         # get method addresses for performance reasonts
        get_neighboring_nodes = affiliation_graph.get_neighboring_nodes
        get_edge_between = affiliation_graph.get_edge_between
        get_edge_attrs = affiliation_graph.get_edge_attrs
        should_connect = self.should_connect

        progress_counter = -1
        for worker in node_slice:

            # log every once in a while
            progress_counter += 1
            if progress_counter % 1000 == 0:
                logging.warn("proc " + self.proc_num +
                             ": processed " + str(progress_counter) +
                             " workers.")

            # In an affiliation graph, we can get the employers just by
            # following the edges from worker and retrieving the neighbors.
            employer_nodes = get_neighboring_nodes(worker)

            for employer in employer_nodes:
                worker_edge = get_edge_between(worker, employer)
                worker_edge_attrs = get_edge_attrs(worker_edge)

                for coworker in get_neighboring_nodes(employer):

                    # sometimes, we can just skip a step in the algorithm...
                    if worker == coworker:
                        continue

                    coworker_edge = get_edge_between(coworker, employer)
                    coworker_edge_attrs = get_edge_attrs(coworker_edge)

                    if should_connect(worker_edge_attrs, coworker_edge_attrs):
                        edges_to_add.add((min(coworker, worker), max(worker, coworker)))

                        # TODO: maybe put time together in the attr?
                        # TODO: maybe put in some other attrs?

                # this worker-employer edge will never be examined again
                # and we are running out of memory, so we might as well
                # nuke the worker_edge_attr from memory. We just set it to None..
                affiliation_graph.attrs_from_edge[worker_edge] = None

        return edges_to_add

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


def add_edges_to_graph(edge_list, new_graph):
    for pair in edge_list:
        new_graph.add_node(pair[0])
        new_graph.add_node(pair[1])
        new_graph.add_edge(pair[0], pair[1])

def add_edges_from_disk(target_folder):
    #returns a new graph
    new_graph = SnapManager()

    found_files = os.listdir(target_folder)
    for subfile in found_files:
        edge_list = pickle.load(open(target_folder + subfile, 'rb'))
        add_edges_to_graph(edge_list, new_graph)

    return new_graph

def save_edges_to_disk(output_folder, edges, proc_num = ""):
    file_name = output_folder + "edge_list_" + proc_num + ".p"
    pickle.dump(edges, open(file_name, 'wb'))

def enable_logging(log_level):
    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=log_level)

def get_node_slice(affiliation_graph):
        # how many pieces are we dividing amonstg
        total_procs = int(sys.argv[2])
        proc_number = int(sys.argv[1])

        node_list = list(get_worker_iterator(affiliation_graph))
        start = (proc_number-1) * len(node_list) / total_procs
        end = proc_number * len(node_list) / total_procs
        return node_list[start:end]


if __name__ == '__main__':
    enable_logging(logging.WARNING)
    min_days = 90
    load_path = "../output_graphs/cds_affiliation.graph"
    save_path = "../output_graphs/cds_connected" + str(min_days) + "_days.graph"
    connector = WorkerConnector()
    connector.min_days_together = min_days
    connector.start_get_edges_worker(load_path)

