import datetime
import logging
from graph_manager import SnapManager
from statistics_gatherer import StatisticsGatherer

def get_worker_iterator(affiliation_graph):
    node_iterator = affiliation_graph.get_node_iterator()
    for node in node_iterator:
        node_type = affiliation_graph.get_node_attr(node, "type")
        if node_type == "worker":
            yield node

def get_overlapping_days(start_1, end_1, start_2, end_2):
    # from https://stackoverflow.com/questions/9044084/efficient-date-range-overlap-calculation-in-python

    latest_start = max(start_1, start_2)
    earliest_end = min(end_1, end_2)

    # timestamps
    if type(start_1) == float:
        return round(max(((earliest_end - latest_start)/60/60/24) + 1, 0))

    # datetimes
    if type(start_1) == datetime.datetime:
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

    def connect_all_workers(self, affiliation_graph, new_graph):

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

    def build_ego_networks(self, ego_list, affiliation_graph):
        network_from_ego = {}
        for ego in ego_list:
            logging.warn("Started building network for ego: " +str(ego))
            network_from_ego[ego] = self.build_ego_network(ego, affiliation_graph)
        return network_from_ego

    def build_ego_network(self, ego, affiliation_graph):
        '''
        :param ego: the ego
        :param affiliation_graph: the affiliation graph that lets us figure
          out the ego network.
        :return:a new graph_manager object (the ego network)
        '''

        # first add nodes present in the ego
        ego_net = SnapManager()
        plant_nodes = affiliation_graph.get_neighboring_nodes(ego)
        for plant in plant_nodes:
            ego_edge = affiliation_graph.get_edge_between(ego, plant)
            ego_edge_attrs = affiliation_graph.get_edge_attrs(ego_edge)

            for coworker in affiliation_graph.get_neighboring_nodes(plant):

                # sometimes, we can just skip a step in the algorithm...
                if should_skip(ego, coworker, ego_net):
                    continue

                coworker_edge = affiliation_graph.get_edge_between(coworker, plant)
                coworker_edge_attrs = affiliation_graph.get_edge_attrs(coworker_edge)

                if self.should_connect(ego_edge_attrs, coworker_edge_attrs):
                    ego_net.add_node(coworker)
                    ego_net.add_edge(ego, coworker)


        # now we interconnect all nodes in the ego.
        # remember that ego net does not include self.
        self.connect_alters(ego, ego_net, affiliation_graph)
        return ego_net

    def connect_alters(self, ego, ego_net, affiliation_graph):

        alter_list = ego_net.get_nodes()
        for alter in alter_list:
            plant_nodes = affiliation_graph.get_neighboring_nodes(alter)
            for plant in plant_nodes:
                alter_edge = affiliation_graph.get_edge_between(alter, plant)
                alter_edge_attrs = affiliation_graph.get_edge_attrs(alter_edge)

                for coworker in affiliation_graph.get_neighboring_nodes(plant):

                    # we only connect alters. Also, ego should not be consider.
                    if (coworker not in alter_list) or (coworker == ego):
                        continue
                    if should_skip(alter, coworker, ego_net):
                        continue

                    coworker_edge = affiliation_graph.get_edge_between(coworker, plant)
                    coworker_edge_attrs = affiliation_graph.get_edge_attrs(coworker_edge)

                    if self.should_connect(alter_edge_attrs, coworker_edge_attrs):
                        ego_net.add_node(coworker)
                        ego_net.add_edge(alter, coworker)

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
            worker_start = worker_edge_attrs[admission_string]
            coworker_start = coworker_edge_attrs[admission_string]

            if (worker_start is not None) \
                and (coworker_start is not None):
                time_together += get_overlapping_days(worker_start ,
                                                      worker_edge_attrs[demission_string],
                                                      coworker_start,
                                                      coworker_edge_attrs[demission_string])

                # we can stop if we were given a min_days, and
                # if that min time has been reached
                if min_days is not None and time_together > min_days:
                    return time_together

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
    connector = WorkerConnector()
    connector.min_days_together = min_days

    logging.warn("Drawing random sample!")
    ego_list = StatisticsGatherer.get_node_sample(affiliation_graph,
                                                  500,
                                                  "worker")
    logging.warn("Sample drawn... will now start building networks:")
    network_from_ego = connector.build_ego_networks(ego_list,affiliation_graph)
    logging.warn("Done!")

    # save it
    # connected_graph.save_graph(save_path)

if __name__ == '__main__':
    enable_logging(logging.WARNING)
    min_days = 182
    load_path = "../output_graphs/rs_affiliation.graph"
    save_path = "../output_graphs/rs_connected" + str(min_days) + "_days.graph"
    logging.warn("Started!")
    run_script(load_path, save_path, min_days)
    logging.warn("Finished!")
