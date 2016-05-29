import logging
import graph_manager
import data_parser
import statistics_gatherer
from collections import defaultdict
import cPickle as pickle

class TableBuilder(object):
    def __init__(self):
        self.count_from_municipality = defaultdict(int)
        self.entrant_from_municipality = defaultdict(int)
        self.spinnoff_from_municipality = defaultdict(int)
        self.ground_truth = statistics_gatherer.StatisticsGatherer.load_ground_truth("X:/state-csv/")

    def process_files(self, source_folder, data_parser, interpreter_class, graph_manager, save_path=None):

        # get a graph from manager
        manager = graph_manager()

        for file_path in data_parser.find_files(source_folder, 0):
            self.process_file(file_path, data_parser, interpreter_class, manager)

        # graph should be complete at this point
        if save_path is not None:
            with open(save_path, "wb") as target:
                target.writeline("Municipality, company count, entrant count, spinoff count")
                for key in self.count_from_municipality.keys():
                    target.writelines(str(key) + "," +
                                      self.count_from_municipality[key]  + "," +
                                      self.entrant_from_municipality[key]  + "," +
                                      self.spinnoff_from_municipality[key])
        return manager

    def process_file(self, file_path, data_parser, interpreter_class, graph):
        """
        :param graph: a graph/graph manager object, which will be changed
        """
        interpreter = interpreter_class()
        logging.warn("Started processing file " + file_path)
        for line in data_parser.lines_reader(file_path, 0):
                parsed_line = data_parser.parse_line(line)

                if self.passes_filter(interpreter.feed_line(parsed_line)):
                    municipality = interpreter.municipality
                    self.count_from_municipality[municipality] += 1
                    if interpreter.employer_id in self.ground_truth:
                        self.entrant_from_municipality[municipality] += 1


    def passes_filter(self, interpreter):
        """
        Checks if the interpreted data is good enough to be considered
        :return: true or false
        """
        # contains worker_id rule
        # sometimes line has no worker id? Why is this even in the database?
        if interpreter.worker_id == -1:
            return False

        # contains IDENTIFICAD rule
        # sometimes line has no employer id? Why is this even in the database?
        if interpreter.employer_id == -1:
            return False

        # contains year rule
        # sometimes line has no year? Huh?
        if interpreter.year == -1:
            return False

        # finally...
        return True

class CorporateBuilder(object):
    def __init__(self):
        self.manager_positions_from_pis = defaultdict(int)
        self.other_jobs_from_pis = defaultdict(set)

    def process_files(self, data_parser, interpreter_class, graph_manager, save_path=None):

        # get a graph from manager
        manager = graph_manager()
        self.process_file("X:/csv_data/poa_only.csv", data_parser, interpreter_class, manager)

        with open("X:/output_stats/poa_directors", "wb") as target:
            target.write("PIS, manager_jobs, other_jobs\n")
            for key in self.manager_positions_from_pis.keys():
                target.write(str(key) + "," +
                                  str(self.manager_positions_from_pis[key])  + "," +
                                  str(len(self.other_jobs_from_pis[key])) + "\n")

        with_dir_keys = set(self.manager_positions_from_pis.keys())
        other_jobs = set(self.other_jobs_from_pis.keys())
        dir_count = len(with_dir_keys)
        no_dir_count = len(other_jobs - with_dir_keys)

        logging.warn("Number of people with some director job = "  +str(dir_count) + " out of " +
                     str(dir_count + no_dir_count))

        pickle.dump(self.manager_positions_from_pis.keys(),
                    open("X:/output_stats/managers_poa.p", 'wb'))

        return manager

    def process_file(self, file_path, data_parser, interpreter_class, graph):
        directors_and_managers = [231,232,233,234,235,236,237,238,239,174,241,242,243,249,352,353,354,355,661]
        directors = [231,232,233,234,235,236,237,238,239]

        interpreter = interpreter_class()
        logging.warn("Started processing file " + file_path)
        for line in data_parser.lines_reader(file_path, 100):
                parsed_line = data_parser.parse_line(line)
                interpreter.feed_line(parsed_line)
                if self.passes_filter(interpreter):
                    worker_id = interpreter.worker_id
                    if interpreter.cbo_group in directors:
                        self.manager_positions_from_pis[worker_id] += 1
                    else:
                        self.other_jobs_from_pis[worker_id].add(interpreter.cbo_group)


    def passes_filter(self, interpreter):
        """
        Checks if the interpreted data is good enough to be considered
        :return: true or false
        """
        # contains worker_id rule
        # sometimes line has no worker id? Why is this even in the database?
        if interpreter.worker_id < 2:
            return False

        # contains IDENTIFICAD rule
        # sometimes line has no employer id? Why is this even in the database?
        if interpreter.employer_id < 2:
            return False

        # contains year rule
        # sometimes line has no year? Huh?
        if interpreter.year == -1:
            return False

        # must have wage
        if interpreter.avg_wage <= 0:
            return  False

        # single state rule
        # just process a single state, derived from municipality
        # 431490 is POA
        # 430510 is Caxias do Sul
        if interpreter.municipality <> '431490':
            return False

        # finally...
        return True


def enable_logging(log_level):
    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=log_level)

if __name__ == '__main__':
    enable_logging(logging.WARNING)
    builder = CorporateBuilder()
    builder.process_files( data_parser.Pis12DataParser(),
                      data_parser.Pis12DataInterpreter,
                      graph_manager.SnapManager)

    logging.warn("Finished!")
