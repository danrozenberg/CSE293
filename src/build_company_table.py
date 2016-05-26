import logging
import graph_manager
import data_parser
import statistics_gatherer
from collections import defaultdict

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
                target.write("Municipality, company count, entrant count, spinoff count\n")
                for key in self.count_from_municipality.keys():
                    target.write(str(key) + "," +
                                      self.count_from_municipality[key]  + "," +
                                      self.entrant_from_municipality[key]  + "," +
                                      self.spinnoff_from_municipality[key] + "\n")
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

class CboBuilder(object):
    def __init__(self):
        self.manager_positions_from_pis = defaultdict(int)
        self.other_jobs_from_pis = defaultdict(set)

    def process_files(self, source_folder, data_parser, interpreter_class, graph_manager, save_path=None):

        # get a graph from manager
        manager = graph_manager()

        for file_path in data_parser.find_files(source_folder, 0):
            self.process_file(file_path, data_parser, interpreter_class, manager)

        # graph should be complete at this point
        if save_path is not None:
            with open(save_path, "wb") as target:
                target.write("PIS, manager_jobs, other_jobs\n")
                for key in self.manager_positions_from_pis.keys():
                    target.write(str(key) + "," +
                                      str(self.manager_positions_from_pis[key])  + "," +
                                      str(len(self.other_jobs_from_pis[key])) + "\n")
        return manager

    def process_file(self, file_path, data_parser, interpreter_class, graph):
        """
        :param graph: a graph/graph manager object, which will be changed
        """
        interpreter = interpreter_class()
        logging.warn("Started processing file " + file_path)
        for line in data_parser.lines_reader(file_path, 0):
                parsed_line = data_parser.parse_line(line)
                interpreter.feed_line(parsed_line)
                if self.passes_filter(interpreter):
                    worker_id = interpreter.worker_id
                    if 100 <= interpreter.cbo_group <= 199:
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

        if len(interpreter.municipality) < 2 or \
            interpreter.municipality[:2] <> '43':
            return False

        # finally...
        return True


def enable_logging(log_level):
    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=log_level)

if __name__ == '__main__':
    enable_logging(logging.WARNING)
    source_folder = "X:/csv_data/"
    output_file_path = "X:/output_stats/director_tables.csv"

    builder = CboBuilder()
    builder.process_files(source_folder,
                      data_parser.Pis12DataParser(),
                      data_parser.Pis12DataInterpreter,
                      graph_manager.SnapManager,
                      output_file_path)

    logging.warn("Finished!")
