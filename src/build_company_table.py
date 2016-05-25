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


def enable_logging(log_level):
    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=log_level)

if __name__ == '__main__':
    enable_logging(logging.WARNING)
    source_folder = "c:/csv_data/"
    output_file_path = "../output_stats/company_tables.csv"

    builder = TableBuilder()


    builder.process_files(source_folder,
                      data_parser.Pis12DataParser(),
                      data_parser.Pis12DataInterpreter,
                      graph_manager.SnapManager,
                      output_file_path)

    logging.warn("Finished!")
