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
        self.ground_truth = statistics_gatherer.StatisticsGatherer.load_ground_truth("../output_stats/ground_truth_new_25.p")

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
                                     str(self.count_from_municipality[key])  + "," +
                                     str(self.entrant_from_municipality[key])  + "," +
                                     str(self.spinnoff_from_municipality[key]) + "\n")
        return manager

    def process_file(self, file_path, data_parser, interpreter_class, graph):
        """
        :param graph: a graph/graph manager object, which will be changed
        """
        interpreter = interpreter_class()
        logging.warn("Started processing file " + file_path)
        for line in data_parser.lines_reader(file_path, 100000):
                parsed_line = data_parser.parse_line(line)
                interpreter.feed_line(parsed_line)

                if self.passes_filter(interpreter):
                    municipality = interpreter.municipality
                    self.count_from_municipality[municipality] += 1
                    if interpreter.employer_id in self.ground_truth:
                        self.entrant_from_municipality[municipality] += 1
                        if self.ground_truth[interpreter.employer_id][1] == "EMPLOYEE_SPINOFF":
                            self.spinnoff_from_municipality[municipality] += 1


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
    output_file_path = "../output_stats/company_tables_new_25.csv"

    builder = TableBuilder()


    builder.process_files(source_folder,
                      data_parser.Pis12DataParser(),
                      data_parser.Pis12DataInterpreter,
                      graph_manager.SnapManager,
                      output_file_path)

    logging.warn("Finished!")
