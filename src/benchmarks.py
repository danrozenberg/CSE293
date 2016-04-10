import logging
import data_parser
from config_manager import Config
from graph_manager import SnapManager
import random

from joblib import Parallel, delayed
import multiprocessing

def configure_log():
    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=logging.DEBUG)


def simple_reading():

    lines_read = 0
    files_read = 0
    config = Config()
    parser = data_parser.Pis12DataParser()
    logging.info("Start simple reading now")

    for file_path in parser.find_files(config.data_path, 0):
        files_read += 1
        logging.info("Started a file: " + file_path)
        for line in parser.lines_reader(file_path, 0):
            parser.parse_line(line)
            lines_read += 1

    logging.info("Finished")
    logging.info("Read " + str(files_read) + " files")
    logging.info("Read " + str(lines_read) + " lines")

def start_parallel_reading():
    config = Config()
    parser = data_parser.Pis12DataParser()
    files = parser.find_files(config.data_path, 0)
    num_cores = multiprocessing.cpu_count()

    logging.info("Start parallel reading now")
    lines_read = Parallel(n_jobs=num_cores)(delayed(parallel_reading)(f, parser) for f in files)
    logging.info("Finished")
    logging.info("read " + str(sum(lines_read)) + " lines.")

def parallel_reading(file_path, parser):
    logging.info("Started a file: " + file_path)
    read_lines = 0
    for line in parser.lines_reader(file_path, 0):
        parser.parse_line(line)
        read_lines += 1
    return read_lines

def start_parallel_node_insertion():
    graph_manager = SnapManager()
    num_cores = multiprocessing.cpu_count()

    logging.info("Start parallel node addition now")

    node_ids = []
    for i in range(1, 2000):
        node_ids.append([random.randint(1,9999999) for _ in range(1,100)])
    unique_ids = set([item for sublist in node_ids for item in sublist])

    Parallel(n_jobs=num_cores, backend='threading') \
        (delayed(parallel_node_insertion) \
             (n, graph_manager) for n in node_ids)
    logging.info("Finished adding nodes, let's verify...")

    found_all = True
    for x in unique_ids:
        if not graph_manager.is_node(x):
            found_all = False
            logging.info("ERROR: Couldn't find node with id " + str(x))
    if found_all:
        logging.info("Found all node ids!")
        logging.info("Added " + str(graph_manager.node_count()) + " nodes.")



def parallel_node_insertion(id_list, graph_manager):
    # Checks if adding nodes is thread safe.
    for ID in id_list:
        graph_manager.add_node(ID)

if __name__ == "__main__":
    configure_log()
    start_parallel_node_insertion()
