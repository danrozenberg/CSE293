import logging
import data_parser
from config_manager import Config

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

def parallel_node_insertion():
    # Checks if adding nodes is thread safe.
    pass

if __name__ == "__main__":
    configure_log()
    start_parallel_reading()
