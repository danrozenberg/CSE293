import logging
import data_parser
from config_manager import Config


from joblib import Parallel, delayed
import multiprocessing

def simple_reading():

    # TODO: config log somewhere else...
    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=logging.DEBUG)

    lines_read = 0
    files_read = 0
    config = Config()

    logging.info("Start simple reading now")

    for file_path in data_parser.file_paths(config.data_path, 0):
        files_read += 1
        logging.info("Started a file: " + file_path)
        for line in data_parser.lines(file_path, 0):
            data_parser.parse_line(line)
            lines_read += 1

    logging.info("Finished")
    logging.info("Read " + str(files_read) + " files")
    logging.info("Read " + str(lines_read) + " lines")

def start_parallel_reading():

    # TODO: config log somewhere else...
    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=logging.DEBUG)

    config = Config()
    files = data_parser.file_paths(config.data_path, 0)
    num_cores = multiprocessing.cpu_count()

    logging.info("Start parallel reading now")
    Parallel(n_jobs=num_cores)(delayed(parallel_reading)(f) for f in files)
    logging.info("Finished")

def parallel_reading(file_path):
    # TODO: config log somewhere else...
    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=logging.DEBUG)

    logging.info("Started a file: " + file_path)
    for line in data_parser.lines(file_path, 0):
        data_parser.parse_line(line)

if __name__ == "__main__":
    start_parallel_reading()
