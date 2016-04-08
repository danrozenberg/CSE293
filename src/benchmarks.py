import logging
import data_parser
from config_manager import Config

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
        logging.info("Finished a file: " + file_path)

    logging.info("Finished")
    logging.info("Read " + str(files_read) + " files")
    logging.info("Read " + str(lines_read) + " lines")


if __name__ == "__main__":
    simple_reading()
