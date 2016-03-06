import os
import csv

class DataParser(object):

    WORKER_EMPLOYER_BY_YEAR_FOLDER = "../data/rais/base-ano"

    def __init__(self):
        self.plant_from_id = {}

    def get_dictionary(self):
        return self.plant_from_id

    @staticmethod
    def process_folder(folder_path):
        found_files = os.listdir(folder_path)

        for subfile in found_files:
            if os.path.isfile(folder_path + "/" +  subfile):
                print subfile + " is a file"
            if os.path.isdir(folder_path + "/" + subfile):
                print subfile + " is a folder, diving in..."
                DataParser.process_folder(folder_path + "/" + subfile)

    def process_csvs(self, folder_path, year = None):
        files = os.listdir(folder_path)

        valid_files_ending = ".csv"
        if year is not None:
            valid_files_ending = str(year) + ".csv"

        for file_name in files:
            if file_name.endswith(valid_files_ending):
                self.parse_worker_factory_ano(folder_path + "/" + file_name)

    def parse_worker_factory_ano(self, file_path):
        reader = csv.reader(open(file_path))

        header = reader.next()
        pis_column = header.index("PIS")
        ano_column = header.index("ANO")
        plant_column = header.index("IDENTIFICAD")  #TODO: make sure this is the plan id.

        for line in reader:
            pis = line[pis_column]
            ano = line[ano_column]
            plant = line[plant_column]
            self.plant_from_id[pis + "_" + ano] = plant

    @staticmethod
    def read_txt(file_path, fetch_num = 0):
        """
        This is a simple generator. If it receives fetchNum = 0 then it reads the whole file.
        It reads the fetchNum lines otherwise.

        :param file_path: the path of the file to open
        :param fetch_num: how many lines to fetch
        :return: the read line.
        """
        lines_read = 0
        for line in open(file_path):
            yield line
            lines_read += 1

            if 0 < fetch_num <= lines_read:
                break

parser = DataParser()
parser.process_csvs("../data/rais/base-ano", 2005)
d = parser.get_dictionary()
print d
