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

    def parse_worker_factory_ano(self, file_path, fetchNum = 0):

        f = open(file_path)
        reader = csv.reader(f)
        header = reader.next()
        pis_column = header.index("PIS")
        year_column = header.index("ANO")
        plant_column = header.index("IDENTIFICAD")  #TODO: make sure this is the plan id.

        linesRead = 0
        for line in reader:
            pis = line[pis_column]
            year = line[year_column]
            plant = line[plant_column]
            yield [pis, year, plant]

            linesRead += 1
            if 0 < fetchNum <= linesRead:
                break

        # remember to clsoe the file :)
        f.close()
