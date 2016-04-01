import os
import csv
import config_manager

#TODO: call this data manager?
class DataParser(object):

    def __init__(self):
        config = config_manager.Config()

        self.data_folder_path = config.data_path
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


    def file_name_generator(self, folder_path, fetch_num = 0):
        """
        A generator...
        Given a folder, returns names of files inside it.
        It returns as many as fetch_num names, which makes it useful in
        small scale tests
        :param folder: the folder to generate file names from. Does not process
            sub-folders
        :param fetch_num: if 0, we will generate names until they run out,
            otherwise, return at most fetch_num names
        :return: the names of the files inside folder.
        """
        found_files = os.listdir(folder_path)

        lines_read = 0
        for subfile in found_files:
            if os.path.isfile(folder_path + "/" +  subfile):
                yield subfile
                lines_read += 1
                if 0 < fetch_num <= lines_read:
                    break

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
