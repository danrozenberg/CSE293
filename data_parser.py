import os
import csv
import config_manager
from  itertools import islice

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


    #TODO: replace with itertools
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

        names_read = 0

        #TODO: rename subfile variable
        for subfile in found_files:
            if os.path.isfile(folder_path + "/" +  subfile):
                yield subfile
                names_read += 1
                if 0 < fetch_num <= names_read:
                    break

    #TODO: replace with itertools
    def file_line_reader(self, file_path, fetch_num = None):

        #TODO: fetch more than 1 line at once on islice call.

        # also accepts 0 as being "all the file"
        if fetch_num == 0:
            fetch_num = None

        f = open(file_path)
        #TODO: check if the file really is a csv.
        reader = csv.reader(f)
        _ = reader.next()  # throws header away
        iterator = islice(reader, 0, 1)

        lines_read = 0
        while True:
            yield iterator.next()
            lines_read += 1
            iterator = islice(reader, 0, 1)
            if 0 < fetch_num <= lines_read:
                break

        # remember to clsoe the file :)
        f.close()



