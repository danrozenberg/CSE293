#TODO: upgrade to some configuration/environment helper library
class Config(object):

    def __init__(self):
        self.config_folder_path = "./config/"
        self.data_path = None
        self.image_output_path = None
        self.__setup_environment()

    def get_data_path(self):
        return self.data_path

    def get_image_output_path(self):
        return self.image_output_path

    def __setup_environment(self):
        f = open(self.config_folder_path + "environment.txt")
        for line in f:
            split_line = line.split("=")
            if split_line[0] == "data_path":
                self.data_path = split_line[1]
            if split_line[0] == "image_output_path":
                self.image_output_path = split_line[1]


