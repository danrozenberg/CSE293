import snap
from config_manager import Config
from data_parser import DataParser

# 1) go throuh each file. For each CNPJ/plant, create a node. (stream)
# 2) add a node for each person (stream)
# 3) connect people to companies with information in the edges. (stream)

def build_employee_employer_graph():
    """
    This method builds a network that links employees to employers through edges.
    We store some information in the edges.
        this information relates to the timeframe to which
        the employee was working there in the company
    Noeds store ?????????
    :return: a network (graph) with employees connected to employers.
    """

    config = Config()

    for file_name in DataParser.get_file_names(config.data_path, 0):
        print file_name



build_employee_employer_graph()
