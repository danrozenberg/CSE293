from datetime import datetime
from config_manager import Config
import graph_manager
import data_parser

def build_worker_employer_graph(data_parser, graph_manager):
    """
    This method builds a network that links employees to employers through edges.
    We store some information in the edges.
        this information relates to the timeframe to which
        the employee was working there in the company
    Noeds store ?????????
    :return: a network (graph) with employees connected to employers.
    """

    # TODO: don't use config like this, pass it as a parameter or something..
    config = Config()
    for file_path in data_parser.file_paths(config.data_path, 0):
        for line in data_parser.lines(file_path, 0):
            parsed_line = data_parser.parse_line(line)

            # line is parsed as a dictionary, but needs interpretation.
            # This is because our data is wacky wacky!
            interpreter = data_parser.Pis12DataInterpreter(parsed_line)

            # Add nodes to graph
            self.create_nodes(interpreter)

            # Create edge and add temporal information to it.
            new_edge = create_worker_employer_edge(interpreter)
            fill_edge_attributes(new_edge, interpreter)

def create_nodes(interpreter):
    # Manager only does it if they don't exist yet.

    # add worker node and a 'worker' property
    # add employer node and a 'employer' property
    raise NotImplementedError

def create_worker_employer_edge(interpreter):
    raise NotImplementedError

def fill_edge_attributes(edge, interpreter, graph):
    '''
    :param edge: an edge id.
    :param interpreter: something that can give us information,
     as properties, as seen below...
    '''

    # get values here to, mostly, make lines shorter :)
    time_at_worker = interpreter.time_at_worker
    year = interpreter.year

    # add values as edge attributes
    graph.add_edge_attr(edge, "YEAR", year)
    graph.add_edge_attr(edge, "TIME_AT_WORKER",time_at_worker)

def connect_workers(raw_graph_manager):
    raise NotImplementedError

    # create a new graph

    # for each employer in raw_graph
        # for each out_edge i
            # for each out_edge j
                # break if the years diverge
                # break if they relate to the same worker
                # create an edge between the 2 workers in the new graph
                # add attrs (year, time together)

# TODO: decide how to express it (number of days is a good candidate).
def time_working_together(worker_1, worker_2):
    """
    :return: a dictionary with key-value pairs as following:
        place of employment / time working there together.
    If the 2 workers never worked together, then the dictionary is empty.
    """
    raise NotImplementedError

def time_working_together_at_employer(worker_1, worker_2, employer, year):
    '''This is the edge between 2 people
    The edge is anottated with the employer, the year
    raise NotImplementedError'''




if __name__ == '__main__':
    build_worker_employer_graph(graph_manager.SnapManager(),
                                data_parser.Pis12DataParser())
