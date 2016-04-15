from datetime import datetime
from config_manager import Config
import graph_manager
import data_parser

def process_files(source_folder, data_parser, interpreter_class, graph_manager):

    # get a graph from manager
    graph = graph_manager()

    for file_path in data_parser.find_files(source_folder, 0):
        process_file(file_path, data_parser, graph)

    # graph should be complete at this point
    return graph

def process_file(file_path, data_parser, interpreter_class, graph):
    """
    :param graph: a graph/graph manager object, which will be changed
    """
    for line in data_parser.lines_reader(file_path, 0):
            parsed_line = data_parser.parse_line(line)

            # line is parsed as a dictionary, but needs interpretation.
            # This is because our data is wacky wacky!
            interpreter = interpreter_class(parsed_line)
            process_line(interpreter, graph)

def process_line(interpreter, graph):
    # Add nodes to graph
    create_nodes(interpreter, graph)

    # Create edge and add temporal information to it.
    create_edges(interpreter, graph)

def create_nodes(interpreter, graph):
    ''' Manager only does it if they don't exist yet.
        It will add one for the worker, one for the employer'''

    # add worker node and a 'worker' property
    NId = graph.add_node(interpreter.worker_id)
    graph.add_node_attr(NId, "type", "worker")

    # add employer node and a 'employer' property
    NId = graph.add_node(interpreter.employer_id)
    graph.add_node_attr(NId, "type", "employer")

def create_edges(interpreter, graph):
    """
    Creates worker-employer edges, filled with temporal data.
    :param interpreter:
    :param graph:
    :return:
    """
    # get values here to, mostly, make lines shorter :)
    time_at_worker = interpreter.time_at_employer
    year = interpreter.year

    # add values as edge attributes
    edge = graph.add_edge()
    graph.add_edge_attr(edge, "YEAR", year)
    graph.add_edge_attr(edge, "TIME_AT_WORKER",time_at_worker)

if __name__ == '__main__':

    # TODO: don't use config like this, pass it as a parameter or something..
    config = Config()
    source_folder = config.get_data_path()

    process_files(source_folder,
                  data_parser.Pis12DataParser(),
                 graph_manager.SnapManager)
