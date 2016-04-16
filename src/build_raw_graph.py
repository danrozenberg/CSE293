from datetime import datetime
from config_manager import Config
import graph_manager
import data_parser

def process_files(source_folder, data_parser, interpreter_class, graph_manager, save_path=None):

    # get a graph from manager
    manager = graph_manager()

    for file_path in data_parser.find_files(source_folder, 0):
        process_file(file_path, data_parser, interpreter_class, manager)

    if save_path is not None:
        manager.save_graph(save_path)
    # graph should be complete at this point

    return manager

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
    create_nodes(interpreter, graph)
    create_edges(interpreter, graph)

def create_nodes(interpreter, graph):

    # add worker node and a 'worker' property
    NId = graph.add_node(interpreter.worker_id)
    graph.add_node_attr(NId, "type", "worker")

    # add employer node and a 'employer' property
    NId = graph.add_node(interpreter.employer_id)
    graph.add_node_attr(NId, "type", "employer")

def create_edges(interpreter, graph):
    # add values as edge attributes

    src_node_id = interpreter.worker_id
    dest_node_id = interpreter.employer_id

    edge = graph.add_edge(src_node_id, dest_node_id)
    graph.add_edge_attr(edge, "year", interpreter.year)
    graph.add_edge_attr(edge, "time_at_worker",interpreter.time_at_worker)
    # We should add more edge attributes here as they are needed.

if __name__ == '__main__':

    # TODO: don't use config like this, pass it as a parameter or something..
    config = Config()
    source_folder = config.get_data_path()

    process_files(source_folder,
                  data_parser.Pis12DataParser(),
                 graph_manager.SnapManager)
