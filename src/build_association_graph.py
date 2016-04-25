import logging
import graph_manager
import data_parser

def process_files(source_folder, data_parser, interpreter_class, graph_manager, save_path=None):

    # get a graph from manager
    manager = graph_manager()

    for file_path in data_parser.find_files(source_folder, 0):
        process_file(file_path, data_parser, interpreter_class, manager)

    # graph should be complete at this point
    if save_path is not None:
        manager.save_graph(save_path)

    return manager

def process_file(file_path, data_parser, interpreter_class, graph):
    """
    :param graph: a graph/graph manager object, which will be changed
    """
    interpreter = interpreter_class()
    logging.warn("Started processing file " + file_path)
    for line in data_parser.lines_reader(file_path, 0):
            parsed_line = data_parser.parse_line(line)

            # line is parsed as a dictionary, but needs interpretation.
            # This is because our data is wacky wacky!
            interpreter.feed_line(parsed_line)
            process_line(interpreter, graph)

def process_line(interpreter, graph):

    if passes_filter(interpreter):
        create_nodes(interpreter, graph)
        create_edges(interpreter, graph)

def passes_filter(interpreter):
    """
    Checks if the interpreted data is good enough to be considered
    :return: true or false
    """
    # contains worker_id rule
    # sometimes line has no worker id? Why is this even in the database?
    if interpreter.worker_id == -1:
        return False

    # contains IDENTIFICAD rule
    # sometimes line has no employer id? Why is this even in the database?
    if interpreter.employer_id == -1:
        return False

    # contains year rule
    # sometimes line has no year? Huh?
    if interpreter.year == -1:
        return False

    return True

def create_nodes(interpreter, graph):

    # add worker node and a 'worker' property
    node_id = graph.add_node(interpreter.worker_id)
    graph.add_node_attr(node_id, "type", "worker")

    # add employer node and a 'employer' property
    node_id = graph.add_node(interpreter.employer_id)
    graph.add_node_attr(node_id, "type", "employer")

def create_edges(interpreter, graph):
    # add values as edge attributes

    src_node_id = interpreter.worker_id
    dest_node_id = interpreter.employer_id

    edge = graph.add_edge(src_node_id, dest_node_id)
    graph.add_edge_attr(edge, "year", interpreter.year)
    graph.add_edge_attr(edge, "time_at_employer",interpreter.time_at_employer)
    # We should add more edge attributes here as they are needed.

def enable_logging(log_level):
    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=log_level)

if __name__ == '__main__':
    enable_logging(logging.INFO)
    process_files("../test/parallel_graph_manip_test_1/",
                  data_parser.Pis12DataParser(),
                  data_parser.Pis12DataInterpreter,
                  graph_manager.SnapManager,
                  "../output_graphs/test.graph")
