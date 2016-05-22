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

    # single state rule
    # just process a single state, derived from municipality
    # TODO: one of these days, don't hard code it...
    # 43 is Rio Grande do Sul...
    if len(interpreter.municipality) < 2 or \
        interpreter.municipality <> '431490':
            return False

    # finally...
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

    # here we guarantee that there will be only 1 edge per pair of nodes.
    edge_id = graph.get_edge_between(src_node_id, dest_node_id)
    if edge_id is None:
        edge_id = graph.add_edge(src_node_id, dest_node_id)

    year = interpreter.year
    graph.add_edge_attr(edge_id, str(year) + "_admission_date",
                        interpreter.admission_timestamp)
    graph.add_edge_attr(edge_id, str(year) + "_demission_date",
                        interpreter.demission_timestamp)

    # We should add more edge attributes here as they are needed.

def enable_logging(log_level):
    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=log_level)

if __name__ == '__main__':
    enable_logging(logging.WARNING)
    source_folder = "c:/csv_data/"
    output_file_path = "../output_graphs/rs_affiliation.graph"


    process_files(source_folder,
                  data_parser.Pis12DataParser(),
                  data_parser.Pis12DataInterpreter,
                  graph_manager.SnapManager,
                  output_file_path)

    logging.warn("Finished!")
