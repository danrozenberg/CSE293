import logging
import graph_manager
import data_parser
import cPickle as pickle

def process_file(file_path, data_parser, interpreter_class, graph,
                 save_path=None, worker_id_filter=None):
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
            process_line(interpreter, graph, worker_id_filter)

    # graph should be complete at this point
    if save_path is not None:
        graph.save_graph(save_path)

def process_line(interpreter, graph, worker_id_filter=None):

    if passes_filter(interpreter, worker_id_filter):
        create_nodes(interpreter, graph)
        create_edges(interpreter, graph)

def passes_filter(interpreter, worker_id_filter=None):
    """
    Checks if the interpreted data is good enough to be considered
    :return: true or false
    """
    # contains worker_id rule
    # sometimes line has no worker id? Why is this even in the database?
    if interpreter.worker_id < 2:
        return False

    if worker_id_filter is not None and \
        interpreter.worker_id not in worker_id_filter:
        return False

    # contains IDENTIFICAD rule
    # sometimes line has no employer id? Why is this even in the database?
    if interpreter.employer_id < 2:
        return False

    # contains year rule
    # sometimes line has no year? Huh?
    if interpreter.year == -1:
        return False

    # must have wage
    if interpreter.avg_wage <= 0:
        return  False

    # municipality rule
    # 430510 is Caxias do Sul || 431490 is POA
    if interpreter.municipality <> '431490':
        return False

    # finally...
    return True

def create_nodes(interpreter, graph):

    # add employer node and a 'employer' property
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
        edge_id = graph.quick_add_edge(src_node_id, dest_node_id)

    year = interpreter.year
    graph.add_edge_attr(edge_id, str(year) + "_ad",      #admission date
                        interpreter.admission_timestamp)
    graph.add_edge_attr(edge_id, str(year) + "_de",      #demissionn date
                        interpreter.demission_timestamp)
    graph.add_edge_attr(edge_id, str(year) + "_aw",      #avg wage
                        interpreter.avg_wage)

    # We should add more edge attributes here as they are needed.

def enable_logging(log_level):
    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=log_level)

if __name__ == '__main__':
    enable_logging(logging.WARNING)
    source_file = "X:/csv_data/poa_only.csv"
    output_file_path = "X:/output_graphs/poa_managers_and_directors_affiliation.graph"
    allowed_worker_ids = pickle.load(open("X:/output_stats/poa_managers_and_directors.p",'rb'))

    process_file(source_file,
                  data_parser.Pis12DataParser(),
                  data_parser.Pis12DataInterpreter,
                  graph_manager.SnapManager(),
                  output_file_path,
                  allowed_worker_ids)

    logging.warn("Finished!")
