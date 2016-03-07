import networkx as nx
import itertools
from data_parser import DataParser

def association_graph():
    details = ["PIS",
               "Plant"]

    parser = DataParser()
    graph_list = []
    plant_set = set()

    # iterate over list
    for i in xrange(2005, 2006):
        file_path = "../data/rais/base-ano/ano" + str(i) + ".csv"

        # delcare graph
        # TODO: take this out of the loop
        g = nx.Graph()

        # read data from the following:
        for line in parser.parse_worker_factory_ano(file_path, 10):
            pis = line[0]
            plant = line[2]
            g.add_node(pis)
            g.node[pis]['category']='pis'
            plant_set.add(plant)
            g.add_node(plant)
            g.node[plant]['category']='plant'
            g.add_edge(pis, plant)
        graph_list.append(g)
    return graph_list

def check_matplotlib():
    try:
        import matplotlib.pyplot as plt
    except ImportError:
        import sys

        print("Matplotlib needed for drawing. Skipping")
        sys.exit(0)
    return plt

def connect_workers_in_same_plant(graph_list, delta_years = 0):
    for graph in graph_list:
        for node in graph:
            category = graph.node[node]['category']
            if category == 'plant':
                new_edge_list = itertools.combinations(graph.neighbors(node), 2)
                graph.add_edges_from(new_edge_list)

    #TODO: make sure it connects people from the past data too.


def main():
    graph_list = association_graph()
    connect_workers_in_same_plant(graph_list)

    G = graph_list[0]

    color_map = {'pis':'blue', 'plant':'red'}
    plt = check_matplotlib()
    nx.draw(G, node_color=[color_map[G.node[node]['category']] for node in G])
    plt.show()


if __name__ == "__main__":
    main()
