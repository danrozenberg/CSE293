import networkx as nx
from data_parser import DataParser

def association_graph():
    details = ["PIS",
               "Plant"]

    # delcare graph
    g = nx.Graph()

    # read data from the following:
    parser = DataParser()
    for line in parser.parse_worker_factory_ano("../data/rais/base-ano/ano2005.csv", 100):
        pis = line[0]
        plant = line[2]
        g.add_node(pis)
        g.node[pis]['category']='pis'
        g.add_node(plant)
        g.node[plant]['category']='plant'
        g.add_edge(pis, plant)
    return g


G = association_graph()
color_map = {'pis':'blue', 'plant':'red'}

try:
    import matplotlib.pyplot as plt
except ImportError:
    import sys
    print("Matplotlib needed for drawing. Skipping")
    sys.exit(0)

nx.draw(G, node_color=[color_map[G.node[node]['category']] for node in G])
plt.show()

