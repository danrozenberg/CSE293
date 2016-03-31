import logging
from graph_generator import *
from graph_analyser import *

def main():

    # TODO: config log somewhere else...
    logging.basicConfig(format='%(asctime)s %(message)s',
                        datefmt='%d %b - %H:%M:%S -',
                        level=logging.DEBUG)

    logging.info("Will start now")

    logging.info("Generating graph...")
    generator = SnapRandomGraphGenerator()
    graph = generator.get_graph()
    logging.info("Graph generated...")


    # logging.info("Starting analysis...")
    # analyser = GraphAnalyser(graph)
    # analyser.draw()



    logging.info("Done!")

if __name__ == "__main__":
    main()

