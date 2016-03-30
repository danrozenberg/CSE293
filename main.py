from graph_generator import *
from graph_analyser import *

def main():
    print "Will start now"

    print "Generating graph..."
    generator = RandomGraphGenerator()
    graph = generator.get_graph()
    print "Graph generated..."


    print "Starting analysis..."
    analyser = GraphAnalyser(graph)
    analyser.draw()

if __name__ == "__main__":
    main()

