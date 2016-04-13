def connect_workers(raw_graph, graph_manager):
    raise NotImplementedError

    # create a new graph

    # for each employer in raw_graph
        # for each out_edge i
            # for each out_edge j
                # break if the years diverge
                # break if they relate to the same worker
                # create an edge between the 2 workers in the new graph
                # add attrs (year, time together)
