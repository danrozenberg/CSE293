import logging
import sys
import subprocess
import connect_workers


logging.basicConfig(format='%(asctime)s %(message)s',
datefmt='%d %b - %H:%M:%S -',
level=logging.WARN)

logging.warn("Clearing edges output folder")
output_folder = "../output_edges/"
connect_workers.delete_files(output_folder)

procs = []
total_procs = 3
for i in range(1,total_procs+1):
    proc = subprocess.Popen([sys.executable,
                             "connect_workers.py",
                             str(i),
                             str(total_procs)])
    procs.append(proc)

for proc in procs:
    proc.wait()


logging.warn("Will connect edges now")
graph = connect_workers.add_edges_from_disk(output_folder)

logging.warn("Will save the graph now")
graph.save_graph("../output_graphs/cds_connected" + str(90) + "_days.graph")

logging.warn("All done!")
