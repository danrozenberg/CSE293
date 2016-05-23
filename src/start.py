import logging
import sys
import subprocess

logging.basicConfig(format='%(asctime)s %(message)s',
datefmt='%d %b - %H:%M:%S -',
level=logging.WARN)

procs = []
first_year = 1994
last_year = 2004
for year in range(first_year,last_year + 1):
    proc = subprocess.Popen([sys.executable,
                             "connect_workers.py",
                             str(year),
                             str(year)])
    procs.append(proc)

for proc in procs:
    proc.wait()

logging.warn("All done!")
