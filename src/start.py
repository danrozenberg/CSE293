import sys
import subprocess

procs = []
for i in range(1,5):
    proc = subprocess.Popen([sys.executable,
                             "connect_workers.py",
                             str(i)])
    procs.append(proc)

for proc in procs:
    proc.wait()



