import logging
import sys
import subprocess
import connect_workers

if __name__ == '__main__':

    logging.basicConfig(format='%(asctime)s %(message)s',
    datefmt='%d %b - %H:%M:%S -',
    level=logging.WARN)

    connector = connect_workers.WorkerConnector(2014)
    connector.min_days_together = 90
    connector.start_connect_worker_proc()
