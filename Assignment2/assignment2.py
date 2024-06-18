#!/usr/bin/env python3

"""Big Data Computing (BDC) assignment 2. 

By Dennis Scheper (373689)

Usage:
    Start server:
        python3 assignment2.py -s rnaseqfile.fastq --host <een workstation> --port <een poort> --chunks <een getal>
    Start client:
        python3 assignment2.py -c --host <diezelfde host als voor server> --port <diezelfde poort als voor server> -n <aantal cpus in client computer>
"""

__author__ = "Dennis Scheper"
__status__ = "Production"
__version__ = "v1.0"
__date__ = "30/05/2024"
__contact__ = "d.j.scheper@st.hanze.nl"

import time
import queue
import multiprocessing as mp
from multiprocessing.managers import BaseManager
import argparse as ap
from phred import PhredScoreCalculator
import os

POISONPILL = "Grim Reaper"

def parse_arguments():
    """
    Defines command-line arguments.
    """
    argparser = ap.ArgumentParser(description="Script voor Opdracht 2 van Big Data Computing; Calculate PHRED scores over the network.")
    mode = argparser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-s", action="store_true", help="Run the program in Server mode; see extra options needed below")
    mode.add_argument("-c", action="store_true", help="Run the program in Client mode; see extra options needed below")
    server_args = argparser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument("-o", action="store", dest="csvfile", type=ap.FileType('w', encoding='UTF-8'),
                        required=False, help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
    server_args.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='*', help="Minstens 1 Illumina Fastq Format file om te verwerken")
    server_args.add_argument("--chunks", action="store", type=int, required=False)

    client_args = argparser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument("-n", action="store",
                        dest="n", required=False, type=int,
                        help="Aantal cores om te gebruiken per host.")
    client_args.add_argument("--host", action="store", type=str, help="The hostname where the Server is listening")
    client_args.add_argument("--port", action="store", type=int, help="The port on which the Server is listening")

    return argparser.parse_args()


def make_server_manager(port, authkey, host):
    """ 
    Create a manager for the server, listening on the given port.
    Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    class QueueManager(BaseManager):
        """
        Encodes for the queue responsible for chunck processing.
        """
        pass

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=(host, port), authkey=authkey)
    manager.start()
    print(f'[Status] Server started at port {host} : {port}')
    return manager


def runserver(port, host, file, n_chuncks, outputfile):
    """
    Runs the server by making a make_sever_manager() function,
    Also, this functions distributes the chuncks over different peons (workers).
    Shuts down the server when there is no more work left to do.

    Returns nothing.
    """
    manager = make_server_manager(port, b'somesecretkey', host)
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    calculator = PhredScoreCalculator(file[0], n_chuncks)
    calculator.make_chuncks()
    chuncks = calculator.get_chunks()

    if not file:
        print("[Error] No data!")
        return

    for chunck in chuncks:
        shared_job_q.put({'fn': calculator.process_file, 'arg': chunck})

    time.sleep(2)
    results = []

    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            if len(results) == len(chuncks):
                print("[Status] All records have been processed.")
                break
        except queue.Empty:
            time.sleep(1)
            continue

    print("[Status] Time to kill some peons!")
    shared_job_q.put(POISONPILL)

    time.sleep(20)
    print("[Status] Shutting down the server...")

    manager.shutdown()
    averages = calculator.calculate_average([res["result"] for res in results])

    if outputfile:
        calculator.csv_writer(averages, outputfile=outputfile, multiple=False)
    else:
        for key, value in averages.items():
            print(f"{key}, {value}")


def make_client_manager(port, authkey, host):
    """
    Create a manager for the client, listening on the given port.
    Return a manager object with get_job_q and get_result_q methods.
    """

    class ServerQueueManager(BaseManager):
        """
        Encodes for the queue responsible for chunck processing.
        """
        pass

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(host, port), authkey=authkey)
    manager.connect()

    print(f"[Status] Client connected to {host} : {port}")
    return manager


def runclient(num_processes, host, port):
    """
    Runs the client and processes the given file by
    running some peons (workers).
    """
    manager = make_client_manager(port, b'somesecretkey', host)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)


def run_workers(job_q, result_q, num_processes):
    """
    This function makes sure the right amount of peons start working;
    each chunck gets its own peon (worker). Puts results into
    results_q.
    """
    processes = []
    for _ in range(num_processes):
        temp = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temp)
        temp.start()
    print(f"[Status] Started {len(processes)} workers!")
    for temp in processes:
        temp.join()


def peon(job_q, result_q):
    """
    Defines the logic behind a peon. Runs until the queue is empty, and
    adds results to the result_q. The peon is killed if either the POISONPILL
    is added to the queue, or when there is no more work to do.
    """
    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            print(job)
            if job == POISONPILL:
                print('done')
                job_q.put(POISONPILL)
                print(f"[Status] Killing: {my_name}")
                return
            else:
                try:
                    result = job['fn'](job['arg'])
                    print(f"[Status] Peon {my_name} is workin   g on: {job['arg']}")
                    result_q.put({'job': job, 'result': result})
                except NameError:
                    print(f"[ERROR] We cannot find {my_name} anywhere...")
                    result_q.put({'job': job, 'result': "No results!"})
        except queue.Empty or EOFError:
            print(f"Closing {my_name}")
            time.sleep(5)


if __name__ == "__main__":
    args = parse_arguments()

    if args.s and not args.chunks:
        print("--chunks is required when running in server mode")

    if args.s:
        args.o = None if not hasattr(args, 'o') else args.o
        server = mp.Process(target=runserver, args=(args.port, args.host, args.fastq_files, args.chunks, args.o))
        server.start()
        time.sleep(1)

    if args.c:
        client = mp.Process(target=runclient, args=(args.n, args.host, args.port))
        client.start()
        client.join()
