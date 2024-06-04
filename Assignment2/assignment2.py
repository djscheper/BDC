#!/usr/bin/env python

""""""

__author__ = "Dennis Scheper"
__status__ = "Production"
__version__ = "v1.0"
__date__ = "30/05/2024"
__contact__ = "d.j.scheper@st.hanze.nl"

from phred import PhredScoreCalculator
from multiprocessing.managers import BaseManager
import multiprocessing as mp
import time, queue
import argparse as ap

POISONPILL = "Grim Reaper"

def parse_arguments():
    """
    """
    argparser = ap.ArgumentParser(description="Script voor Opdracht 2 van Big Data Computing;  Calculate PHRED scores over the network.")
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
    """ Create a manager for the server, listening on the given port.
        Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    class QueueManager(BaseManager):
        pass

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=(host, port), authkey=authkey)
    manager.start()
    print(f'[Status] Server started at port {host} : {port}')
    return manager


def runserver(port, host, file, n_chuncks, outputfile):
    """
    added chuncks, obj, and host
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

    time.sleep(5)
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
    """
    class ServerQueueManager(BaseManager):
        """
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
    """
    #try except
    manager = make_client_manager(port, b"somesecretkey", host)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)


def run_workers(job_q, result_q, num_processes):
    """
    """
    processes = []
    for p in range(num_processes):
        temP = mp.Process(target=peon, args=(job_q, result_q))
        processes.append(temP)
        temP.start()
    print(f"[Status] Started {len(processes)} workers!")
    for temP in processes:
        temP.join()


def peon(job_q, result_q):
    """
    """
    my_name = mp.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print(f"[Status] Killing: {my_name}")
                return
            else:
                try:
                    result = job['fn'](job['arg'])
                    print(f"[Status] Peon f{my_name} is working on: {job['arg']}")
                    result_q.put({'job': job, 'result': result})
                except NameError:
                    print(f"[ERROR] We cannot find {my_name} anywhere...")
                    result_q.put({'job': job, 'result': "No results!"})

        except queue.Empty:
            print(f"Closing {my_name}")
            time.sleep(5)


if __name__ == "__main__":
    args = parse_arguments()

    if args.s and not args.chunks:
        ap.error("--chunks is required when running in server mode")

    if args.s:
        args.o = None if not hasattr(args, 'o') else args.o
        server = mp.Process(target=runserver, args=(args.port, args.host, args.fastq_files, args.chunks, args.o))
        server.start()
        time.sleep(1)

    if args.c:
        client = mp.Process(target=runclient, args=(args.n, args.host, args.port))
        client.start()
        client.join()