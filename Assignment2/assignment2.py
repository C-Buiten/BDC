"""
Assignment 2
"""

import argparse as ap
import multiprocessing
import multiprocessing.managers
import sys
import time
import queue
from assignment1_copy import MultiFastq


POISONPILL = "MEMENTOMORI"
ERROR = "DOH"


def make_server_manager(host, port, authkey):
    """
    Create a manager for the server, listening on the given port.
    Return a manager object with get_job_q and get_result_q methods.
    """
    job_q = queue.Queue()
    result_q = queue.Queue()

    # This is based on the examples in the official docs of multiprocessing.
    # get_{job|result}_q return synchronized proxies for the actual Queue
    # objects.
    class QueueManager(multiprocessing.managers.BaseManager):
        """
        Child class of BaseManager
        """

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    manager = QueueManager(address=(host, port), authkey=authkey)
    manager.start()
    print(f'Server started at port {port}s')
    return manager


def runserver(obj, chunks, host, port):
    """
    Run in server mode and send data
    """
    # Start a shared manager server and access its queues
    manager = make_server_manager(host, port, b'whathasitgotinitspocketsesss?')
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    if not chunks:
        print("Nothing to do here!")
        return

    for chunk in chunks:
        shared_job_q.put({'fn': obj.read_file, 'args': (chunk)})

    print("Sending data!")
    time.sleep(2)

    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            if len(results) == len(chunks):
                print("All chunks have been processed and added to the results")
                break
        except queue.Empty:
            time.sleep(1)
            continue
    # Tell the client process no more data will be forthcoming
    print("There will no more data forthcoming...")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Server is finished")
    manager.shutdown()


def make_client_manager(host, port, authkey):
    """
    Create a manager for a client. This manager connects to a server on the
    given address and exposes the get_job_q and get_result_q methods for
    accessing the shared queues from the server.
    Return a manager object.
    """

    class ServerQueueManager(multiprocessing.managers.BaseManager):
        """Server queue manager"""

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(host, port), authkey=authkey)
    manager.connect()

    print(f'Client connected to {host}s:{port}s')
    return manager


def runclient(num_processes, host, port):
    """
    Starting a local client manager.
    """
    manager = make_client_manager(host, port, b'whathasitgotinitspocketsesss?')
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, num_processes)


def run_workers(job_q, result_q, num_processes):
    """
    Local client, the commandline core arguments of cores are turned into workers
    """
    processes = []
    for i in range(num_processes):
        temp = multiprocessing.Process(target=peon, args=(job_q, result_q))
        processes.append(temp)
        temp.start()
    print(f"Started {len(processes)}s workers!")
    for temp in processes:
        temp.join()


def peon(job_q, result_q):
    """
    Manage the jobs
    """
    my_name = multiprocessing.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print(f"This job died: {my_name}")
                return
            try:
                result = job['fn'](job['args'])
                print(f"Worker {my_name} busy with {job['args']}!")
                result_q.put({'job': job, 'result': result})
                print(f"Peon {my_name}s Workwork on {job['args']}s!")
            except NameError:
                print("Worker cannot be found...")
                result_q.put({'job': job, 'result': ERROR})

        except queue.Empty:
            print(f"Sleep time for: {my_name}")
            time.sleep(1)


def arg_parser():
    """
    Argument parser for the command line.
    :return: The user-given command line arguments.
    """
    argparser = ap.ArgumentParser(
        description="Script voor Opdracht 2 van Big Data Computing;  Calculate PHRED scores over the network.")
    mode = argparser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-s", action="store_true", help="Run the program in Server mode; see extra options needed below")
    mode.add_argument("-c", action="store_true", help="Run the program in Client mode; see extra options needed below")
    server_args = argparser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument("-o", action="store", dest="csvfile", type=ap.FileType('w', encoding='UTF-8'),
                             required=False,
                             help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
    server_args.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='*',
                             help="Minstens 1 Illumina Fastq Format file om te verwerken")
    server_args.add_argument("--chunks", action="store", type=int, required=False)

    client_args = argparser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument("-n", action="store",
                             dest="n", required=False, type=int,
                             help="Aantal cores om te gebruiken per host.")
    client_args.add_argument("--host", action="store", type=str, help="The hostname where the Server is listening")
    client_args.add_argument("--port", action="store", type=int, help="The port on which the Server is listening")

    args = argparser.parse_args()

    return args


def main():
    """
    Main function of the script, here the command line arguments are collected and processed.
    :return int: 0 if the script is executed correctly, otherwise an error code.
    """
    # Collect command line arguments
    args = arg_parser()
    files = args.fastq_files
    output = args.csvfile
    cores = args.n

    if args.s:
        print("Started Server side")
        operator = MultiFastq(files, output, cores)

        server = multiprocessing.Process(target=runserver,
                                         args=(operator.calc_scores, args.host, args.port))
        server.start()
        time.sleep(1)

    if args.c:
        print("Started client side")
        client = multiprocessing.Process(target=runclient, args=(args.n, args.host, args.port,))
        client.start()
        client.join()


if __name__ == "__main__":
    sys.exit(main())
