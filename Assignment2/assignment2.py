#!/usr/local/bin/python3

"""
Assignment 2
"""

import multiprocessing
import multiprocessing.managers
import sys
import time
import queue
import argparse as ap
from assignment1_redo import calc_scores, split_files


POISONPILL = "MEMENTOMORI"
ERROR = "DOH"
AUTHKEY = b'whathasitgotinitspocketsesss?'


def make_server_manager(port, host):
    """
    Create a server manager on the given port.
    Returns a manager object.
    """

    job_q = queue.Queue()
    result_q = queue.Queue()

    class QueueManager(multiprocessing.managers.BaseManager):
        """Server Queue Manager"""

    QueueManager.register('get_job_q', callable=lambda: job_q)
    QueueManager.register('get_result_q', callable=lambda: result_q)

    while True:
        try:
            manager = QueueManager(address=(host, port), authkey=AUTHKEY)
            manager.start()
            print(f'Server starting using port {port}s')
            break
        except (OSError, EOFError):
            port += 1

    return manager


def runserver(function, fastq, cores, port, host, output_file=None):
    """
    Runs server
    :param output_file: Destination csv, default stdout
    :param function: Function to run
    :param fastq: fastq file
    :param cores: amount of cores to spread data over
    :param port: Port number
    :param host: IP number
    """
    # Start a shared manager server and access its queues
    manager = make_server_manager(port, host)
    shared_job_q = manager.get_job_q()
    shared_result_q = manager.get_result_q()

    chunks = split_files(fastq, cores)

    if not chunks:
        print("Zzzz...")
        return
    print("Data incoming!")
    for chunk in chunks:
        shared_job_q.put({'fn': function, 'arg': (fastq, chunk[0], chunk[1])})

    time.sleep(2)

    results = []
    while True:
        try:
            result = shared_result_q.get_nowait()
            results.append(result)
            print("Got result!", result)
            if len(results) == len(chunks):
                print("Results are in!")
                break
        except queue.Empty:
            time.sleep(1)
            continue

    # Tell the client process no more data will be forthcoming
    print("There will be no more data forthcoming...")
    shared_job_q.put(POISONPILL)
    # Sleep a bit before shutting down the server - to give clients time to
    # realize the job queue is empty and exit in an orderly way.
    time.sleep(5)
    print("Aaaaaaaaaaaaand we're done for the server!")
    manager.shutdown()

    if output_file:
        with open(output_file, "w", encoding='UTF-8') as myfile:
            for pos in results[0]['result']:
                output = str(pos+1) + "," + str(results[0]['result'][pos])
                myfile.write(output + "\n")
    else:
        for pos in results[0]['result']:
            output = str(pos+1) + "," + str(results[0]['result'][pos])
            print(output)
            sys.stdout.write(output + '\n')


def make_client_manager(host, port, authkey):
    """ Create a manager for a client. This manager connects to a server on the
        given address and exposes the get_job_q and get_result_q methods for
        accessing the shared queues from the server.
        Return a manager object.
    """

    class ServerQueueManager(multiprocessing.managers.BaseManager):
        """wrapper"""

    ServerQueueManager.register('get_job_q')
    ServerQueueManager.register('get_result_q')

    manager = ServerQueueManager(address=(host, port), authkey=authkey)

    manager.connect()

    print(f'Client connected to {host}s:{port}s')
    return manager


def runclient(processes, host, port):
    """
    Running client and connecting to server
    :param processes: Amount of processes
    :param host: Host IP
    :param port: Port number
    """
    manager = make_client_manager(host, port, AUTHKEY)
    job_q = manager.get_job_q()
    result_q = manager.get_result_q()
    run_workers(job_q, result_q, processes)


def run_workers(job_q, result_q, num_processes):
    """
    Run workers for the client
    :param job_q:
    :param result_q:
    :param num_processes:
    :return:
    """
    processes = []
    for _ in range(num_processes):
        temp = multiprocessing.Process(target=peon, args=(job_q, result_q))
        processes.append(temp)
        temp.start()
    print(f"Started {len(processes)}s workers!")
    for temp in processes:
        temp.join()


def peon(job_q, result_q):
    """Peon func"""
    my_name = multiprocessing.current_process().name
    while True:
        try:
            job = job_q.get_nowait()
            if job == POISONPILL:
                job_q.put(POISONPILL)
                print("RIP Peon", my_name)
                return
            try:
                result = job['fn'](job['arg'][0], job['arg'][1], job['arg'][2])
                print(f"Peon {my_name}s Workwork on {job['arg']}s!")
                result_q.put({'job': job, 'result': result})
            except NameError:
                print("Can't find Waldo!")
                result_q.put({'job': job, 'result': ERROR})

        except queue.Empty:
            print(f"Putting {my_name} to sleep.")
            time.sleep(1)


def arg_parser():
    """
    Argument parser for command line arguments.
    """

    argparser = ap.ArgumentParser(description="Script voor Opdracht 2 van Big Data Computing;"
                                              "Calculate PHRED scores over the network.")
    mode = argparser.add_mutually_exclusive_group(required=True)
    mode.add_argument("-s", "--server",
                      action="store_true",
                      help="Run the program in Server mode;"
                           "see extra options needed below")
    mode.add_argument("-c", "--client",
                      action="store_true", help="Run the program in Client mode;"
                                                "see extra options needed below")
    server_args = argparser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument("-o", "--output", required=False,
                             help="CSV file om de output in op te slaan."
                                  "Default is output naar terminal STDOUT")
    server_args.add_argument("fastq_files", action="store",
                             type=str, nargs='*',
                             help="Minstens 1 Illumina Fastq Format file om te verwerken")
    server_args.add_argument("-chunks", "--chunks", action="store",
                             type=int, nargs='*',
                             help="Hoeveel chunks het bestand in gedeeld moet worden")
    client_args = argparser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument("-n", "--ncores", action="store",
                             required=False, type=int,
                             help="Aantal cores om te gebruiken per host.")

    client_args.add_argument("--host", action="store",
                             type=str,
                             help="The hostname where the Server is listening")

    client_args.add_argument("--port", action="store",
                             type=int,
                             help="The port on which the Server is listening")

    args = argparser.parse_args()

    return args


def main():
    """
    Main function of the script.
    """
    # Collect command line arguments
    args = arg_parser()

    if args.server:
        print("Started Server side")
        server = multiprocessing.Process(target=runserver,
                                         args=(calc_scores, args.fastq_files, args.chunks,
                                               args.port, args.host, args.output))
        server.start()
        time.sleep(1)

    if args.client:
        print("Started client side")
        client = multiprocessing.Process(target=runclient, args=(args.ncores, args.host, args.port,))
        client.start()
        client.join()


if __name__ == "__main__":
    sys.exit(main())
