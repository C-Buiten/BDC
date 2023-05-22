import argparse as ap
import multiprocessing
import sys
import csv


def arg_parser():
    """
    Argument parser for the command line.
    :return: The user-given command line arguments.
    """
    argparser = ap.ArgumentParser(description="Script voor Opdracht 1 van Big Data Computing")
    argparser.add_argument("-n", action="store",
                           dest="n",
                           required=True, type=int,
                           help="Aantal cores om te gebruiken.")
    argparser.add_argument("-o", action="store", dest="csvfile",
                           type=ap.FileType('w', encoding='UTF-8'),
                           required=False,
                           help="CSV file om de output in op te slaan. "
                                "Default is output naar terminal STDOUT")
    argparser.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='+',
                           help="Minstens 1 Illumina Fastq Format file om te verwerken")
    args = argparser.parse_args()

    return args
