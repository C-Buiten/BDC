#!/usr/local/bin/python3

"""
Assignment 2
"""

import sys
import argparse as ap


def arg_parser():
    """
    Argument parser for command line arguments.
    """

    argparser = ap.ArgumentParser(description="Script voor Opdracht 2 van Big Data Computing;" \
                                              "Calculate PHRED scores over the network.")

    argparser.add_argument("fastq_files", action="store",
                           type=str, nargs='*',
                           help="Minstens 1 Illumina Fastq Format file om te verwerken")

    args = argparser.parse_args()

    return args


def main():
    """
    Main function of the script.
    """

    # Collect command line arguments
    args = arg_parser()

    args


if __name__ == '__main__':
    sys.exit(main())
