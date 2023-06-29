#!/usr/local/bin/python3

"""
Assignment 4
"""

import sys
import argparse as ap
import statistics


def validate(fastq):
    """
    Checks Fastq file for validity.
    :param fastq:
    :return: Filename,Validity(,Min_length,Max_length,Average_length)
    """
    min_len = 10000000
    max_len = 0
    lengths = []

    with open(fastq[0], "r", encoding="utf-8") as file:
        for lines, line in enumerate(file):
            if lines % 4 == 1:
                nuc_len = len(line.strip())
                lengths.append(nuc_len)
                min_len = min(min_len, nuc_len)
                max_len = max(max_len, nuc_len)
            if lines % 4 == 3:
                score_len = len(line.strip())
                if score_len != nuc_len:
                    return str(fastq[0].split("/")[-1]), "False"

        if (lines + 1) % 4:
            return str(fastq[0].split("/")[-1]), "False"

        return str(fastq[0].split("/")[-1]), "True", str(min_len), str(max_len), str(statistics.mean(lengths))


def arg_parser():
    """
    Argument parser for command line arguments.
    """

    argparser = ap.ArgumentParser(description="Script voor Opdracht 4 van Big Data Computing;" \
                                              "Checks validity of FastQ files.")

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

    output = validate(args.fastq_files)

    print(",".join(output))


if __name__ == '__main__':
    sys.exit(main())
