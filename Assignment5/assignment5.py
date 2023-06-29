# /data/dataprocessing/interproscan/all_bacilli.tsv
# /data/datasets/EBI/interpro/refseqscan

import argparse as ap
import sys
import csv


def InterPRO(args):
    """
    InterPRO spark function
    :param args:
    :return:
    """
    return 1


def write_output(results, output_file):
    """
    Writes output to csv.
    :return:
    """

    with open(output_file, "a", newline='') as file:
        csv_obj = csv.writer(file, delimiter=",")
        for question, answer in enumerate(results, start=1):
            csv_obj.writerow([question, answer])


def arg_parser():
    """
    Argument parser for the command line.
    :return: The user-given command line arguments.
    """
    argparser = ap.ArgumentParser(description="Script voor Opdracht 1 van Big Data Computing")

    argparser.add_argument("tsv_file", action="store", type=str,
                           help="1 TSV file")
    args = argparser.parse_args()

    return args

def main():
    """
    Call InterPRO Spark functions
    :param args:
    :return:
    """

    args = arg_parser()

    answers = InterPRO(args)

    write_output(answers, "output.csv")


if __name__ == "__main__":
    sys.exit(main())
