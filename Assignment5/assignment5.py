# /data/dataprocessing/interproscan/all_bacilli.tsv
# /data/datasets/EBI/interpro/refseqscan

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


def main(args):
    """
    Call InterPRO Spark functions
    :param args:
    :return:
    """
    answers = InterPRO(args)

    write_output(answers, "output.csv")


if __name__ == "__main__":
    sys.exit(main(sys.argv))
