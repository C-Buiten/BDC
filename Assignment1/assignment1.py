"""
Assignment 1
"""

import argparse as ap
import multiprocessing
import sys
import csv
from itertools import zip_longest
import statistics


class MultiFastq:
    """
    Class that calculates average phred scores per base position.
    """

    def __init__(self, files, output, cores):
        self.files = files
        self.output = output
        self.cores = cores

    def calc_scores(self):
        """
        Divide quality scores from all given files over the given number of cores.
        :return:
        """
        avg_scores_per_file = []
        for file in self.files:
            with open(file, "r") as fastq:
                data = fastq.read().split("\n")
                scores = []
                for line in range(3, len(data), 4):
                    scores.append(data[line])

            with multiprocessing.Pool(self.cores) as pool:
                score_lists = pool.map(self.get_scores, scores)

            avg_scores = []
            for pos in [list(filter(None, i)) for i in zip_longest(*score_lists)]:
                avg_scores.append(statistics.mean(pos))

            avg_scores_per_file.append(avg_scores)

        self.write_output(avg_scores_per_file)

    def get_scores(self, scores):
        """
        Get scores from the files as determined by calc_scores.
        :return:
        """
        score_lists = []
        for score in scores:
            num_score = ord(score) - 33
            score_lists.append(num_score)

        # print(score_lists)

        return score_lists

    def write_output(self, avg_scores_per_file):
        """
        Writes base positions and average phred scores to output.
        :return:
        """
        if self.output is None:
            for avg_scores in avg_scores_per_file:
                print(self.files[avg_scores_per_file.index(avg_scores)])
                for i, avg in enumerate(avg_scores, start=1):
                    print(f"{i},{avg}")
        else:
            with open(self.output, "a", newline='') as file:
                csv_obj = csv.writer(file, delimiter=",")
                for avg_scores in avg_scores_per_file:
                    for i, avg in enumerate(avg_scores, start=1):
                        csv_obj.writerow([i, avg])


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
                           type=str,
                           required=False,
                           help="CSV file om de output in op te slaan. "
                                "Default is output naar terminal STDOUT")
    argparser.add_argument("fastq_files", action="store", type=str, nargs='+',
                           help="Minstens 1 Illumina Fastq Format file om te verwerken")
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

    operator = MultiFastq(files, output, cores)
    operator.calc_scores()


if __name__ == "__main__":
    sys.exit(main())
