"""
Assignment 1 Copy
"""

import csv
import sys
import argparse as ap


def calc_scores(fastq_file, start, end):
    """
    Calculate quality scores for all given chunks.
    :return:
    """

    # Create scores dicts
    count_dict = {}
    length_per_base = {}

    with open(fastq_file[0], "r", encoding="utf-8") as file:
        data = file.read().split("\n")
        prev_line = ""
        for i, line in enumerate(data):
            if start <= i <= end:
                if prev_line == "+":
                    print(line)
                    for pos, char in enumerate(line.strip(), start=1):
                        if pos in count_dict:
                            count_dict[pos] += ord(char) - 33
                            length_per_base[pos] += 1
                        else:
                            count_dict[pos] = ord(char) - 33
                            length_per_base[pos] = 1
            prev_line = line


        # Average the value by
        for key in count_dict:
            count_dict[key] = round(count_dict[key] / length_per_base[key], 4)

        return count_dict


def split_files(filename, cores):
    """Function to allocate chunk data to cores"""
    with open(filename[0], 'r', encoding='utf-8') as open_file:
        for line, _ in enumerate(open_file):
            pass
    file_len = line + 1

    chunk_size = int(file_len / cores[0])

    while chunk_size % 4 != 0:
        chunk_size += chunk_size % 4

    line_list = [[x, x + chunk_size] for x in range(0, file_len, chunk_size)]
    return line_list


def write_output(output, avg_scores_per_file):
    """
    Writes base positions and average phred scores to output.
    :return:
    """
    if output is None:
        for avg_scores in avg_scores_per_file:
            print(avg_scores_per_file.index(avg_scores))
            for i, avg in enumerate(avg_scores, start=1):
                print(f"{i},{avg}")
    else:
        with open(output, "a", newline='') as file:
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
    argparser.add_argument("fastq_files", action="store", type=str, nargs='+',
                           help="Minstens 1 Illumina Fastq Format file om te verwerken")
    args = argparser.parse_args()

    return args


def main():
    """
    Main function of the script.
    """
    # Collect command line arguments
    args = arg_parser()
    files = args.fastq_files

    print(calc_scores(files, 0, 19))


if __name__ == "__main__":
    sys.exit(main())
