"""
Assignment 1 Copy
"""

import csv

def calc_scores(fastq_file, start, end):
    """
    Calculates average phred scores per base of several quality lines and returns
    a dict with the averages.
    :param end:
    :param start:
    :param fastq_file:
    """

    # Create dictionaries
    temp_dict = {i: chr(i) for i in range(128)}
    ascii_dict = {a: z for z, a in temp_dict.items()}
    count_dict = {}
    length_per_base = {}

    with open(fastq_file[0], "r", encoding="utf-8") as file:
        for i, line in enumerate(file):
            if start <= i <= end:
                for count, char in enumerate(line):
                    if count in count_dict:
                        count_dict[count] += ascii_dict[char] - 33
                        length_per_base[count] += 1
                    else:
                        count_dict[count] = ascii_dict[char] - 33
                        length_per_base[count] = 1

        # Average the value by
        for key in count_dict:
            count_dict[key] = round(count_dict[key] / length_per_base[key], 4)

        return count_dict


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
