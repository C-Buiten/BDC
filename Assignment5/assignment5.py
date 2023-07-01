# /data/dataprocessing/interproscan/all_bacilli.tsv
# /data/datasets/EBI/interpro/refseqscan

import argparse as ap
import sys
import csv
import pyspark.sql
import pyspark.sql.functions


def InterPRO(input_file):
    """
    InterPRO spark function
    :param input_file: Input InterPROScan.tsv
    :param args:
    :return:
    """
    questions = [one, two, three, four, five, six, seven, eight, nine, ten]
    results = []
    for function in questions:
        results.append(function(input_file))

    return results


def one(file):
    """How many distinct protein annotations are found in the dataset? I.e. how many distinc InterPRO numbers are there?
    def explain(self, extended=False):
        if extended:
            print(self._jdf.queryExecution().toString())
        else:
            print(self._jdf.queryExecution().simpleString())
    """
    distinct = file.select("_c11").distinct().count()
    explain = file._jdf.queryExecution().toString().split("\n\n")[3]

    return [distinct, explain]


def two(file):
    """How many annotations does a protein have on average?"""
    avg_anno = file.groupBy("_c0").count().select(pyspark.sql.functions.mean("count")).collect()[0][0]
    explain = file._jdf.queryExecution().toString().split("\n\n")[3]

    return [avg_anno, explain]


def three(file):
    """What is the most common GO Term found?"""
    go_term = file.groupby('_c13').count().orderBy(pyspark.sql.functions.desc('count')).take(1)
    explain = file._jdf.queryExecution().toString().split("\n\n")[3]

    return [go_term, explain]


def four(file):
    """What is the average size of an InterPRO feature found in the dataset?"""
    return


def five(file):
    """What is the top 10 most common InterPRO features?"""
    return


def six(file):
    """If you select InterPRO features that are almost the same size (within 90-100%) as the protein itself, what is the top10 then?"""
    return


def seven(file):
    """If you look at those features which also have textual annotation, what is the top 10 most common word found in that annotation?"""
    return


def eight(file):
    """And the top 10 least common?"""
    return


def nine(file):
    """Combining your answers for Q6 and Q7, what are the 10 most commons words found for the largest InterPRO features?"""
    return


def ten(file):
    """What is the coefficient of correlation ($R^2$) between the size of the protein and the number of features found?"""
    return


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
    :return:
    """

    args = arg_parser()

    sparks = pyspark.sql.SparkSession.builder.master('local[16]').appName("SparkTime").getOrCreate()
    input_file = sparks.read.csv(args.tsv_file, header=False, sep="\t")

    answers = InterPRO(input_file)

    write_output(answers, "output.csv")


if __name__ == "__main__":
    sys.exit(main())
