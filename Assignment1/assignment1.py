#!/usr/bin/env python3

"""
Calculates the average PHRED score per base position in a given FASTQ file.
Final results may be written to a csv file, if desired by the user. Otherwise,
results are printed to command line.

Usage:
    python3 assignment1.py -n <n_cpus> [-o <csv file>] fastq_files [FASTQ_file1, FASTQ_fileN]
"""

import argparse as ap
import csv
import multiprocessing as mp
import sys
import numpy as np

__author__ = "Dennis Scheper (373689)"
__date__ = "18/05/2023"
__version__ = "v1.0"
__contact__ = "d.j.scheper@st.hanze.nl"

def define_arguments() -> ap.Namespace:
    """
    Defines arguments for command-line usage.
    :return: argparser
    """
    argparser = ap.ArgumentParser(
        description="""Script voor Opdracht 1 van Big Data Computing.
        Delivered by Dennis Scheper (373689).""")
    argparser.add_argument("-n", action="store",
                        dest="n", required=True, type=int,
                        help="Aantal cores om te gebruiken.")
    argparser.add_argument("-o", action="store", dest="csvfile",
                           type=ap.FileType('w', encoding='UTF-8'),
                           required=False, help="""CSV file om de output in op te slaan.
                        Default is output naar terminal STDOUT""")
    argparser.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='+',
                           help="Minstens 1 Illumina Fastq Format file om te verwerken")

    return argparser.parse_args()


def read_fastq(file: str, part_size=0.1) -> list:
    """
    Read in a fastQ file, splits each part of a read at a "\n" symbol, and
    only selects for the quality line. Resulting in a big lists of quality
    lines. This is list is spliced into innerlists; amount of quality lines 
    in one innerlist is a determined by the part_size parameter, which
    default is 0.1 of the total length of the initial larger list. Each
    innerlist can be used by one process of the Pool.map() function.
    :param: inputfile, fastq file
    :param: part_size, size of each innerlist (default: 0.1)
    :return: a list containing innerlists with quality lines
    """
    with open(file, 'r', encoding='utf-8') as inputfile:
        try:
            lines = inputfile.read().splitlines()[3::4] # omit \n and pick only quality line
        except FileNotFoundError:
            print(f"Couldn't find {inputfile}")
        except IOError as error:
            print(f"An error occured while trying to open {inputfile}. The error: {error}")

    # split into multiple innerlists for multiprocessing
    part = int(part_size*len(lines))
    sublists = [lines[i:i+part] for i in range(0, len(lines), part)]

    return sublists


def calculate_phred(q_strings: list) -> list:
    """
    Calculates the PHRED score for all characters in a read.
    :param: q_strings contains a part with quality strings
    :return: the resulting list with PHRED scores
    """
    # initialize list with length of one read (allows us to work per base position)
    phreds = [0] * len(q_strings[0])

    for quality in q_strings:
        for index, quality in enumerate(quality):
            try:
                phreds[index] += ord(quality) - 33
            except IndexError:
                phreds.append(ord(quality) - 33)

    return [score / len(q_strings) for score in phreds]


def write_csv(csvfile: str, results: list) -> int:
    """
    Writes results to a csv file per given FASTQ file.
    Name of resulting csv file will be formatted differently if there are multiple FastQ files:
        `[name of inputfile].[name of outputfile].csv`
    Otherwise, the resulting csv will bear the name given by the user.
    :param: csvfile, csvfile name
    :param: results, PHRED score list
    :return: 0
    """
    with open(csvfile, 'w', newline="", encoding='utf-8') as csv_output:
        csvwriter = csv.writer(csv_output)

        for base_nmr, av_phred in enumerate(results):
            csvwriter.writerow([base_nmr, av_phred])

    return 0


def main() -> int:
    """
    Main function in which command-line arguments are parsed and functions executed.
    """
    args = define_arguments()

    for file in args.fastq_files:
        data = read_fastq(file.name)

        with mp.Pool(processes=args.n) as pool:
            results = pool.map(calculate_phred, data)

        average = np.mean(results, axis=0)

        if args.csvfile is not None:
            csvfile = f"{str(file.name)}.{str(args.csvfile.name)}" if len(args.fastq_files)>1 else args.csvfile.name
            write_csv(csvfile, average)
        else:
            print(f"Results for {file.name}")
            csv_writer = csv.writer(sys.stdout)
            for index, row in enumerate(average, start=1):
                csv_writer.writerow([index, row])

    return 0

if __name__ == "__main__":
    sys.exit(main())