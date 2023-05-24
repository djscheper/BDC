#!/usr/bin/env python3

"""
Calculates the average PHRED score per base position in a given FASTQ file.
Final results may be written to a csv file, if desired by the user.

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


def calculate_phred(q_string: str) -> list:
    """
    Calculates the PHRED score for all characters in a read.
    :param: quality string of a read
    :return: the resulting list with PHRED scores 
    """
    return [ord(q) - 33 for q in q_string]


def write_csv(csvfile: str, results: list) -> int:
    """
    Writes results to a csv file per given FASTQ file.
    Name of resulting csv file will be formatted differently if there are multiple FastQ files:
        `[name of inputfile].[name of outputfile].csv`
    Otherwise, the resulting csv will bear the name given by the user.
    :param: csvfile csvfile name
    :param: results PHRED score list
    :param: name name of inputfile
    :param: multiple_files whether more than one inputfile was given
    :return: 0
    """
    header = ['base_nr', 'average_phred_score']
    with open(csvfile, 'w', newline="", encoding='utf-8') as csv_output:
        csvwriter = csv.writer(csv_output)
        csvwriter.writerow(i for i in header)

        for base_nmr, av_phred in enumerate(results, start=1):
            csvwriter.writerow([base_nmr, av_phred])

    return 0


def main() -> int:
    """
    Main function in which command-line arguments are parsed and functions executed.
    """
    args = define_arguments()

    for file in args.fastq_files:
        try:
            with open(file.name, 'r', encoding='utf-8') as inputfile:
                lines = inputfile.read().splitlines() # remove \n
                print(lines[3::4])
        except FileNotFoundError:
            print(f"Couldn't find {inputfile}")
        except IOError as error:
            print(f"An error occured while trying to open {inputfile}. The error: {str(error)}")

        with mp.Pool(processes=args.n) as pool:
            # only take the fourth line of read
            results = pool.map(calculate_phred, lines[3::4])

        average = np.mean(results, axis=0)

        if args.csvfile is not None:
            csvfile = f"{str(file.name)}.{str(csvfile.name)}" if len(args.fastq_files)>1 else csvfile.name
            write_csv(csvfile, average)
        else:
            print(f"Results for {file.name}")
            csv_writer = csv.writer(sys.stdout)
            for index, row in enumerate(average, start=1):
                csv_writer.writerow([index, row])

    return 0

if __name__ == "__main__":
    sys.exit(main())
