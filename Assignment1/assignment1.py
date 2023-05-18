#!/usr/bin/env python

"""
Calculates the average PHRED score per base position in a given FASTQ file.
Final results may be written to a csv file, if desired by the user.

Usage:
    python3 assignment1.py -n <n_cpus> [-o <csv file>] fastq_files [FASTQ_file1, FASTQ_fileN]
"""

import argparse as ap
import numpy as np
import multiprocessing as mp
import csv
import sys

__author__ = "Dennis Scheper (373689)"
__date__ = "17/05/2023"
__version__ = "v1.0"
__contact__ = "d.j.scheper@st.hanze.nl"

def define_arguments() -> None:
    """
    Defines arguments for command-line usage.
    """
    argparser = ap.ArgumentParser(
        description="Script voor Opdracht 1 van Big Data Computing. Delivered by Dennis Scheper (373689).")
    argparser.add_argument("-n", action="store",
                        dest="n", required=True, type=int,
                        help="Aantal cores om te gebruiken.")
    argparser.add_argument("-o", action="store", dest="csvfile", type=ap.FileType('w', encoding='UTF-8'),
                        required=False, help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
    argparser.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='+', 
                           help="Minstens 1 Illumina Fastq Format file om te verwerken")
    
    return argparser.parse_args()


def calculate_phred(fastqfile: str) -> list:
    """
    Calculates the PHRED score for all characters in a read.
    """
    return [ord(q) - 33 for q in fastqfile]


def write_csv(csvfile, results: list, name) -> None:
    """
    Writes results to a csv file per given FASTQ file.
    Name of resulting csv file will be formatted as:
        `[name of inputfile].[name of outputfile].csv`
    """
    header = ['base_nr', 'average_phred_score']
    outputfile = f"{str(name.name)}.{str(csvfile.name)}"
    with open(outputfile, 'w') as csv_output:
        csvwriter = csv.writer(csv_output)
        csvwriter.writerow(i for i in header)

        for base_nmr, p in enumerate(results, start=1):
            csvwriter.writerow([base_nmr, p])


def main():
    """
    Main function in which command-line arguments are parsed and functions executed.
    """
    args = define_arguments()
    
    for file in args.fastq_files:
        try:
            with open(file.name, 'r') as inputfile:
                lines = inputfile.read().splitlines() # remove \n
        except FileNotFoundError:
            print(f"Couldn't find {inputfile}")
        except IOError as error:
            print(f"An error occured while trying to open {inputfile}. The error: {str(error)}")

        with mp.Pool(args.n) as pool:
            results = pool.map(calculate_phred, lines[3::4]) # only take the fourth line of read

        average = np.mean(results, axis=0)
        
        if args.csvfile is not None:
            write_csv(args.csvfile, average, file)
        else:
            print(f"Results for {file.name}")
            csv_writer = csv.writer(sys.stdout)
            for index, row in enumerate(average, start=1):
                csv_writer.writerow([index, row])

if __name__ == "__main__":
    main()