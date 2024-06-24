#!/usr/bin/env python3

"""
Big Data Computing (BDC) assignment 3. 

By Dennis Scheper (373689)

FIRST:
    chmod +x assignment3.sh

Usage:
    ./assignment3.sh [inputfiles]
"""

from collections import defaultdict
import argparse as ap
import csv
import ast
import sys
import numpy as np

__author__ = "Dennis Scheper (373689)"
__status__ = "Work in progress..."
__date__ = "23/06/2024"
__contact__ = "d.j.scheper@st.hanze.nl"

def parse_arguments():
    """
    Parses all arguments.
    """
    parser = ap.ArgumentParser(description="Big data computing assignment 3 by Dennis Scheper")
    parser.add_argument("--calculate", action="store_true", required=False,
                        help="Generate chunks.")
    parser.add_argument("--merge", action="store_true", required=False, help="Calculation option.")
    parser.add_argument("fastq_files", action="store", nargs='*', help="Minstens 1 Illumina Fastq Format file om te verwerken")
    parser.add_argument("-o", action="store", dest="csvfile", required=False, help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
    parser.add_argument("-n", type=int, required=False)
    return parser.parse_args()


def process_qline():
    """
    Processes the quality line of a given fastq file. Data comes in from a bash file and
    here we calculate the quality per base position. Thereafter, we 'dump' the data via
    json back to the bash script for further handling.
    
    Arguments: 
      X
      
    Returns:
      Prints the quality score per base position in json format
    """
    data = sys.stdin.buffer
    phred_scores = defaultdict(list)
    line_num = 0

    with data as input_data:
        for line in input_data:
            line_num += 1
            if line_num % 4 == 0:
                qual = line.strip()
                for pos, quality in enumerate(qual):
                    phred_scores[pos].append(quality - 33)
    # calculate the average per base position in place (for chunk)
    print({key: np.mean(values) for key, values in phred_scores.items()})


def calculate_average(filename, *, outputfile="output.csv", multiple=False):
    """
    Calculates the average quality score per base position. Handles the results
    by writing it to a CSV or prints it back to the command line.
    
    Arguments:
      - filename: name of the fastq file
      - outputfile: name output file (CSV)
      - multiple: boolean whether determine if multiple fastq files need to be handled
      
    Returns:
      X
    """
    data = sys.stdin.read().strip().split("\n")
    #put back to a dict
    result = [ast.literal_eval(res) for res in data]
    combined_values = defaultdict(list)

    # combine into one defaultdict
    for dictonary in result:
      for key, value in dictonary.items():
          combined_values[key].append(value)
    
    # calculate the averages for the whole file
    averages = {key: np.mean(values) for key, values in combined_values.items()}

    name_output = outputfile if not multiple else f"{filename}.{outputfile}"
    if filename is not None:
        with open(name_output, 'w', encoding='UTF-8') as csv_file:
            writer = csv.writer(csv_file)
            for key, value in averages.items():
                writer.writerow([key, value])
    else: 
        for key, value in averages.items():
            print(f"{key}, {value}")

def main():
    """
    The main function; directs all functionality.
    """
    args = parse_arguments()
    if args.calculate:
        process_qline()
    if args.merge:
        filename = args.csvfile if args.csvfile else None
        multiple = args.n > 1
        calculate_average(filename=filename, multiple=multiple)

if __name__ == "__main__":
    main()
