#!/usr/bin/env python3

"""
Big Data Computing (BDC) assignment 4.

By Dennis Scheper (373689)

FIRST:
    chmod +x deliverable2.sh

Usage:
    ./deliverable2.sh [inputfiles]
"""

import argparse as ap
import time

import numpy as np
from mpi4py import MPI

from phred import PhredScoreCalculator

def parse_arguments():
    """
    A simple argument parser.
    """
    parser = ap.ArgumentParser(description="Big data computing assignment 3 by Dennis Scheper")
    parser.add_argument("fastq_files", nargs='*', help="At least one Illumina Fastq Format file to process")
    parser.add_argument("-o", dest="csvfile", required=False, help="CSV file to store the output. Default is output to terminal STDOUT")
    return parser.parse_args()


def main():
    """
    Main function - calls upon all other functions.
    """
    start = time.time()
    args = parse_arguments()

    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    amount_processes = comm.Get_size()

    calculator = PhredScoreCalculator(args.fastq_files[0], amount_processes)

    if rank == 0:
        calculator.make_chuncks()
        chunks = calculator.get_chunks()
        array_chunks = np.array_split(chunks, amount_processes)
    else:
        array_chunks = None

    scatter_chunks = comm.scatter(array_chunks, root=0)
    res = [calculator.process_chunck(chunk) for chunk in scatter_chunks]

    all_processed_chunks = comm.gather(res, root=0)

    if rank == 0:
        averages = calculator.calculate_average(all_processed_chunks)
        calculator.csv_writer(averages, outputfile=args.csvfile)
        end = time.time()
        run_time = end - start
        print(run_time)


if __name__ == "__main__":
    main()
