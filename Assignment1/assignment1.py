#!/usr/bin/env python

import argparse as ap
import multiprocessing as mp

def parse_arguments():
    """
    """
    argparser = ap.ArgumentParser(description="Script voor Opdracht 1 van Big Data Computing")
    argparser.add_argument("-n", action="store",
                       dest="n", required=True, type=int,
                       help="Aantal cores om te gebruiken.")
    argparser.add_argument("-o", action="store", dest="csvfile", type=ap.FileType('w', encoding='UTF-8'),
                       required=False, help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")
    argparser.add_argument("fastq_files", action="store", type=ap.FileType('r'), nargs='+', help="Minstens 1 Illumina Fastq Format file om te verwerken")
    return argparser.parse_args()

def main():
    """
    """
    args = parse_arguments()

    with mp.Pool(args.n) as pool:
        result = pool.map()


if __name__ == "__main__":
    main()