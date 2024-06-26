#!/usr/bin/env python3

"""
Big Data Computing (BDC) assignment 5.

By Dennis Scheper (373689)

Usage:
    python3 assignment5.py
"""

import argparse as ap
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,

schema = StructType([
    StructField("FEATURES", StringType(), True),
    #StructField("LOCUS", StringType(), True)
])

def parse_arguments():
    parser = ap.ArgumentParser(description="Big data computing assignment 3 by Dennis Scheper")
    parser.add_argument("gbff_file", nargs='*', help="At least one Illumina Fastq Format file to process")
    #parser.add_argument("-o", dest="csvfile", required=False, help="CSV file to store the output. Default is output to terminal STDOUT")
    return parser.parse_args()


def create_session():
    """Creates a Spark session"""
    spark = SparkSession.builder.master("local[16]").appName('djscheper_ass5').getOrCreate()
    return spark


def main():
    """Main function"""
    args = parse_arguments()
    spark = create_session()

    df = spark.read.csv(args.gbff_file[0], schema=schema)
    df.show()

if __name__ == "__main__":
    main()