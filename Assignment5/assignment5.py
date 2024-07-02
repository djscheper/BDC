#!/usr/bin/env python3

"""
Big Data Computing (BDC) assignment 5.

By Dennis Scheper (373689)

Usage:
    python3 assignment5.py <GBFF> [-o [name of csv; default is 'output.csv']]
"""

import argparse as ap
import re
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import avg, min, max, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from Bio import SeqIO

# schema for spark dataframe
schema = StructType([
    StructField("organism", StringType(), True),
    StructField("type", StringType(), True),
    StructField("length", IntegerType(), True)
])

def parse_arguments():
    """Parses all the command-line arguments."""
    parser = ap.ArgumentParser(description="Big data computing assignment 3 by Dennis Scheper")
    parser.add_argument("gbff_file", nargs='*', help="At least one Illumina Fastq Format file to process")
    parser.add_argument("-o", dest="csvfile", nargs='?', const="output.csv", default=None, help="CSV of not")
    return parser.parse_args()


def create_session():
    """Creates a Spark session"""
    return SparkSession.builder.appName("Assignment5").master("local[16]").getOrCreate()


def extract_features(gbff_file):
    """
    Extracts the necessary features from a GenBank Format File (GBFF).

    Calculates the length of a location beforehand.

    Returns a list with all necessary features, which are all in rows in dict format.
    """
    records = []

    with open(gbff_file, 'r') as inputfile:
        for record in SeqIO.parse(inputfile, 'genbank'):
            organism = None
            for feature in record.features:
                if 'organism' in feature.qualifiers:
                    organism = feature.qualifiers['organism'][0]

                # propeptide can be mat_peptide, too? check to make sure!!
                if feature.type in ['CDS', 'gene', 'ncRNA', 'propeptide', 'rRNA', 'mat_peptide']:

                    # if location is ambigu, skip the feature
                    if re.search(r'[<>]', str(feature.location)):
                        continue

                    start = feature.location.start
                    end = feature.location.end

                    # only append the necessary information
                    records.append({
                        'organism': organism,
                        'type': str(feature.type),
                        'length': int(end - start)
                    })

    return records


def main():
    """Main function"""
    args = parse_arguments()
    spark = create_session()

    predefined_file = "/data/datasets/NCBI/refseq/ftp.ncbi.nlm.nih.gov/refseq/release/archaea/archaea.2.genomic.gbff"
    args.gbff_file = predefined_file if not hasattr(args, 'gbff_file') else args.gbff_file

    features = extract_features(predefined_file)
    df = spark.createDataFrame(features, schema=schema)
    coding_features = ["CDS", "propeptide", "mat_peptide"]

    # Question 1
    question_1 = df.groupBy('organism').count().agg(avg("count"))

    # Question 2
    coding = df.filter(col('type').isin(coding_features)).count()
    non_coding = df.filter(~col("type").isin(coding_features)).count() # inverse
    question_2 = coding /non_coding

    # Question 3
    proteins = df.filter(col('type').isin(coding_features)).groupBy('organism').count()
    min_prot = proteins.agg(min('count'))
    max_prot = proteins.agg(max('count'))

    # Question 4
    coding_only = df.filter(col("type").isin(coding_features))
    # overwrite if already exists; to spark format?!
    coding_only.write.mode('overwrite').save("coding_df")

    # Question 5
    question_5 = df.agg(avg('length'))

    print(f'question 1: {question_1.first()[0]}')
    print(f'question 2: {question_2}')
    print(f'question 3: min: {min_prot.first()[0]}; max: {max_prot.first()[0]}')
    print(f'question 5: {question_5.first()[0]}')

    # is this necessary?
    if args.csvfile:
        answers = {
            'question1': f'{question_1.first()[0]}',
            'question2': f'{question_2}',
            'question3a': f'{min_prot.first()[0]}',
            'question3b': f'{max_prot.first()[0]}',
            'question5': f'{question_5.first()[0]}'
        }
        answers_df = pd.DataFrame([answers])

        output_file = args.csvfile if args.csvfile != 'True' else 'output.csv'
        answers_df.to_csv(output_file, index=False)

if __name__ == "__main__":
    main()

