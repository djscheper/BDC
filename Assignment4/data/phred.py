"""
Big Data Computing (BDC) assignment 1. 

By Dennis Scheper (373689)

Usage:
    python3 assignment1.py -n <aantal_cpus> [OPTIONEEL: -o <output csv file>] fastabestand1.fastq [fastabestand2.fastq ... fastabestandN.fastq]
"""

__author__ = "Dennis Scheper (373689)"
__status__ = "Work in progress..."
__date__ = "17/05/2024"
__contact__ = "d.j.scheper@st.hanze.nl"

import argparse as ap
import csv
import multiprocessing as mp
import os
from collections import defaultdict
import numpy as np


class PhredScoreCalculator:
    """
    Class used to handle the processing of a FastQ file to Phred scores.

    Functions:
    - make_chuncks: splits FastQ file into chuncks in bytes based on the amount of cores
    - get_chuncks: simply retrieve all chuncks
    - process_file: this function calculates the Phred score per base position per chunck
    - calculate_average: calculates the average Phred score per base position by concatenating
                         all chuncks into defaultdict
    - write_csv: used for writing the results to a CSV format
    """

    def __init__(self, fastq, n):
        """
        Initiator. 

        args:
        - fastq: FastQ file to be processed
        - n: amount of cores

        self.chuncks: holds the chuncks defined by the make_chuncks function
        """
        self.fastq = fastq
        self.n = n
        self.chuncks = []

    def make_chuncks(self):
        """
        Determines how large a chunck is by calculating its start and end positions in bytes.
        The amount of chuncks is equal to the number of cores given by the user.
        All start and end positions are appended to self.chuncks for easy access within the class.
        """
        try:
            file_size = os.stat(self.fastq).st_size
        except FileNotFoundError as err:
            print(f"{err}: File in question has not been found. Are you sure it exists?")

        chunck_size = file_size // self.n

        for i in range(self.n):
            start = i * chunck_size
            end = start + chunck_size if i < self.n - 1 else file_size
            self.chuncks.append((start, end))


    def get_chunks(self):
        """
        Returns all the chuncks.
        """
        return self.chuncks
    

    def process_chunck(self, start_end):
        """
        """
        start, end = start_end
        with open(self.fastq, 'rb') as inputfile:
            inputfile.seek(start)
            chunk = inputfile.read(end-start)
        
        lines = chunk.split(b"\n")
        line_num = 0
        num_dict = defaultdict(list)
        for line in lines:
            line_num += 1
            if line_num % 4 == 0:
                qual = [c - 33 for c in line.strip()]
                for pos, score in enumerate(qual):
                    num_dict[pos].append(score)
        
        return num_dict

    def calculate_average(self, phred_scores):
        """
        Calculates the average Phred score per base position by first merging
        every dict per chunck into one. Then simply calculate the average.

        args:
        - phred_scores: list of defaultdicts 

        returns:
        - a defaultdict with the average phred score per base position
        """
        result = defaultdict(list)

        # Assuming each d in all_processed_chunks is a dictionary of lists
        for d in phred_scores:
            for sub_dict in d:
                for key, value_dict in sub_dict.items():
                    result[key].extend(value_dict)
        
        # then simply return the mean per base position
        return {key: np.mean(values) for key, values in result.items()}

    
    def csv_writer(self, phred_scores, *, outputfile="output.csv", multiple=False):
        """
        Writes the result to a csv specified by the user.

        args:
        - phred_scores: defaultdict with average phred score per base position
        - outputfile: name and location of csv file
        - multiple: boolean to determine whether multiple files are being processed 
                    for file naming purposes

        output:
        - average phred score per base position in CSV format
        """
        filename = os.path.basename(self.fastq) # get name of fastq file
        name_output = outputfile if not multiple else f"{filename}.{outputfile}.csv"
        with open(name_output, 'w', newline='', encoding='UTF-8') as csv_file:
            writer = csv.writer(csv_file)
            for key, value in phred_scores.items():
                writer.writerow([key, value])
