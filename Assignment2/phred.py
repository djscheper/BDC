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
        self.fastq = fastq.name
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


    def process_file(self, chunck):
        """
        Processes the FastQ file, calculates the Phred score, and adds it to a
        defaultdict. Defaultdict allows us to add more base positions in a
        straightforward manner.

        args:
        - chunck: chunck to be processed

        returns:
        - phred_scores: defaultdict with Phred score per base position for said chunck
        """
        # unpack the chunck's start and end positions
        start, end = chunck
        # defaultdict makes adding more base positions more flexible
        phred_scores = defaultdict(list)

        with open(self.fastq, 'rb') as inputfile:
            inputfile.seek(start)
            while inputfile.tell() < end:
                line = inputfile.readline()
                if line.startswith(b"@"):
                    inputfile.readline()
                    inputfile.readline()
                    qual = inputfile.readline().strip()
                    for pos, quality in enumerate(qual):
                        phred_scores[pos].append(quality - 33)  # no need for ord() since we are already working in bytes

        return phred_scores

    def calculate_average(self, phred_scores):
        """
        Calculates the average Phred score per base position by first merging
        every dict per chunck into one. Then simply calculate the average.

        args:
        - phred_scores: list of defaultdicts 

        returns:
        - a defaultdict with the average phred score per base position
        """
        combined = defaultdict(list)

        # combine every chunck into one dict
        for sub_dict in phred_scores:
            for key, value in sub_dict.items():
                combined[key].extend(value)
        
        # then simply return the mean per base position
        return {key: np.mean(values) for key, values in combined.items()}

    
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
        with open(name_output.name, 'w', newline='', encoding='UTF-8') as csv_file:
            writer = csv.writer(csv_file)
            for key, value in phred_scores.items():
                writer.writerow([key, value])