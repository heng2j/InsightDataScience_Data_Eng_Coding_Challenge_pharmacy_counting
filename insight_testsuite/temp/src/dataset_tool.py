#!/usr/bin/env python3
# dataset_tool.py
# ---------------
# Author: Zhongheng Li

"""
dataset_tool.py is hosting functions to modify data sets

    Run the script as following:

    time python3 ./src/dataset_tool.py -n 500 -i ./input/de_cc_data.txt -o ./input/de_cc_data_head500.txt

"""

from argparse import ArgumentParser


# create sample input file with limited line of instances for testing purposes
def create_sample_inputfile(filename, nlines):
    from itertools import islice
    with open(filename) as infile:
        with open(outputFile, 'w') as output_file:
            output_file.write(infile.readline())
        for line in islice(infile, nlines):
            with open(outputFile, 'a') as output_file:
                output_file.write(line)


if __name__ == '__main__':
    # Set up argument parser
    parser = ArgumentParser()
    parser.add_argument("-i", "--inputfile", help="Input file path", required=True)
    parser.add_argument("-o", "--outputfile", help="Output file path", required=True)
    parser.add_argument("-n", "--nlines", metavar='N', type=int, nargs='+', help="Number of line for output file",
                        required=True)

    args = parser.parse_args()

    # Assign input, output files and number of lines variables from command line arguments
    inputFile = args.inputfile
    outputFile = args.outputfile
    nlines = args.nlines[0]

    print("The input file is: ", inputFile)
    print("The output file is: ", outputFile)
    print("The number of lines are: ", nlines)

    create_sample_inputfile(inputFile, nlines)
