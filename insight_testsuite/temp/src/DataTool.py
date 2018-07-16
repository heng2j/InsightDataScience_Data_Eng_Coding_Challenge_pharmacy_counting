import sys
import os.path
import time
import math


## Run this file with
## python3 ./src/DataTool.py ./input/de_cc_data.txt ./input/itcont_head500.txt


start_time = time.time()


# Check if there is missing arguments for input or output files
if len(sys.argv)  <= 2:
    print ("Error: There are missing arguments.")
    print("Please rerun the python script with the following convention with file paths for both input and output files: python pharmacy_counting.py inputFile outputFile  ")
    sys.exit(1)


# Check if input file exist
if not (os.path.exists(sys.argv[1])):
    print("Error: This input file does not exist:", (sys.argv[1]))
    sys.exit(1)

# Accept input and output files from command line arguments
inputFile = sys.argv[1]
outputFile = sys.argv[2]


print ("The input file is: ", inputFile)
print ("The output file is: ", outputFile)


def get_feeds(stdin):
    for line in stdin:
        line = line.strip()
        drug_name, prescriber, drug_cost = line.split('\t')
        yield (drug_name, prescriber, math.ceil(float(drug_cost)))


def create_sample_inputfile(filename, nlines):
    from itertools import islice
    with open(filename) as infile:

        with open(outputFile, 'w') as output_file:
            output_file.write(infile.readline())

        for line in islice(infile, nlines):
            with open(outputFile, 'a') as output_file:
                output_file.write(line)


create_sample_inputfile(inputFile, 500)

# # #Use Generator
# # #Scan the file line by line
# with open(inputFile) as infile:
#     # use the function property of readline() to read and omit the header of the file
#     # infile.readline()
#
#     with open(outputFile, 'w') as output_file:
#         output_file.write(infile.readline())
#
#     for i in range(0, 100):
#
#         for line in infile:
#
#             with open(outputFile, 'a') as output_file:
#                 output_file.write(line)




print("--- Finished in ---")
print("--- %s seconds ---" % (time.time() - start_time))