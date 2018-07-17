#!/bin/bash
#
# Use this shell script to compile (if necessary) your code and then execute it. Below is an example of what might be found in this file if your program was written in Python
#
# the python script were developed with Python 3



# Run the following code to generate a sample de_cc_data.txt with top 500 instances
# Used time command to measure the actual runtime on the current machine
# echo "Running dataset_tool.py to genearate a sample dataset..."
# python3 ./src/dataset_tool.py -n 500 -i ./input/de_cc_data.txt -o ./input/de_cc_data_head500.txt
# echo "Done"

# echo " "

# Run pharmacy_counting.py with the original sample dataset itcont.txt
# echo "Running with pharmacy_counting.py on sample dataset itcont.txt..."
# python3 ./src/pharmacy_counting.py -i ./input/itcont.txt -o ./output/top_cost_drug.txt
# echo "Done"

# echo " "

# Run pharmacy_counting.py with itcont_sameCost.txt a modified version of the original itcont.txt where AMBIEN costed the same as CHLORPROMAZINE in their first 2 prescriptions
#python3 ./src/pharmacy_counting.py -i ./input/itcont_sameCost.txt -o ./output/top_cost_drug.txt

# Run pharmacy_counting.py with first 500 lines of de_cc_data.txt
# echo "Running with pharmacy_counting.py on first 500 lines of de_cc_data.txt..."
# python3 ./src/pharmacy_counting.py -i ./input/de_cc_data_head500.txt -o ./output/top_cost_drug_head500.txt
# echo "Done"

# Run pharmacy_counting.py with the large dataset de_cc_data.txt
# echo "Running with pharmacy_counting.py on the large dataset de_cc_data.txt..."
# time python3 ./src/pharmacy_counting.py -i ./input/de_cc_data.txt -o ./output/top_cost_drug_large.txt
# echo "Done"

# echo " "

# Run with mapper and reducer with the original sample dataset itcont.txt
echo "Running with mapper and reducer on itcont.txt..."
time tail +2 ./input/itcont.txt | python3 ./src/mapper.py | sort | python3 ./src/reducer.py | sort -t $'\t' -k 3nr -k 1,1  | python3 -i ./src/saveOutput.py  -o./output/top_cost_drug.txt
echo "Done"

# Run with mapper and reducer with first 500 lines of de_cc_data.txt
echo "Running with mapper and reducer on first 500 lines of de_cc_data.txt..."
time tail +2 ./input/de_cc_data_head500.txt | python3 ./src/mapper.py | sort | python3 ./src/reducer.py | sort -t $'\t' -k 3nr -k 1,1  | python3 -i ./src/saveOutput.py -o ./output/top_cost_drug_head500.txt
echo "Done"

# Run with mapper and reducer with the large dataset de_cc_data.txt
# echo "Running with mapper and reducer on the large dataset de_cc_data.txt..."
# time tail +2 ./input/de_cc_data.txt | python3 ./src/mapper.py | sort | python3 ./src/reducer.py | sort -t $'\t' -k 3nr -k 1,1  | python3 ./src/saveOutput.py -o ./output/top_cost_drug_large_mapreduce.txt
# echo "Done"

