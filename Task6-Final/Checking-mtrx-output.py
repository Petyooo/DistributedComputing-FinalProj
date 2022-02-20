'''
Petio Todorov
Wael Fato
1/1/21

Checking-mtrx-output.py

This program checks whether our resulting matrix, computed with mrjob,
matches the product of input matrices A and B when they are computed
in numpy. Note that the mrjob computation yields tuples in the format:
(index in final output matrix, element value in final output matrix).
There was a problem importing file C.txt (the shape was not correct)
so we directly computed C by multiplying A and B as matrices in numpy.

To RUN: run it like a regular .py file in Pycharm.
Make sure to load the correct files for A, B, and computed_values_mrjob.

Output-
1. Proper output- "Success, all values match!"
2. Improper output- -1 (indicates that some values between matrix C and the values computed
with mrjob do not match).
'''


import numpy as np
import math
import ast

# max error that we allow between the mrjob computed values and the numpy dot product values.
max_difference = .0000000001

computed_values_mrjob = "mtrx-output.txt"
A = np.loadtxt("A.txt", dtype='float')
B = np.loadtxt("B.txt", dtype='float')
original_C_matrix = A.dot(B)
#print(original_C_matrix.shape)

with open(computed_values_mrjob, 'r') as sparse_matrix:
    data = sparse_matrix.readlines()
    flag = False
    for line in data:
        # matrix output line format e.g. [0, 0]	10.673303052759014
        line = line.split("	")
        index = ast.literal_eval(line[0]) # creates a list of the index in the final output matrix, e.g. [i, k]
        value = float(line[1].strip()) # element value at index [i, k]

        if(math.fabs(value - original_C_matrix[index[0], index[1]]) > max_difference):
            flag = True
            break

    if flag == False:
        print("Success, all values match!")
    else:
        print(-1)
