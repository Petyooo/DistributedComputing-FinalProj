'''
Wael Fato
Petio Todorov
1/1/21

gen_A_B_tuples.py

Converts matrices stored in text files into text files that have tuples with the following format:
<M,i,j,m_ij> Where- M: matrix name, i: row index, j: column index, m_ij : item,

To RUN:
run it like a regular .py file in Pycharm.

Output-
1. Text files with the correct format for the matrix. A_tuples.txt, B_tuples.txt.
Used as input to Task6.py
'''

import numpy as np
from pathlib import Path

# These matrices were created and then used for testing purposes only.
M = np.array([[1, 2, 3,4], [4, 5, 6,7],[3,3,3,3]])
N= np.array([[10,10],[20,20], [30,30],[40,40]])
np.savetxt('M.txt', M, fmt='%s')
np.savetxt('N.txt', N, fmt='%s')


def generate_tuples(file_path):
    matrix_name = Path(file_path).stem # cuts the stem, only the name of the file remains e.g. C.txt -- C
    matrix = np.loadtxt(file_path, delimiter=" ") # load the file contents into an np array

    matrix_tuples = []
    for i in range(len(matrix)):
        for j in range(len(matrix[0])):
            matrix_tuples.append((matrix_name, i, j, matrix[i][j]))
    np.savetxt(fr"{matrix_name}_tuples.txt", matrix_tuples, fmt='%s') # save the tuples to a text file


# Generate the new files for the input matrices A and B
generate_tuples("A.txt")
generate_tuples("B.txt")

# these two files of tuples were used for testing purposes only
generate_tuples("M.txt")
generate_tuples("N.txt")



