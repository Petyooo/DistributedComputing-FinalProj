'''
Petio Todorov
Wael Fato
1/1/21

Task6.py

Computes the product of two matrices in a distributed fashion using
mrjob. We assume that the input matrices are stored in the following format: tuples of
<M,i,j,m_ij> Where- M: matrix name, i: row index, j: column index, m_ij : item. In this way, we will
follow the distributed matrix multiplication algorithm as explained in the course theory.
Before running Task6.py, we need to use the gen_A_B_tuples.py program to convert the matrices
stored in text files A.txt and B.txt into the previously mentioned format.

To RUN:
python Task6.py --runner=local --no-bootstrap-mrjob  A_tuples.txt B_tuples.txt > mtrx-output.txt

Output-
1. A text file with the resulting matrix, mtrx-output.txt
The output matrix can be checked by running the program Checking-mtrx-output.py
'''


from mrjob.job import MRJob
from mrjob.step import MRStep
import mrjob.protocol
from ast import literal_eval  # https://stackoverflow.com/questions/8494514/converting-string-to-tuple

# Data for small sized matrices. These were used to test the program initially.
# mat1= "M" # Matrix Name
# mat2= "N" # Matrix Name
# M_r = 3 # number of rows in Matrix M
# N_c = 2 # number of columns of Matrix N


# Matrix data- for input matrices A and B
mat1 = "A" # Matrix Name
mat2 = "B" # Matrix Name
M_r = 1000  # number of rows in Matrix M
N_c = 2000  # number of columns of Matrix N


class MRMatrixDot(MRJob):

    def mapper_produce_pairs(self, _, line):
        '''
        We assume that the matrices are stored as tuples in the following format:
        <M,i,j,m_ij> Where- M: matrix name, i: row index, j: column index, m_ij : item
        <N,j,k,n_ij> Where- N: matrix name, j: row index, k: column index, n_ij : item

        Each mapper will read a subset of tuples and will then yield them in a modified format:
        (Matrix name, j, element value from input matrix)

        :param _: None
        :param line: one line (containing a tuple) from an input matrix file
        :return: tuples of the form (index in final output matrix, (Matrix name, j, element value from input matrix))
        '''
        items = line.split()
        itemTuple = tuple(items)

        # the output format depends on whether a tuple comes from matrix M or N
        if itemTuple[0] == mat1:
            name, i, j, Mij = itemTuple
            for k in range(N_c):  # M 0 0 1.0 is the tuple format
                yield (int(i), k), (name, int(j), float(Mij))  # i, j, Mij are strings that should be cast to int

        if itemTuple[0] == mat2:
            name, j, k, Njk = itemTuple
            for i in range(M_r): #N 0 0 10.0 is the tuple format
                yield (i, int(k)), (name, int(j), float(Njk))  # k, j, Njk are strings that should be cast to int

    def combiner_produce_partial_lists(self, key, value):
        '''
        Each combiner yields a LIST of tuples (values) from it's corresponding
        mapper where all of the tuples have a matching key (i,k).
        Each list contains tuples of the form (M, j, m_ij) and (N, j, n_ij).

        It is really important to understand that the input param "value" from the mapper comes
        in the form of a generator object. Because the internal protocol for transferring data
        between the mapper and combiner is JSON, you must cast the generator object into a
        JSON serializable object (e.g. list) before yielding it; otherwise an error occurs.

        source: https://mrjob.readthedocs.io/en/latest/job.html

        :param key: (i,k) index in the final output matrix, obtained as input from the mapper
        :param value: a generator object with tuples of the form (M, j, m_ij) or (N, j, n_ij). Each tuple
                      has key (i,k) in the final output matrix
        :return: tuples of the form (index in final output matrix, [tuple1, tuple2, ..., tupleZ])
        '''
        key = tuple(key)
        list_of_tuples = list(value)
        yield key, list_of_tuples

    def reducer_ik_items(self, key, value):
        '''
        Each reducer gets as input tuples that correspond to the same index (i,k) in the final output matrix.
        Essentially this means that all items of a row "i" in M and all items from a column "k" in N will be sent
        to the same reducer. The tuples corresponding to the same Matrix (either M or N) will placed in a corresponding
        list and then be sorted by index j. The two lists will then have corresponding elements (by index number
        in the list) multiplied and all of the products summed. The final result will be yielded along
        with the key (i,k).

        It is not necessary to cast the generator object "value" into a list because we are not transferring it
        to another step or within a step of an mrjob.

        :param key: (i,k) index in the final output matrix, obtained as input from the combiner
        :param value: a generator object with, likely, multiple lists of the form [tuple1, tuple2, ..., tupleZ].
                      The tuples inside each list have the same key (i,k) in the final output matrix
        :return: a tuple of the form: (index in final output matrix, element value in final output matrix)
        '''
        key = tuple(key)

        M_items = []  # items from Matrix M in one reducer
        N_items = []  # items from Matrix N in one reducer

        # distribute the tuples based on which matrix (M or N) they are coming from
        for item_list in value:
            for tup in item_list:
                if tup[0] == mat1:
                    M_items.append(tup)
                else:
                    N_items.append(tup)

        sorted_M_items = sorted(M_items, key=lambda x: x[1])  # sort the items by index j
        sorted_N_items = sorted(N_items, key=lambda x: x[1])  # sort the items by index j

        # essentially we are multiplying corresponding elements from one row in M with the corresponding
        # elements of a column in N and then summing the resulting products.
        # the final value is the element value at index (i,k) in the final output matrix
        final_value = sum(
            [sorted_M_items[i][2] * sorted_N_items[i][2] for i in range(len(sorted_M_items))])

        yield (key, final_value)

    def steps(self):
        return [
            MRStep(mapper=self.mapper_produce_pairs,
                   combiner=self.combiner_produce_partial_lists,
                   reducer=self.reducer_ik_items)
        ]


if __name__ == '__main__':
    MRMatrixDot.run()