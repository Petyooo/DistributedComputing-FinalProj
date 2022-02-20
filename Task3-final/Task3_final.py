'''
3/11/20

Task3_final.py

Authors:
Wael Fato
Petio Ivanov Todorov

Job Execution:
To run this file you need to run the following commands in the Terminal:
# for the retail year 2010-2011
$ python Task3_final.py --runner=local --no-bootstrap-mrjob retail1011.csv > Task3-results1011.txt
# for the retail year 2009-2010
$ python Task3_final.py --runner=local --no-bootstrap-mrjob retail0910.csv > Task3-results0910.txt
'''

# Import required libraries
from mrjob.job import MRJob     # To create the job
from mrjob.job import MRStep    # To define the steps of the job

# Define the indices of the "interesting" fields within the given data records
customer_ID_index = 6   # Index of the field "Customer ID" within the given records "map inputs"
price_index = 5         # Index of the field "Price" within the given records "map inputs"
quantity_index = 3      # Index of the field "Quantity" within the given records "map inputs"


# Define helper function to check against errors in the input data
def exception_handler(row):
    '''
    It is a helper function that we used to check against errors since the data has errors such as:
    - empty strings instead of customer IDs.
    - omitted fields ("cells") that causes incorrect indexing which causes errors
      while calculating revenues "price*quantity".
    It tries to:
    - convert customer_id into integer type. If it was not able to (ex: in case customer_id=' '),
       then an error will be raised.
    - convert price and quantity into float. If it was not able to (ex: in case incorrect indexing
        due to omitted values which causes price and quantity to have unconvertible values),
        then an error will be raised.

    In case an error or errors have been raised due to what is mentioned above,
     the function will return a string says "THIS ROW HAS AN ERROR"
    Otherwise "if everything goes well in try block", it returns the pair (customer_ID, (price , quantity))
    @:param "row": a record of our data (as a list of its fields)
    @:returns: for each record a tuple: (customer_ID, price * quantity),
               or "THIS ROW HAS AN ERROR", in case of an error has been raised
    '''

    try:
        customer_id = int(row[customer_ID_index])
        price = float(row[price_index])
        quantity = float(row[quantity_index])

    except ValueError:
        # Handle the error by returning a message
        return "THIS ROW HAS AN ERROR"
    else:
        # If everything went well
        return [customer_id, price, quantity]


# Create our job class
class MRTop10Buyers(MRJob):

    # Define a map function for our job
    def mapper_get_customer_revenue (self, _, line):
        '''
        mapper_get_customer_revenue:
        It reads the data line by line and for each one it will:
        1- splits the line based on "," since the line information is separated that way.
        2- checks against errors in the values of the given line, using the function "exception_handler".
            - In case of an error in any field, do nothing "the line will be ignored".
            - Otherwise, it yields for each line an intermediate (key, value) pair:
               key: customerID corresponding to that line.
               value: the revenue (price* quantity) for the given line.

        :param self: a reference to the current instance of the class.
        :param _: None "the key is ignored here, since we are reading chunks of data that belong to the same
        file which is stored in DFS".
        :param line: one line from the input file.
        :return: an intermediate key value pair (customerID, revenue="price * quantity").
        '''

        record_fields = line.split(',')
        # Notice: The records which have errors in its values will be ignored in this implementation
        checked_line = exception_handler(record_fields)
        if checked_line != "THIS ROW HAS AN ERROR" :
            yield (checked_line[0], checked_line[1] * checked_line[2])


    # Define a combiner function for our MRJob
    def combiner_sum_customer_revenues(self, customer_id, revenues):
        '''
        combiner_sum_customer_revenues:
        This combiner, for each customer_id observed by the attached mapper,
        - It combines (sums) all the values "revenues" that have been seen so far "by the attached mapper".
        - Then, it yields them with the corresponding customer_id to different reducers based on the customer_id.
        Combiner process is a kind of optimization process- "less data to be sent between processes".

        :param customer_id: an observed customerID.
        :param revenues: a revenue for a given customerID.
        :return: (customer_id, sum(revenues))
                 sum(revenues): summation over subset of revenues that are observed by the attached mapper
                 for a given customerID.

        Note: Multiple revenues with the same key ("customer ID”) will be sent to ONE reducer for that key
        from different combiners.
        '''

        yield customer_id, sum(revenues)


    # Define the first reducer function for our MRJob
    def reducer_total_customer_revenues(self, customer_id, total_revenues):
        '''
        reducer_total_customer_revenues:
        This reducer, for each customer "identified by the given Key":
        - It receives all revenues that are sent by multiple combiners.
        - Then, it yields customer_id and the sum of all of its revenues (total).

        :param customer_id: a customerID.
        :param total_revenues: revenues that are sent by multiple combiners.
        :return: (None ,(sum(total_revenues), customer_id))

        Note: since we want to find the top10 buyers in the next step, then all the reducers at this step
        should send all the pairs (sum(total_revenues),customer_id) to the same reducer in the next step.
        That is done by specifying "key = None" for outputs of this step

        '''

        yield None ,(sum(total_revenues), customer_id)


    # Define the second reducer function for our MRJob
    def reducer_get_top10_buyers(self, _, totalRevenues_cusID_pairs):
        '''
        reducer_get_top10_buyers:
        this final reducer
        - It gets all customerIDs, each with the corresponding total revenues
        - Sorts them based on the total revenues in a descending manner
        - Then yields the Top 10 CustomerID with the highest total revenues

        :param _: discard the key; "since all the output tuples of the previous step should be sent to this reducer"
        :param totalRevenues_cusID_pairs: a list of tuples where each tuple contains info (ID, total revenues)
        about one customer.
        :return: Top 10 CustomerID with the highest total revenue and their total revenue

        '''

        top10_buyers = sorted(totalRevenues_cusID_pairs, key=lambda pair: pair[0], reverse=True)
        list_length = len(top10_buyers) if len(top10_buyers) <= 10 else 10
        for i in range(list_length):
            yield "Customer_ID: " + str(top10_buyers[i][1]) , "With Total Revenues: "+str(top10_buyers[i][0])

    # Define the steps of our MRJob
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_customer_revenue,
                    combiner=self.combiner_sum_customer_revenues,
                    reducer=self.reducer_total_customer_revenues),
            MRStep(reducer=self.reducer_get_top10_buyers)
        ]

if __name__ == "__main__":
    MRTop10Buyers.run()