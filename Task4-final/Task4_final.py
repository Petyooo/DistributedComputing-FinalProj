'''
3/11/20

Task4_final.py

Authors:
Wael Fato
Petio Ivanov Todorov

"Using MapReduce find the best selling product, once in terms of total quantity,
and once in terms of total revenue for both retail years." (source: Project pdf)

Job Execution:
$ python Task4_final.py --runner=local --no-bootstrap-mrjob retail1011.csv retail0910.csv > Task4-both-results.txt

'''

# Import required libraries
from mrjob.job import MRJob     # To create the job
from mrjob.job import MRStep    # To define the steps of the job

# Define the indices of the "interesting" fields within the given data lines
stockCode_index = 1     # Index of the field "stock code" for a certain product within the given records
price_index = 5         # Index of the field "Price" within the given records
quantity_index = 3      # Index of the field "Quantity" within the given records


# Define helper function to check against errors in the input data
def exception_handler(row):
    '''
    It is a helper function that we used to check against errors since the data has errors such as:
    - empty strings instead of product_SC
    - omitted fields ("cells") that causes incorrect indexing which causes errors
      while calculating revenues "price*quantity".
    It tries to:
    - If product_SC is an empty string then an error will be raised.
    - convert price and quantity into float. If it was not able to (ex: in case incorrect indexing
        due to omitted values which causes price and quantity to have inconvertible values),
        then an error will be raised.

    In case an error or errors have been raised due to what is mentioned above,
     the function will return a string saying "THIS ROW HAS AN ERROR"
    Otherwise "if every thing goes well in try block", it returns the pair (customer_ID, (price , quantity))
    @:param "row": a record of our data (as list of its fields)
    @:returns: for each record a pair of (customer_ID, price * quantity),
               or "THIS ROW HAS AN ERROR", in case of an error has been raised
    '''

    try:
        # NOTE:
        # We did not convert product_SC to integer since some of them have numerical and
        # symbolic characters (Ex: 85123A, 84970S, 84029G, POST ,etc)
        product_SC = row[stockCode_index]
        # In case of empty string as an input value for a stock code of given product
        if product_SC == "" : raise ValueError
        price = float(row[price_index])
        quantity = float(row[quantity_index])

    except ValueError:
        # Handle the error by returning a message
        return "THIS ROW HAS AN ERROR"
    else:
        # If everything went well
        return [product_SC, quantity, price]


# Create our job class
class MRTheBestSellingProduct(MRJob):

    # Define a map function for our job
    def mapper_get_product_quantity_revenue (self, _, line):
        '''
        mapper_get_product_quantity_revenue:
        It reads the data line by line and for each one it will:
        1- split the line based on "," since the line information is separated that way.
        2- checks against errors in the values of the given line, using the function "exception_handler"
            - In case of an error in any field, do nothing "the line will be ignored".
            - Otherwise, it yields for each line an intermediate (key, value) pair:
               key: product_SC corresponds to an observed stock code of the given line.
               value: tuple of the quantity and the revenue (quantity ,revenue "price* quantity") for the given line.

        :param self: a reference to the current instance of the class.
        :param _: None "the key is ignored here, since we are reading chunks of data that belong to the
        same file which is stored in DFS".
        :param line: one line from the input file.
        :return: an intermediate key value pair (product_SC, (quantity, revenue "price* quantity")).

        '''

        record_fields = line.split(',')
        # Notice: The records which have errors in its values will be ignored in this implementation
        checked_line = exception_handler(record_fields)
        if checked_line != "THIS ROW HAS AN ERROR" :
            yield checked_line[0], (checked_line[1], checked_line[2] * checked_line[1])

    # Define a combiner function for our MRJob
    def combiner_sum_product_quantities_revenues(self,product_SC, quantities_revenues_pairs):
        '''
        combiner_sum_product_quantities_revenues:
        This combiner, for a given product_SC "stock code of a product"
        - It combines (sums) all the revenues and quantities we've seen so far "by the attached mapper".
        - Then, it yields them with the corresponding product_SC to different reducers
        based on the product_SC.
        Combiner process is a kind of optimization process "less data to be sent between processes".

        :param product_SC: an observed stock code of a product.
        :param quantities_revenues_pairs:  tuples (quantities, revenues) for a given product_SC.
        :return: (product_SC, (sum(quantities), sum(revenues))).
            key: product_SC
            value: tuple of
                - summation over subset of quantities that are observed by the attached mapper for a given product_SC.
                - summation over subset of revenues that are observed by the attached mapper for a given product_SC.

        '''

        # NOTE:
        # Trying to access generators by index will trigger a TypeError, and to do so we have converted it to a list
        list_quantities_revenues = list(quantities_revenues_pairs)
        # The length of the list
        list_length = len(list_quantities_revenues)

        # For each product we create 2 lists from the received list
        # a list of quantities
        quantities= []
        # a list of revenues
        revenues= []
        for i in range(list_length):
            # remember that a quantities_revenues_pair = (quantity, revenue)
            quantities.append(list_quantities_revenues[i][0])
            revenues.append(list_quantities_revenues[i][1])

        yield (product_SC, (sum(quantities), sum(revenues)))


    def reducer_total_quantities_revenues(self, product_SC, total_quantities_revenues_pairs):
        '''
        reducer_total_quantities_revenues:
        This reducer, for each product "identified by a given Key (product_SC)":
        - It receives all (quantities and revenues) that are sent by multiple combiners.
        - Then, it yields
                - product_SC
                - the sum of all of its quantities (total) and the sum of all of its revenues (total) "as a tuple".
        :param product_SC: a product_SC for a product.
        :param total_quantities_revenues_pairs: tuples (quantities, revenues) that are sent by multiple combiners.
        :return: ( None, ((sum(total_quantities), sum(total_revenues)), product_SC) )

        Note: since we want to find the the best products (in terms of quantities and revenues )in the next step,
        then all the reducers at this step should send all the pairs to the same reducer in the next step.
        That is done by specifying "key = None" for outputs of this step.

        '''

        # NOTE:
        # Trying to access generators by index will trigger a TypeError, and to do so we have converted it to a list
        list_total_quantities_revenues = list(total_quantities_revenues_pairs)
        # Length of the list
        list_length= len(list_total_quantities_revenues)

        # For each product we create 2 lists from the received list
        # a list of total quantities and
        total_quantities = []
        # a list of total revenues
        total_revenues = []
        for i in range(list_length):
            # remember that a total_quantities_revenues_pair = (quantities, revenues)
            total_quantities.append(list_total_quantities_revenues[i][0])
            total_revenues.append(list_total_quantities_revenues[i][1])

        yield None, ((sum(total_quantities), sum(total_revenues)), product_SC)



    def reducer_bestSelling_quantities_revenues(self, _, total_quantReven_prodSC_pairs):
        '''
        reducer_bestSelling_quantities_revenues:
        this final reducer
        - It gets all productSC, each with the corresponding total quantities and total revenues
        - First, to get the productSC of the product with the highest quantity, we sort the received tuples
            based on quantities in a descending manner, thus the desired item is the first one.
        - Then, to get the productSC of the product with the highest revenues, we resort the received tuples
            based on revenues in a descending manner, thus the desired item is the first one.
        - Then yield the stock code of the product with the highest total quantity and its total quantity.
            and yield the stock code of the product with the highest total revenue and its total revenue.

        :param _: discard the key; "since all the output tuples of the previous step should be sent to this reducer"
        :param total_quantReven_prodSC_pairs : each tuple contains info about one product
        ((total quantities, total revenues), product_SC).
        :return: The stock code of the product with the highest total quantities and
                 The stock code of the product with the highest total revenues

        '''

        # Sort the received pairs based on the quantity "product with highest total quantity first"
        products = sorted(total_quantReven_prodSC_pairs, key=lambda pair: pair[0][0], reverse=True)
        # Get the pair of the best seller product in terms of total quantity
        bestSelling_quantities= products[0]

        # Sort the products list again but based on the revenues "product with highest total revenues first"
        products.sort(key=lambda pair: pair[0][1], reverse=True)
        # Get the pair of the best seller product in terms of total revenues
        bestSelling_revenues = products[0]

        yield ('The Best Selling Product in Terms of (Quantity) has:',
               ('StockCode: '+ bestSelling_quantities[1],'Total Quantity: ' + str(bestSelling_quantities[0][0])))

        yield ('The Best Selling Product in Terms of (Revenues) has:',
               ('StockCode: '+ bestSelling_revenues[1],'Total Revenues: ' + str(bestSelling_revenues[0][1])))


    # Define the steps of our Job
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_product_quantity_revenue,
                    combiner=self.combiner_sum_product_quantities_revenues,
                    reducer=self.reducer_total_quantities_revenues),
            MRStep(reducer=self.reducer_bestSelling_quantities_revenues)
        ]


if __name__ == "__main__":
    MRTheBestSellingProduct.run()