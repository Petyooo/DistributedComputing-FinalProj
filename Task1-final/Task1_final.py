'''
3/11/20
Task1_final.py

Authors:
Wael Fato
Petio Ivanov Todorov

"For all entities for the type movie and short, find the top 50 most common
keywords used in the primary titles using MapReduce. Try to avoid auxiliary
verbs, prepositions, articles and conjunctions as keywords. You can use libraries
like NLTK to help you with the latter part." (project PDF)

Job Execution:
To run this file you need to run the following commands in the Terminal:
$ python Task1_final.py --runner=local --no-bootstrap-mrjob title.basics.tsv > Task1_results.txt

'''

# Import required libraries
from mrjob.job import MRJob     # To create the job
from mrjob.step import MRStep   # To define the steps of the job
import re                       # To create patterns for words matching
import nltk
from nltk.corpus import stopwords as sw  # To remove stop words


# Create a list of the stopwords from different languages
# This list will be used later to remove stop words form "primaryTitle"
# Method of Concatenation is "Concatenation with Asterisk Operator *"
stop_words = [*sw.words('english'),*sw.words('german'),*sw.words('spanish'),*sw.words('french'),*sw.words('italian')]

# Define a regular expression to clean the field primaryTitle in given data
# It matches words either alphanumeric or an apostrophe! # basically no whitespace
WORD_RE = re.compile(r"[\w']+")

# Define the indices of the "interesting" fields within the given data records
type_index = 1      # Index of the titleType within the given records "map input"
title_index = 2     # Index of the primaryTitle within the given records "map input"

# Create a sub_class of the class MRJob
class MostCommonKeywords(MRJob):

    def mapper_get_words(self, _, line):
        '''
        mapper_get_words:
        this mapper yields each word in the primaryTitle as the key along with a count of 1 as the value
        :param self: a reference to the current instance of the class
        :param _: None "the key is ignored here, since we are reading chunks of data that belong to the same file"
        :param line: one line from the input file
        :return: for each word within the given primaryTitle "excluding stopwords", returns tuples of (key,value)
                 where the key is the word itself and the value always will be 1.

        Notes:
        - We used split("\t"), since the line information are separated by tabs
        - We converted the words to lowercase then "e.g. HELLO and hello" are not treated as 2 different words.
        '''


        line = line.split("\t")
        if line[type_index] in ['movie', 'short']:
            for word in WORD_RE.findall(line[title_index]):
                if word.lower() not in stop_words:
                    yield (word.lower(), 1)


    def combiner_count_words(self, word, counts):
        '''
        combiner_count_words:
        this combiner combines all the values "counts" we've seen so far for each word ONLY FROM EACH MAPPER
        as a kind of optimization- "less data to be copied "sent" between processes"
        :param word: a word observed in the given file = "key"
        :param counts: 1 (number of occurrences of the word)
        :return: (word, sum (counts))
                 word : a key "a certain word has been observed by the attached mapper"
                 sum : summation of the number of times that the attached mapper has seen the key "word"
        '''
        yield (word, sum(counts))


    def reducer_count_words(self, word, counts):
        '''
        reducer_count_words:
        It will aggregate the frequencies (counts) that are associated with a given word, then sum them.
        Then for each word "key", it sends the pair (sum (counts), word) to the same final reducer.

        :param word: a word observed in the given file = "key"
        :param counts: number of occurrences of the word from the result of the combiners
        :return: (None, (sum(counts), word))
                 None : to send all the tuples (frequencies, word) to the same reducer. Since the last
                 step in our job is to find out the most 50 frequent words then the last reducer should have
                 access to all the tuples to decide.
        '''
        yield None, (sum(counts), word)


    def reducer_sort_counts(self, _, word_count_pairs):
        '''
        reducer_sort_counts:
        this final reducer gets the most commonly used word
        :param _: discard the key; it is just None "since all the output tuples would be sent to this reducer"
        :param word_count_pairs: each item of word_count_pairs is (count, word)
        :return: (key=counts, value=word)

        Notes:
        sorted method has been used to sort the (#occurrences, word) tuples based on
        the #occurrences in a descending manner
        '''

        top50_words = sorted(word_count_pairs, key= lambda x: x[0], reverse=True) [:50]

        for word in top50_words:
            yield word

    # Define the steps of our MRJob
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_sort_counts)
        ]

if __name__ == '__main__':
    MostCommonKeywords.run()   # where MostCommonKeywords is your job class "our module"
















