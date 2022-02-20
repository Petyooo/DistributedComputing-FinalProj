'''
Petio Todorov
Wael Fato
25/11/20

Task5.py

Computes the cosine similarity score for a set of scientific papers from arxiv stored in the JSON lines file,
mod-arxiv.jl. The paper "Similarity Measures for Text Document Clustering" by Anna Huang was used as the main reference.
As described in the paper, "when documents are represented as term vectors, the similarity of two documents
corresponds to the correlation between the vectors. This is quantified as the cosine of the angle between vectors."
The cosine similarity is computed by taking the dot product of the two vectors and dividing the result by the
magnitude of each vector. "The cosine similarity is non-negative and bounded between [0, 1]."
Additionally, it "is independent of document length."

The documents are represented as vectors using the bag of words approach, "where words are assumed to appear
independently and the order is immaterial. Words are counted in the bag. Each word corresponds to a dimension
in the resulting data space and each document then becomes a vector consisting of non-negative values on each dimension.
Here we use the frequency of each term as its weight, which means terms that appear more frequently are more
important and descriptive for the document."


To RUN:
1. Before running Task5.py, we need to use the Create-JSON-lines-file.py program to convert the file arxivData.json
from JSON format into JSON line format. The output file is named mod-arxivData.jl.
2. $ python Task5.py --runner=local --no-bootstrap-mrjob  mod-arxivData.jl > Task5-output.txt

Input-
1. A json file with metadata (including summaries) of scientific papers from arxiv, mod-arxivData.jl
2. The variable "text_to_match" below contains the string for which we are trying to find similar paper summaries.

Output-
1. A text file with the id's of the top 10 papers with highest Cosine Similarity score, Task5-output.txt
'''

import mrjob.protocol
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
import numpy as np
import nltk
from nltk.corpus import stopwords
from sklearn import feature_extraction

stop_words = list(stopwords.words('english'))


def clean_input_text(text):
    '''
    -----NOTE-----: the following code is almost entirely based from the following source, with our modifications:
    https://towardsdatascience.com/text-classification-with-nlp-tf-idf-vs-word2vec-vs-bert-41ff868d1794

    Here we clean and prepare each input summary for vectorization by removing any punctuation (?, ,, ., :, ;, etc.),
    removing common stop words from the English language that do not hold significant meaning
    (e.g. the, and, i, it, etc), and suffix stripping (i.e. removing ion, ing, etc. from the end of each word).

    :param text: a string (most likely a paper summary) that we want to prepare for vectorization
    :return: cleaned_text: the cleaned version of the string
    '''

    # convert the input string to LOWERCASE and remove any white space from the beginning and end of the string
    # also replace any dashes with a space, for example consider the word "state-of-the-art" that might appear
    # in a paper summary; it becomes "state of the art"
    new_text = re.sub(r'-', ' ', str(text).lower().strip())

    # used directly from source above. we remove all punctuation and unnecessary characters from the input string.
    # the regular expression can be broken down as follows: ^: Match a single character not present in the list below:
    # \w matches any word character (equal to [a-zA-Z0-9_])
    # \s matches any whitespace character (equal to [\r\n\t\f\v ] (https://regex101.com/ was used to analyze the RE)
    new_text = re.sub(r'[^\w\s]', '', new_text)

    # here we turn the string into a list and remove any stop words. The stop words come only from the English language.
    filtered_words = [word for word in new_text.split() if word not in stop_words] # used directly from source above

    # suffix stripping- ion, ing, etc. are removed
    # again this code is taken directly from the source mentioned above!!!
    porter_stemmer = nltk.stem.porter.PorterStemmer()
    ported_words = [porter_stemmer.stem(word) for word in filtered_words]
    cleaned_text = " ".join(ported_words)
    return cleaned_text

def vectorize_text(text, text_to_match):
    '''
    ------NOTE-------: the following code is mainly coming from the following sources, with our modifications:
    https://towardsdatascience.com/text-classification-with-nlp-tf-idf-vs-word2vec-vs-bert-41ff868d1794
    https://www.mygreatlearning.com/blog/bag-of-words/

    Here we create a vector representation for each of the input texts; the combo of all input texts results
    in a matrix. This is done using a Bag of Words approach, where the features (columns) are unigrams and bigrams
    (i.e. single words or pairs of words) taken from the union of the input texts (text and text_to_match).
    There are two rows, one for each input text (text is the 0th row, text_to_match is the 1st row). The value at each
    index (row, col) of this matrix is the frequency (count) of the ngram (column/feature) in the current text (row).
    Essentially we are counting the number of times a term appears in each of the input texts.

    :param text: the current scientific paper summary
    :param text_to_match: a string which contains words/text. We'd like to find the scientific
    :return: vec: a vector of words
    '''

    input_data = [text_to_match, text]

    # CountVectorizer converts a collection of text documents to a matrix of token counts (i.e. frequency of each term)
    # source: (https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.text.CountVectorizer.
    # html#sklearn.feature_extraction.text.CountVectorizer)
    # ngram range (1, 2) indicates we take both unigrams and bigrams; this means that pairs of words can
    # be considered as one or two words- for example New York can be 1 word as well as 2 separate words
    # analyzer indicates that the features should be word ngrams rather than character ngrams
    vectorizer = feature_extraction.text.CountVectorizer(analyzer='word', ngram_range=(1, 2))

    # this is a matrix; the rows are the input_data texts and the columns are terms from the union of ngrams of
    # those texts. each entry is the frequency of each term (column) as found in the current text (row)
    vec = vectorizer.fit_transform(input_data)
    return vec

def cosine_similarity(vec1, vec2):
    '''
    Computes the cosine_similarity between two input vectors.

    :param vec1: 1st input vector
    :param vec2: 2nd input vector
    :return: the cosine similarity score (a scalar value)
    '''
    norm1 = np.linalg.norm(vec1) # magnitude of vector 1
    norm2 = np.linalg.norm(vec2) # magnitude of vector 2

    if (norm1 and norm2 > 0): # make sure you don't have a division by 0 error
        cos_sim = np.dot(vec1, vec2) / (norm1 * norm2)
    else:
        cos_sim = -1 # return -1 to indicate a division by 0 error

    return cos_sim


####################################INPUT QUERY###########################################################
# This is a RANDOM summary, or snippet of a summary. We want to find similar papers based on it.
##########################################################################################################
text_to_match = "features and enables the model to reason relations between several parts of the image and question. " \
                "Our single model outperforms # we will append this to summaries later on"
cleaned_search_text = clean_input_text(text_to_match)


class MRcosineSimilarity(MRJob):
    # we first need to indicate that each line of the input file is actually in JSON
    # source: https://mrjob.readthedocs.io/en/latest/job.html
    INPUT_PROTOCOL = mrjob.protocol.JSONValueProtocol

    def mapper_get_summaries(self, _, line):
        '''
        Each mapper calculates the cosine similarity score for each scientific paper, in a subset of the archive,
        relative to the "text_to_match." The "text_to_match" is a summary or snippet of a summary that we would
        like to find similar papers to. (It acts as the random summary.)

        :param _: None (no key for input file)
        :param line: one JSON line from the input file mod-arxivData.jl
        :return: key = none, value = tuple of form (cos. sim. score, scientific paper id, scientific paper summary)
        '''

        id_num = line['id'] # get the scientific paper ID number
        summary = line['summary'] # get the scientific paper summary

        # pre-process the summaries
        trans_words = clean_input_text(summary) # cleaned/transformed version of the input string
        # vectorize the texts and convert from sparse matrix to regular matrix
        vec = vectorize_text(trans_words, cleaned_search_text).toarray()
        cos_sim = cosine_similarity(vec[0], vec[1])

        yield None, (cos_sim, id_num, summary)

    def reduce_sort_cos_sim(self, _, index_cos_sim):
        '''
        A single reducer which sorts the input tuples from highest to lowest by their cosine similarity score.

        :param _: None (no key for each tuple)
        :param index_cos_sim: a tuple with format: (cos. sim. score, scientific paper id, scientific paper summary)
        :return: yields the 10 highest cosine similarity scores and the id of their corresponding scientific paper
        '''
        sorted_cos_sim = sorted(index_cos_sim, key=lambda x: x[0], reverse=True) # sort by cos. sim. score

        for i in range(10): # yield the 10 highest scores
            yield sorted_cos_sim[i][0], sorted_cos_sim[i][1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_summaries,
                   reducer=self.reduce_sort_cos_sim)
        ]

if __name__ == '__main__':
    MRcosineSimilarity.run()   # where MRcosineSimilarity is your job class


