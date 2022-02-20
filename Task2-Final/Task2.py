'''
Petio Todorov
Wael Fato
3/11/20

Task2.py

We are given the file title.basics.tsv, which contains "information on movies, tv-shows and shorts from
IMDB. Parameters include: [tconst titleType primaryTitle originalTitle isAdult startYear endYear runtimeMinutes genres]"

"For each possible genre of the type movie (this excludes tv-shows, etc...) find the top 15 most common keywords
within their primary titles using MapReduce. Try to avoid auxiliary verbs, prepositions, articles and conjunctions
as keywords. You can use libraries like NLTK to help you with the latter part."
(source- DCSA_project_outline2021.pdf)

To RUN:
1. $ python Task2.py --runner=local --no-bootstrap-mrjob title.basics.tsv > Task2-results.txt

Input-
1. title.basics.tsv

Output-
1. A text file with the top 15 keywords from each genre where the entity type is 'movie', Task2-results.txt
'''

from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from nltk.corpus import stopwords

# includes stopwords from all languages (english, german, spanish, french, italian, etc.)
stop_words = list(stopwords.words())

WORD_RE = re.compile(r"[\w']+") # match words- either alphanumeric or an apostrophe! basically no whitespace
# source- https://mrjob.readthedocs.io/en/latest/guides/writing-mrjobs.html

class MostCommonKeywordsPerGenre(MRJob):

    def mapper_get_words(self, _, line):
        '''
        For each keyword in the primary title of an entity of the type movie, we generate a tuple with the keyword
        for each of the genres of the entity.

        Each mapper yields a key, value pair where the key is the genre and a keyword and the value is the number of
        occurrences of the keyword = 1.

        :param _: None
        :param line: one line from the input file [tconst titleType primaryTitle originalTitle isAdult startYear
        endYear runtimeMinutes genres]
        :return: (key, value) where key=(genre, word.lower()) and value=1 (number of occurrences of the word)
        '''
        # sample line, delimited with tabs!
        # "tt0020350\tmovie\tThe Runaway Princess\tThe Runaway Princess\t0\t1929\t\\N\t\\N\tCrime,Drama"

        cur_line = line.split("\t")

        type = cur_line[1]
        primary_title = cur_line[2]
        genres = cur_line[8]

        if type == 'movie':
            for word in WORD_RE.findall(primary_title): # find all words in the title
                for genre in WORD_RE.findall(genres):
                    if word.lower() not in stop_words: # CAPS MATTERS A != a
                        yield ((genre, word.lower()), 1) # for each word, make a pair with every genre for this movie

    def combiner_count_words(self, key, counts):
        '''
        For each mapper, there is a combiner which sums the counts for all output lines that have the same key; this is
        done only for each separate mapper.

        :param key: key=(genre, word)
        :param counts: 1 (number of occurrences of the word)
        :return: (key, sum(counts))
        '''

        yield key, sum(counts)

    def reducer_count_words(self, key, counts):
        '''
        Each of the combiners sends its output lines to reducers based on their key. Each reducer then sums the word
        counts for each line that it receives (all of which have the same key).

        Each reducer then yields each word and its corresponding final count to another reducer based on its genre,
        which acts as the new key.

        :param key: key=(genre, word)
        :param counts: the number of occurrences of the word from the result of the combiners
        :return: genre, (sum(counts), word)
        '''

        genre, word = key
        yield genre, (sum(counts), word)

    def reducer_sort_counts(self, genre, word_count_pairs):
        '''
        Each reducer receives the words and their counts for one particular genre, which is the key. Each reducer
        sorts the words based on their counts from highest to lowest and then yields the 15 words with the highest
        counts.

        :param _: key: key= genre
        :param word_count_pairs: each item of word_count_pairs is (count, word)
        :return: (key, value) where key=(genre, word) and value= (number of occurrences of the word) but only the top
        15 highest values!
        '''

        sorted_by_count = sorted(word_count_pairs, key=lambda x: x[0], reverse=True)

        for i in range(15):
            yield (genre, sorted_by_count[i][1]), sorted_by_count[i][0]


    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_words,
                   combiner=self.combiner_count_words,
                   reducer=self.reducer_count_words),
            MRStep(reducer=self.reducer_sort_counts)
        ]


if __name__ == '__main__':
    MostCommonKeywordsPerGenre.run()   # where MostCommonKeywordsPerGenre is your job class


