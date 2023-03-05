import os
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from datetime import datetime

WORD_REGEX = re.compile(r"\b\w+\b")

class Punto8a(MRJob):

    def mapper(self, _,line):
        # Ceparate the content of the database row
        fields = line.split(';')
        # Define variables of the useful data
        tweet = fields[2]
        # Yield the tuple as the value in order to go thought the values list in the reducer function.
        yield _, (len(str(tweet)), 1)

    def reducer(self, _, values):
        # Get the average of the tweets length
        len_sum = 0
        total = 0
        for length, _ in values:
            len_sum += length
            total += 1
        # Yield an "User friendly" output
        yield 'Average:', int(len_sum/total)

if __name__ == "__main__":
    Punto8a.run()

# > python3 8b_tweet_average.py tweets2016_olympic_rio.test > 7b_out.txt
