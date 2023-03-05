from mrjob.job import MRJob
import re
import os

WORDS_REGREX =  re.compile(r"\b\w+\b")

class Punto4(MRJob):

    def mapper(self, _, line):
        # Get the author name by searching 'Author:' in the input line and yielding the remaining content
        if re.search('Author:', line):
            author= line[8:]
            yield(author,1)
        # Get the first author of the list of authors
        elif re.search('Authors:', line):
            author = line[9:]
            yield (author,1)

    # It is not necesary the use of a combiner function for solving this problem. Although, it can be implemented in order to reduce the computer complexity and it would be the same function as the reducer.           

    def reducer(self, author, values):
        # Yield the author and the sum of their books
        yield (author, sum(values))

if __name__ == '__main__':
    Punto4.run()

# python3 4_autorcount.py input/* > 5_out.txt