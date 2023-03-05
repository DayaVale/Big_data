import os
from mrjob.job import MRJob
from mrjob.step import MRStep
import re
from datetime import datetime

WORD_REGEX = re.compile(r"\b\w+\b")

class Punto8a(MRJob):
    ## Step 1
    def mapper(self, _,line):
        # Ceparate the content of the database row
        fields = line.split(';')
        # Define variables of the useful data
        time_epoch = int(fields[0])/1000 
        hour = datetime.utcfromtimestamp(time_epoch).strftime('%H')
        yield (hour,1)

    def combiner(self, hour, values):
        # Count the values of each hour
        count = sum(values)
        yield hour, count

    def reducer(self, hour, values):
        # Count the total of tweets at each hour
        count = sum(values)
        # Yield the touple as the value in order to get the max in the next step
        yield None, (count, hour)

    ## Step 2
    def reducer_max(self, _, hours):
        # Get the max value and yield it
        count, hour = max(hours)
        yield hour, count

    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   combiner=self.combiner,
                   reducer=self.reducer),
            MRStep(reducer=self.reducer_max)
        ]

if __name__ == "__main__":
    Punto8a.run()

# > python3 8a_tweetcount.py tweets2016_olympic_rio.test > 7a_out.txt