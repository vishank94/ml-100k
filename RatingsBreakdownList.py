from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        #(userID, movieID, rating, timestamp) = line.split('\t')
        while True:
            alist = line.split('\t')
            userID = alist[0]
            movieID = alist[1]
            rating = alist[2]
            timestamp = alist[3]
            yield rating, 1

    def reducer_count_ratings(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
    RatingsBreakdown.run()
