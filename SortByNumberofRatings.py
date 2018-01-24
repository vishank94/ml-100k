#movieID - Sorted by number of ratings for that movie

from mrjob.job import MRJob
from mrjob.step import MRStep

class RatingsBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ratings,
                   reducer=self.reducer_count_ratings), #note second MRStep is within the first one
            MRStep(reducer=self.reducer_sort_ratings)
        ]

    def mapper_get_ratings(self, _, line):
        #(userID, movieID, rating, timestamp) = line.split('\t')
        #i=0
        #while True:
        alist = line.split('\t')
        #print(alist)
        #i = i+1
        #int ratingCount = 0
        userID = alist[0]
        movieID = alist[1]
        rating = alist[2]
        timestamp = alist[3]
        #ratingCount = ratingCount + 1
        yield movieID, 1 #key-val pair

    def reducer_count_ratings(self, key, values):
        yield sum(values), key #returns generator iterator but all data computed in just one call of the function
        #yield key, sum(values)
    
    def reducer_sort_ratings(self, key, values): #should be yield output from previous reducer
        sorterDict = {key:values}
        keylist = sorterDict.keys()
        keylist.sort()
        for key in keylist:
            yield key, sorterDict[key] #returns generator iterator but all data computed in just one call of the function  

if __name__ == '__main__':
    RatingsBreakdown.run()
