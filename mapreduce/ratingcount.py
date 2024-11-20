from mrjob.job import MRJob
from mrjob.step import MRStep


class RatingCount(MRJob):
	def steps(self):
		return [MRStep(mapper=self.map_rating_count, reducer=self.reduce_rating_count)]
	def map_rating_count(self, _, line):
		userid, movieid, rating, timestamp = line.split(',')
		if userid != 'userId':
			yield(rating, 1)
    def reduce_rating_count(self, key, values):
	    yield key, sum(values)

if __name__=='__main__':
	RatingCount.run()
