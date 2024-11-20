from mrjob.job import MRJob
from mrjob.step import MRStep


class PopMovie(MRJob):
	def steps(self):
		return [
				MRStep(mapper=self.map_rating_count, 
					combiner=self.combine_rating_count, 
					reducer=self.reduce_rating_count),
				MRStep(reducer=self.reduce_sort)
				]
					    
	def map_rating_count(self, _, line):
		userid, movieid, rating, timestamp = line.split(',')
		if userid != 'userId':
			yield(movieid, 1)

	def combine_rating_count(self, movie_id, count):
		yield (movie_id, sum(count))

	def reduce_rating_count(self, movie_id, counts):
		yield (str(sum(counts)).zfill(6), movie_id)

	def reduce_sort(self, count, movie_ids):
		for movie in movie_ids:
			yield (movie, count)

if __name__=="__main__":
	PopMovie.run()

