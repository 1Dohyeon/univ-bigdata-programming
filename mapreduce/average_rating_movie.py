from mrjob.job import MRJob
from mrjob.step import MRStep

class Average_rating_movie(MRJob):
	def steps(self):
		return [
				MRStep(mapper=self.map_rating, reducer=self.reduce_average_rating)
				]
					    
	def map_rating(self, _, line):
		userid, movieid, rating, timestamp = line.split(',')
		if userid != 'userId':
			yield movieid, float(rating)
																							    
	def reduce_average_rating(self, movie_id, ratings):
		ratings_list  = list(ratings)
		average_rating = sum(ratings_list) / len(ratings_list) # 평균 구함
		yield movie_id, round(average_rating, 2)  # 소수점 두 자리만

if __name__ == "__main__":
	Average_rating_movie.run()

