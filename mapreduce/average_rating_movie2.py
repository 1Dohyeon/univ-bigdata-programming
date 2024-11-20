from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
import csv

class Average_rating_movie2(MRJob):
	def configure_args(self):
		super(Average_rating_movie2, self).configure_args()
		self.add_file_arg('--movies', default='hdfs:///user/maria_dev/ml-latest-small/movies.csv')

	def steps(self):
		return [
				MRStep(mapper_init=self.mapper_init,
					mapper=self.map_rating,
					reducer=self.reduce_average_rating),
				MRStep(reducer=self.reduce_sort)
				]

	def mapper_init(self):
		self.movie_titles = {}
		with open(self.options.movies, encoding='utf-8') as f:
			reader = csv.reader(f)
			next(reader)
			for row in reader:
				movie_id, title =row[0], row[1]
				self.movie_titles[movie_id] = title
			
	def map_rating(self, _, line):
		userid, movieid, rating, timestamp = line.split(',')
		if userid != 'userId':  
			year = datetime.datetime.fromtimestamp(int(timestamp)).year
			title = self.movie_titles[movieid]
			yield (title, year), float(rating)
			
	def reduce_average_rating(self, title_year, ratings):
		ratings_list = list(ratings)
		average_rating = sum(ratings_list) / len(ratings_list)
		yield None, (average_rating, title_year)


	def reduce_sort(self, _, avg_title_year):
		for average_rating, (title, year) in sorted(avg_title_year, reverse=True):
			yield title, (round(average_rating, 2), f"year: {year}")

if __name__ == "__main__":
	Average_rating_movie2.run()

