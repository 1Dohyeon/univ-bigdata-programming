
from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
import csv
import os
import subprocess

class AverageRatingMovie(MRJob):
	def configure_args(self):
		super(AverageRatingMovie, self).configure_args()
					    self.add_file_arg('--movies', help="Path to the movies.csv file")

							    def steps(self):
									        return [
													            MRStep(mapper_init=self.mapper_init,
																	                   mapper=self.map_rating,
																					                      reducer=self.reduce_average_rating),
																            MRStep(reducer=self.reduce_sort)
																			        ]

											    def mapper_init(self):
													        # HDFS에서 파일을 로컬로 복사
															        if self.options.movies.startswith('hdfs://'):
																		            local_path = '/tmp/movies.csv'
																					            subprocess.run(['hdfs', 'dfs', '-copyToLocal', self.options.movies, local_path], check=True)
																								            movies_file = local_path
																											        else:
																														            movies_file = self.options.movies

																																	        # 영화 ID와 제목을 매핑
																																			        self.movie_titles = {}
																																					        with open(movies_file, encoding='utf-8') as f:
																																								            reader = csv.reader(f)
																																											            next(reader)  # 헤더 건너뛰기
																																														            for row in reader:
																																																		                movie_id, title = row[0], row[1]
																																																						                self.movie_titles[movie_id] = title

																																																										    def map_rating(self, _, line):
																																																												        userid, movieid, rating, timestamp = line.split(',')
																																																														        if userid != 'userId':  # 헤더 제외
																																																																	            year = datetime.datetime.fromtimestamp(int(timestamp)).year
																																																																				            title = self.movie_titles.get(movieid, "Unknown Title")
																																																																							            yield (title, year), float(rating)

																																																																										    def reduce_average_rating(self, movie_year, ratings):
																																																																												        ratings_list = list(ratings)
																																																																														        average_rating = sum(ratings_list) / len(ratings_list)
																																																																																        yield None, (average_rating, movie_year)  # (평균 평점, (영화 제목, 연도)) 반환

																																																																																		    def reduce_sort(self, _, avg_movie_year_pairs):
																																																																																				        for average_rating, (title, year) in sorted(avg_movie_year_pairs, reverse=True):
																																																																																							            yield f"{title} ({year})", round(average_rating, 2)  # 소수점 2자리 반올림

																																																																																										if __name__ == "__main__":
																																																																																											    AverageRatingMovie.run()

