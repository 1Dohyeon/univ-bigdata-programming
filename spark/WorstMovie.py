from pyspark import SparkConf, SparkContext
from itertools import islice
import csv
import re

def parse_rating(lines):
	column = lines.split(",")
	return (int(column[1]), (float(column[2]),1))

def parse_movie(lines):
	column = list(csv.reader([lines]))[0]
	return (int(column[0]), str(column[1].replace("\"","")))

if __name__ =="__main__":
	conf = SparkConf().setAppName("WorstMovie")
	sc = SparkContext(conf = conf)
	rating_lines = sc.textFile("hdfs:///user/maria_dev/ml-latest-small/ratings.csv")
	rating_lines = rating_lines.mapPartitionsWithIndex(
		lambda idx, it: islice(it, 1, None) if idx == 0 else it
		)
		 
	rating_sum_count_map = rating_lines.map(parse_rating)
		   
	rating_sum_count_reduce = rating_sum_count_map.reduceByKey(lambda x1, x2:(x1[0]+x2[0], x1[1]+x2[1]))

	rating_ave = rating_sum_count_reduce.map(lambda x: (x[0], round(x[1][0]/x[1][1],2)))
	
	rating_ave_sort = rating_ave.sortBy(lambda x: x[1])

	movie_lines = sc.textFile("hdfs:///user/maria_dev/ml-latest-small/movies.csv")
	movie_lines = movie_lines.mapPartitionsWithIndex(
		lambda idx, it: islice(it, 1, None) if idx == 0 else it
	)
	movie_map = movie_lines.map(parse_movie)
	
	worst_top10_title = rating_ave_sort.join(movie_map).sortBy(lambda x: x[1][0]).take(30)
	for e in worst_top10_title:
		print("%s %.2f"%(e[1][1],e[1][0]))

