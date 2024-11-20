REATE TABLE temp_movies (col_value STRING) STORED AS TEXTFILE;
CREATE TABLE temp_ratings (col_value STRING) STORED AS TEXTFILE;

LOAD DATA INPATH '/user/maria_dev/ml-latest/movies.csv' OVERWRITE INTO TABLE temp_movies;
LOAD DATA INPATH '/user/maria_dev/ml-latest/ratings.csv' OVERWRITE INTO TABLE temp_ratings;

CREATE TABLE movies (
	    movieId INT,
		    title STRING,
			    genres STRING
			) STORED AS TEXTFILE;

			CREATE TABLE ratings (
				    userId INT,
					    movieId INT,
						    rating FLOAT,
							    event_timestamp STRING
							) STORED AS TEXTFILE;

							INSERT OVERWRITE TABLE movies
							SELECT
							    CAST(regexp_extract(col_value, '^(?:([^,]*),?){1}', 1) AS INT) AS movieId,
								    regexp_extract(col_value, '^(?:([^,]*),?){2}', 1) AS title,
									    regexp_extract(col_value, '^(?:([^,]*),?){3}', 1) AS genres
										FROM temp_movies;

										INSERT OVERWRITE TABLE ratings
										SELECT
										    CAST(regexp_extract(col_value, '^(?:([^,]*),?){1}', 1) AS INT) AS userId,
											    CAST(regexp_extract(col_value, '^(?:([^,]*),?){2}', 1) AS INT) AS movieId,
												    CAST(regexp_extract(col_value, '^(?:([^,]*),?){3}', 1) AS FLOAT) AS rating,
													    regexp_extract(col_value, '^(?:([^,]*),?){4}', 1) AS event_timestamp
														FROM temp_ratings;

														-- 각 영화별 평균 평점, 평점 수 구함
WITH ratingsSummary AS (
	    SELECT
		        movieId,
				        COUNT(*) AS ratings_count,
						        AVG(rating) AS avg_rating
								    FROM ratings
									    GROUP BY movieId
									),

									-- 영화 and 평점 데이터 조인
moviesAndRatings AS (
	    SELECT
		        m.movieId,
				        m.title,
						        m.genres,
								        rs.ratings_count,
										        rs.avg_rating
												    FROM movies m
													    INNER JOIN ratingsSummary rs
														    ON m.movieId = rs.movieId
														),

														-- 평점 수가 30 이상인 데이터만 가져옴
filteredMovies AS (
	    SELECT
		        movieId,
				        title,
						        genres,
								        ratings_count,
										        avg_rating
												    FROM moviesAndRatings
													    WHERE ratings_count >= 30
													),

													-- 장르별로 데이터를 분리
genreSplit AS (
	    SELECT
		        movieId,
				        title,
						        genres_split.value AS genre,
								        ratings_count,
										        avg_rating
												    FROM filteredMovies
													    LATERAL VIEW EXPLODE(SPLIT(genres, '\\|')) genres_split AS value
													),

													-- 장르별 그룹화 및 집계
genreSummary AS (
	    SELECT
		        genre,
				        SUM(ratings_count) AS total_count,
						        AVG(avg_rating) AS avg_rating
								    FROM genreSplit
									    GROUP BY genre
									)

									-- 내림차순으로 정렬하고 가져옴
SELECT
    genre,
	    total_count,
		    avg_rating
			FROM genreSummary
			ORDER BY total_count DESC;
