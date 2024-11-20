import pandas as pd
import numpy as np

if __name__ == "__main__":
    # 각 CSV 파일 불러오기
    movies = pd.read_csv('ml-latest/movies.csv')
    genome_scores = pd.read_csv('ml-latest/genome-scores.csv')
    genome_tags = pd.read_csv('ml-latest/genome-tags.csv')
    links = pd.read_csv('ml-latest/links.csv')
    ratings = pd.read_csv('ml-latest/ratings.csv')
    tags = pd.read_csv('ml-latest/tags.csv')

    # genres 컬럼을 리스트로 분리
    movies['genres'] = movies.genres.str.split('|')
    print(movies.head())

    # movieFeatures에 각 장르별 컬럼 추가
    movieFeatures = movies.copy()
    for index, row in movies.iterrows():
        for genre in row['genres']:
            movieFeatures.at[index, genre] = 1
    movieFeatures = movieFeatures.fillna(0)

    # 연도 추출 및 제목 정리
    movies['year'] = movies.title.str.extract('(\(\d\d\d\d\))', expand=False)
    movies['year'] = movies.year.str.extract('(\d\d\d\d)', expand=False)
    movies['title'] = movies.title.str.replace('(\(\d\d\d\d\))', '', regex=True)
    movies['title'] = movies['title'].apply(lambda x: x.strip())

    # 예시로 사용할 소규모 데이터 생성
    small_movie = movieFeatures.iloc[:5][["Adventure", "Animation", "Comedy", "Fantasy", "Romance"]]

    # 사용자 입력 데이터
    userInput = [
        {'title': 'Breakfast Club, The', 'rating': 5},
        {'title': 'Toy Story', 'rating': 3.5},
        {'title': 'Jumanji', 'rating': 2},
        {'title': "Pulp Fiction", 'rating': 5},
        {'title': 'Akira', 'rating': 4.5}
    ]
    inputMovies = pd.DataFrame(userInput)

    # 사용자가 평가한 영화의 movieId 가져오기
    inputId = movies[movies['title'].isin(inputMovies['title'].tolist())]
    inputMovies = pd.merge(inputId, inputMovies)
    inputMovies = inputMovies.drop('genres', axis=1)

    # 사용자가 평가한 영화들의 장르 매트릭스 가져오기
    userMovies = movieFeatures[movieFeatures['movieId'].isin(inputMovies['movieId'].tolist())]
    userMovies = userMovies.reset_index(drop=True)
    userMovies = userMovies.drop(['movieId', 'title', 'genres'], axis=1)

    # 사용자의 프로필 생성
    userProfile = userMovies.transpose().dot(inputMovies['rating'])

    # 영화 피처 매트릭스를 movieId로 설정하고 불필요한 컬럼 제거
    movieFeatures = movieFeatures.set_index('movieId')
    movieFeatures = movieFeatures.drop(['title', 'genres'], axis=1)

    # 추천 테이블 생성
    recommendationTable = ((movieFeatures * userProfile).sum(axis=1)) / (userProfile.sum())
    recommendationTable = recommendationTable.sort_values(ascending=False)

    # 추천 상위 20개의 영화 출력
    recommended_movies = movies.loc[movies['movieId'].isin(recommendationTable.head(20).keys())]
    print(recommended_movies)

