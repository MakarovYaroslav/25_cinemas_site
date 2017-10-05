import requests
from bs4 import BeautifulSoup
from datetime import date
import re
from multiprocessing import Pool


def fetch_afisha_page():
    afisha_link = 'http://www.afisha.ru/msk/schedule_cinema/'
    afisha_page = requests.get(afisha_link)
    return afisha_page.content


def parse_afisha_list(raw_html):
    min_count_of_cinemas = 30
    movies_info = dict()
    soup = BeautifulSoup(raw_html, 'lxml')
    movies = soup.find_all('div', "object s-votes-hover-area collapsed")
    for movie in movies:
        table = movie.find('table')
        table_body = table.find('tbody')
        cinemas = table_body.find_all('tr')
        movie_name = movie.find('h3').text
        cinema_count = len(cinemas)
        if cinema_count >= min_count_of_cinemas:
            h3_tag = movie.find('h3')
            afisha_url = h3_tag.a['href']
            movies_info[movie_name] = {
                'cinema_count': cinema_count,
                'afisha_url': afisha_url
            }
    return movies_info


def get_movie_kinopoisk_id_with_rating(movie_title):
    current_year = date.today().year
    request_parameters = {'kp_query': movie_title.encode('cp1251'),
                          'm_act[from_year]': current_year - 1,
                          'm_act[to_year]': current_year + 1}
    returned_data = requests.get(
        'https://www.kinopoisk.ru/index.php',
        params=request_parameters,
        allow_redirects=False
    )
    soup = BeautifulSoup(returned_data.content, 'lxml')
    searched_film = soup.find('div', "element most_wanted")
    if searched_film is None:
        movie_url = returned_data.headers['location']
        kinopoisk_url = 'https://www.kinopoisk.ru%s' % movie_url
    else:
        movie_url = searched_film.find('p', 'pic').a['data-url']
        kinopoisk_url = 'https://www.kinopoisk.ru%s' % movie_url
    movie_id = re.search('[0-9]+/$', kinopoisk_url).group()[:-1]
    rating = requests.get(
        'http://www.kinopoisk.ru/rating/' + movie_id + '.xml')
    soup = BeautifulSoup(rating.content, 'lxml')
    try:
        movie_rating = soup.find('kp_rating').text
        if not float(movie_rating):
            movie_rating = soup.find('imdb_rating').text
    except AttributeError:
        movie_rating = '0'
    return {'kinopoisk_id': movie_id, 'rating': movie_rating}


def get_movie_info(movie_url):
    movie_page = requests.get(movie_url)
    soup = BeautifulSoup(movie_page.content, 'lxml')
    movie_info = dict()
    try:
        movie_info['genre'] = soup.find('div', 'b-tags').get_text()
        movie_info['description'] = soup.find(
            'p',
            id='ctl00_CenterPlaceHolder_ucMainPageContent_pEditorComments'
        ).get_text()
    except AttributeError:
        movie_info['genre'] = 'Жанр'
        movie_info['description'] = 'Описание'
    return movie_info


def get_movie_image_url(kinopoisk_id):
    movie_image_url = 'https://st.kp.yandex.net/images/film_iphone/iphone360_' + kinopoisk_id + '.jpg'
    return movie_image_url


def get_movies_data_for_template_engine(count_movies_to_output):
    movies_url_and_cinemas = parse_afisha_list(fetch_afisha_page())
    list_of_movies = list(movies_url_and_cinemas)
    count_of_processes = 10
    with Pool(count_of_processes) as pool:
        rating_and_kinopoisk_id = pool.map(
            get_movie_kinopoisk_id_with_rating, list_of_movies)
    for number_of_movie_in_list, movie in enumerate(list_of_movies):
        movies_url_and_cinemas[movie]['kinopoisk_id'] = \
            rating_and_kinopoisk_id[number_of_movie_in_list]['kinopoisk_id']
        movies_url_and_cinemas[movie]['rating'] = \
            rating_and_kinopoisk_id[number_of_movie_in_list]['rating']
    sorted_movies = sorted(
        movies_url_and_cinemas.items(),
        key=lambda x: x[1]['rating'],
        reverse=True)
    list_of_afisha_urls = [info['afisha_url'] for movie, info in sorted_movies]
    with Pool(count_of_processes) as pool:
        genre_and_description = pool.map(get_movie_info, list_of_afisha_urls)
    for number_of_movie_in_list, (movie, info) in enumerate(sorted_movies):
        info['genre'] = genre_and_description[number_of_movie_in_list]['genre']
        info['description'] = \
            genre_and_description[number_of_movie_in_list]['description']
        info['image'] = get_movie_image_url(info['kinopoisk_id'])
    movies_info = sorted_movies[:count_movies_to_output]
    return movies_info
