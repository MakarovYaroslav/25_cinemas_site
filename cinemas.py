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


def fetch_kinopoisk_page(movie_title):
    current_year = date.today().year
    request_parameters = {'kp_query': movie_title.encode('cp1251'),
                          'm_act[from_year]': current_year - 1,
                          'm_act[to_year]': current_year + 1}
    kinopoisk_page = requests.get(
        'https://www.kinopoisk.ru/index.php',
        params=request_parameters,
        allow_redirects=False
    )
    return kinopoisk_page


def get_movie_kinopoisk_id(movie_kinopoisk_html):
    soup = BeautifulSoup(movie_kinopoisk_html.content, 'lxml')
    searched_film = soup.find('div', "element most_wanted")
    if searched_film is None:
        movie_url = movie_kinopoisk_html.headers['location']
        kinopoisk_url = 'https://www.kinopoisk.ru%s' % movie_url
    else:
        movie_url = searched_film.find('p', 'pic').a['data-url']
        kinopoisk_url = 'https://www.kinopoisk.ru%s' % movie_url
    movie_id = re.search('[0-9]+/$', kinopoisk_url).group()[:-1]
    return movie_id


def fetch_movie_rating_page(movie_id):
    rating_page = requests.get(
        'http://www.kinopoisk.ru/rating/%s.xml' % movie_id)
    return rating_page.content


def get_movie_rating(movie_rating_page):
    soup = BeautifulSoup(movie_rating_page, 'lxml')
    try:
        movie_rating = soup.find('kp_rating').text
        if not float(movie_rating):
            movie_rating = soup.find('imdb_rating').text
    except AttributeError:
        movie_rating = '0'
    return movie_rating


def get_movie_kinopoisk_id_with_rating(movie_title):
    movie_id = get_movie_kinopoisk_id(fetch_kinopoisk_page(movie_title))
    movie_rating = get_movie_rating(fetch_movie_rating_page(movie_id))
    return {'kinopoisk_id': movie_id, 'rating': movie_rating}


def fetch_movie_afisha_page(movie_url):
    movie_afisha_page = requests.get(movie_url)
    return movie_afisha_page.content


def get_movie_genre_and_description(movie_afisha_page):
    soup = BeautifulSoup(movie_afisha_page, 'lxml')
    genre_and_description = dict()
    try:
        genre_and_description['genre'] = soup.find('div', 'b-tags').get_text()
        genre_and_description['description'] = soup.find(
            'p',
            id='ctl00_CenterPlaceHolder_ucMainPageContent_pEditorComments'
        ).get_text()
    except AttributeError:
        genre_and_description['genre'] = None
        genre_and_description['description'] = None
    return genre_and_description


def get_movie_info_from_afisha(movie_url):
    movie_afisha_page = fetch_movie_afisha_page(movie_url)
    movie_info = get_movie_genre_and_description(movie_afisha_page)
    return movie_info


def get_movie_image_url(kinopoisk_id):
    movie_image_url = 'https://st.kp.yandex.net/images/' \
                      'film_iphone/iphone360_%s.jpg' % kinopoisk_id
    return movie_image_url


def get_movies_urls_and_cinemas():
    movies_urls_and_cinemas = parse_afisha_list(fetch_afisha_page())
    return movies_urls_and_cinemas


def get_movies_data_for_template_engine(
        movies_urls_and_cinemas, count_movies_to_output):
    list_of_movies = list(movies_urls_and_cinemas)
    count_of_processes = 10
    with Pool(count_of_processes) as pool:
        rating_and_kinopoisk_id = pool.map(
            get_movie_kinopoisk_id_with_rating, list_of_movies)
    for number_of_movie_in_list, movie_name in enumerate(list_of_movies):
        movies_urls_and_cinemas[movie_name]['kinopoisk_id'] = \
            rating_and_kinopoisk_id[number_of_movie_in_list]['kinopoisk_id']
        movies_urls_and_cinemas[movie_name]['rating'] = \
            rating_and_kinopoisk_id[number_of_movie_in_list]['rating']
    sorted_movies = sorted(
        movies_urls_and_cinemas.items(),
        key=lambda x: x[1]['rating'],
        reverse=True)
    list_of_afisha_urls = [info['afisha_url'] for movie, info in sorted_movies]
    with Pool(count_of_processes) as pool:
        genre_and_description = pool.map(
            get_movie_info_from_afisha, list_of_afisha_urls)
    for number_of_movie_in_list, (movie_name, movie_info) in enumerate(sorted_movies):
        movie_info['genre'] = genre_and_description[number_of_movie_in_list]['genre']
        movie_info['description'] = \
            genre_and_description[number_of_movie_in_list]['description']
        movie_info['image'] = get_movie_image_url(movie_info['kinopoisk_id'])
    movies_info = sorted_movies[:count_movies_to_output]
    return movies_info
