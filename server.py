from flask import Flask, render_template
from cinemas import get_movies_data_for_template_engine
from werkzeug.contrib.cache import FileSystemCache
import tempfile

app = Flask(__name__)

tmpfile = tempfile.gettempdir()
cache = FileSystemCache(cache_dir=tmpfile)


@app.route('/')
def films_list():
    movies_data = cache.get('movies_data')
    if movies_data is None:
        movies_data = get_movies_data_for_template_engine(10)
        cache.set('movies_data', movies_data, timeout=60 * 60)
    return render_template('films_list.html', movies=movies_data)


if __name__ == "__main__":
    app.run()
