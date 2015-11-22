import os

from sys import platform as _platform

from decimal import Decimal
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy import func

# os.remove('mydb')

# Initialize and set up database
# for development and simple test, use this engine
engine = create_engine('sqlite:///mydb', echo=False)

# To use the MySQL DB on the classes server, you should
#  * Use ssh and port forward port 3306 between your local machine and the classes server.
#    This can be done by the following command in a terminal:
#      ssh -L3306:localhost:3306 your_username@classes.csc.lsu.edu
#  * Use the following engine, set login to be the string of yourusername:yourpassword
#    and "dbname" to be the string of your mysql login name.
# from config import *
# engine = create_engine('mysql+mysqlconnector://'+login+'@localhost:3306/'+dbname)

Base = declarative_base()


class Rating(Base):
    __tablename__ = 'ratings'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    movie_id = Column(Integer, ForeignKey('movies.id'))
    rating = Column(Integer)

    user = relationship("User", back_populates="ratings")
    movie = relationship("Movie", back_populates="ratings")
    # genre = relationship("Genre", backref="ratings")

    def __repr__(self):
        return "<id = '%i', user_id='%i', movie_id='%i', rating='%i'>" % \
               (self.id, self.user_id, self.movie_id, self.rating)


class User(Base):
    __tablename__ = 'users'
    id = Column(Integer, primary_key=True)
    age = Column(Integer)
    gender = Column(String(1))
    occupation = Column(String(20))
    zipcode = Column(String(10))

    ratings = relationship("Rating", back_populates="user")

    def __repr__(self):
        return "<User(id='%i', age='%i', gender='%s', occupation='%s', zipcode='%s')>" % (self.id, self.age,
                                                                                          self.gender, self.occupation,
                                                                                          self.zipcode)


class Movie(Base):
    __tablename__ = 'movies'
    id = Column(Integer, primary_key=True)
    title = Column(String(50))
    releasedate = Column(String(15))
    videoreleasedate = Column(String(15))
    imdburl = Column(String(100))

    ratings = relationship("Rating", back_populates="movie")
    # http://docs.sqlalchemy.org/en/rel_1_0/orm/backref.html?highlight=back_populates
    genre = relationship("Genre", backref="movie")

    def __repr__(self):
        return "<Movie(id = '%i' title ='%s' releasedate = '%s' vidreleasedate='%s' imdburl='%s)>" % \
               (self.id, self.title, self.releasedate, self.videoreleasedate, self.imdburl)


class Genre(Base):
    __tablename__ = 'genres'
    # Have unique identifier for each genre, save space, avoid putting genre in movies table
    genreid = Column(Integer, primary_key=True)
    movieid = Column(Integer, ForeignKey('movies.id'), ForeignKey('ratings.movie_id'))
    genre = Column(String(20))

    def __repr__(self):
        return "<MovieGenre(genreid = '%i' movie id = '%i' genre= '%s')>" % (self.genreid, self.movieid, self.genre)


# TODO: Error Checking
Session = sessionmaker(bind=engine)
session = Session()

users = []
movies = []
genres = []
ratings = []
ratingdictionary = {}


def user_read(filename):
    usersfile = open(filename, 'r')
    for line in usersfile:
        user = line.strip('\n').split('|')
        newuser = User(id=int(user[0]), age=int(user[1]), gender=user[2], occupation=user[3], zipcode=user[4])
        users.append(newuser)

    session.add_all(users)
    session.commit()
    usersfile.close()


def movie_read(filename):
    moviefile = 0
    # Fix error in reading file encoding
    if _platform == 'darwin':
        moviefile = open(filename, 'r', encoding = "ISO-8859-1")
    else:
        moviefile = open(filename, 'r')
    for line in moviefile:
        movie = line.strip('\n').split('|')
        newmovie = Movie(id=int(movie[0]), title=movie[1], releasedate=movie[2], videoreleasedate=movie[3],
                         imdburl=movie[4])
        for num in range(5, len(movie)):
            if movie[num] == '1':
                newgenre = Genre(genreid=len(genres), movieid=int(movie[0]), genre=gentable(num))
                genres.append(newgenre)

        movies.append(newmovie)

    session.add_all(movies)
    session.add_all(genres)
    session.commit()
    moviefile.close()


def gentable(pos):
    return{
        5: 'unknown',
        6: 'Action',
        7: 'Adventure',
        8: 'Animation',
        9: 'Children\'s',
        10: 'Comedy',
        11: 'Crime',
        12: 'Documentary',
        13: 'Drama',
        14: 'Fantasy',
        15: 'Film-Noir',
        16: 'Horror',
        17: 'Musical',
        18: 'Mystery',
        19: 'Romance',
        20: 'Sci-Fi',
        21: 'Thriller',
        22: 'War',
        23: 'Western'
    }.get(pos)


def rating_read(filename):
    ratingfile = open(filename, 'r')
    for line in ratingfile:
        ratingline = line.strip('\n').split('\t')
        newrating = Rating(id=len(ratings), user_id=int(ratingline[0]), movie_id=int(ratingline[1]),
                           rating=int(ratingline[2]))
        ratings.append(newrating)
        ratingdictionary[(int(ratingline[0]), int(ratingline[1]))] = int(ratingline[2])
    session.add_all(ratings)
    session.commit()
    ratingfile.close()


def average_calc(movie_list, user_list, rating_dict, avg):
    print('Calculating averages...')
    # data/averages.item
    # TODO: Work for any file
    if os.path.isfile('data/averages.item'):
        file = open('data/averages.item', 'r')
        for line in file:
            new_avg = line.strip('\n').split('\t')
            new_key = new_avg[0].strip('()').split(',')
            new_item = int(new_key[0])
            new_other = int(new_key[1])
            avg[(new_item, new_other)] = float(new_avg[1])
        file.close()
        return

    f = open('data/averages.item', 'w')
    for item in movie_list:
        item_id = item[0]
        for other in movie_list:
            other_id = other[0]
            average = 0.0
            item_count = 0
            if item_id > other_id:
                for user in user_list:
                    user_id = user[0]
                    if (user_id, item_id) in rating_dict and ((user_id, other_id) in rating_dict):
                        item_count += 1
                        average += rating_dict[user_id, other_id] - rating_dict[user_id, item_id]
                # If at least one person has rated both movies
                if item_count != 0:
                    avg[(item_id, other_id)] = average / item_count
                    f.write('{}\t{}\n'.format((item_id, other_id), avg[(item_id, other_id)]))
                    f.flush()
    f.close()


def slope_one_recommend(target_user, target_movie, avg):
    rating_count = 0
    rating_total = 0.0
    # TODO: Work for any file
    if target_user == 7 and(target_movie == 599):
        print('oh no')
    target_user_ratings = session.query(Rating.movie_id).filter(Rating.user_id == target_user).all()
    for t_rating in target_user_ratings:
        movie_id = t_rating[0]
        user_rate_target = session.query(Rating.user_id).filter(Rating.movie_id == movie_id).all()
        for use in user_rate_target:
            user_id = use[0]
            if (user_id, target_movie) in ratingdictionary:
                if target_movie < movie_id:
                    rating_total += -1 * avg[(movie_id, target_movie)] + ratingdictionary[(target_user, movie_id)]
                    rating_count += 1
                else:
                    rating_total += avg[(target_movie, movie_id)] + ratingdictionary[(target_user, movie_id)]
                    rating_count += 1
    # for user in user_list:
    #     user_id = user[0]
    #     if target_user == user_id:
    #         continue
    #     for mov in movie_list:
    #         movie_id = mov[0]
    #         if (target_user, movie_id) not in ratingdictionary:
    #             continue
    #         if target_movie == movie_id:
    #             continue
    #         if (user_id, movie_id) in ratingdictionary and ((user_id, target_movie) in ratingdictionary):
    #             if target_movie < movie_id:
    #                 rating_total += -1 * avg[(movie_id, target_movie)] + ratingdictionary[(target_user, movie_id)]
    #                 rating_count += 1
    #             else:
    #                 rating_total += avg[(target_movie, movie_id)] + ratingdictionary[(target_user, movie_id)]
    #                 rating_count += 1
    # TODO: Print to file
    recommend_rating = rating_total / rating_count
    return recommend_rating


# TODO: Work for multiple files (number)
def slope_one_unknown(file_input_name, number):
    print('testing file {}'.format(number))
    input_file = open(file_input_name, 'r')
    if os.path.isfile("data/u{}.test.UnknownRating".format(number)):
        os.remove("data/u{}.test.UnknownRating".format(number))
    output_file = open("data/u{}.test.UnknownRating".format(number), 'w')
    for input_line in input_file:
        result = input_line.strip('\n').split('\t')
        output_file.write(result[0] + '\t' + result[1] + '\n')
        output_file.flush()
    print('test file {} created'.format(number))
    input_file.close()
    output_file.close()


def slope_one_testing(file_input_name, number, user_list, movie_list, avg_dict):
    input_file = open("data/u{}.test.UnknownRating".format(number), 'r')
    output_file = open("data/u{}.test.Prediction".format(number), 'w')
    print('testing file {}...'.format(number))
    input_file.seek(0)
    output_file.seek(0)
    for input_line in input_file:
        result = input_line.strip('\n').split('\t')
        us = int(result[0])
        mo = int(result[1])
        rat = slope_one_recommend(us, mo, avg_dict)
        output_file.write('{}\t{}\t{}\n'.format(us, mo, rat))
        output_file.flush()
    print('done!')
# Main Program execution
if not (engine.has_table('users')) or not (engine.has_table('movies')) or not (engine.has_table('genres')) \
        or not (engine.has_table('ratings')):
    print('need to add tables')
    # run this only once to create tables
    # TODO: Create separate function
    Base.metadata.create_all(engine)

if session.query(User).count() == 0:
    print("User table empty. Reading in user data...")
    try:
        user_read("data/u.user")
        print("User information read in")
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))

if session.query(Movie).count() == 0 and(session.query(Genre).count() == 0):
    print("Movie table empty. Reading in movie data...")
    try:
        movie_read("data/u.item")
        print("Movie item information read in")
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))

# TODO: rework for multiple u_i.base files
if session.query(Rating).count() == 0:
    print("Rating table empty, reading in ratings...")
    try:
        rating_read('data/u1.base')
        print("Rating information read in")
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
# # What is average rating by age group, gender, and occupation of the reviewers?
# q = session.query(User.age, User.gender, User.occupation, func.avg(Rating.rating)).join(Rating)\
#     .group_by(User.age, User.gender, User.occupation)
# for li in q:
#     print(li)
# # What is average rating by movie genre?
# for r in session.query(Genre.genre, func.avg(Rating.rating)).join(Rating).group_by(Genre.genre).all():
#     print(r)


# Slope one recommendation
averages = {}
query = session.query(User.id, Rating.movie_id, Rating.rating).join(Rating).all()
for li in query:
    if li[0] == 6 and(li[1] == 10):
        print('oh no')
    ratingdictionary[(li[0], li[1])] = li[2]

moviesfromdb = session.query(Movie.id).all()
usersfromdb = session.query(User.id).all()

average_calc(moviesfromdb, usersfromdb, ratingdictionary, averages)

try:
    # a = range(1,6)
    slope_one_unknown('data/u1.test', 1)
    slope_one_testing('data/u1.test', 1, usersfromdb, moviesfromdb, averages)
except IOError as e:
    print("I/O error({0}): {1}".format(e.errno, e.strerror))
