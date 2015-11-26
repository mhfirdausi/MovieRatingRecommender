# Slope One Movie Recommendation System
# CS 2730
# December 1, 2015
# Mohammad Firdausi
# Ben Graham
# Robert Pohlman

import os

from sys import platform as _platform

import math
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import ForeignKey
from sqlalchemy import distinct
from sqlalchemy import inspect
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import make_transient
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
# login = 'cs273009:ofepopja'
# dbname = 'cs273004 -password=eopo6b'
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


Session = sessionmaker(bind=engine)
session = Session()

users = []
movies = []
genres = []
ratings = []
ratingdictionary = {}
averages = {}


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
    # Fix error in reading file encoding
    if _platform == 'darwin':
        moviefile = open(filename, 'r', encoding="ISO-8859-1")
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


def average_calc(movie_list, user_list, file_num):
    print('Calculating averages for file {}...'.format(file_num))
    # data/averages.item
    if os.path.isfile('data/averages_{}.item'.format(file_num)):
        file = open('data/averages_{}.item'.format(file_num), 'r')
        for line in file:
            new_avg = line.strip('\n').split('\t')
            new_key = new_avg[0].strip('()').split(',')
            new_item = int(new_key[0])
            new_other = int(new_key[1])
            averages[(new_item, new_other)] = float(new_avg[1])
        file.close()
        return

    f = open('data/averages_{}.item'.format(file_num), 'w')
    for item in movie_list:
        item_id = item[0]
        for other in movie_list:
            other_id = other[0]
            average = 0.0
            item_count = 0
            if item_id > other_id:
                for user in user_list:
                    user_id = user[0]
                    if (user_id, item_id) in ratingdictionary and ((user_id, other_id) in ratingdictionary):
                        item_count += 1
                        average += ratingdictionary[user_id, other_id] - ratingdictionary[user_id, item_id]
                # If at least one person has rated both movies
                if item_count != 0:
                    averages[(item_id, other_id)] = average / item_count
                    f.write('{}\t{}\n'.format((item_id, other_id), averages[(item_id, other_id)]))
                    f.flush()
    f.close()


def slope_one_recommend(target_user, target_movie):
    rating_count = 0
    rating_total = 0.0
    target_user_ratings = session.query(Rating.movie_id).filter(Rating.user_id == target_user).all()
    for t_rating in target_user_ratings:
        movie_id = t_rating[0]
        substmt = session.query(Rating.user_id).filter(Rating.movie_id == target_movie).subquery()
        stmt = session.query(distinct(Rating.user_id)).filter(Rating.movie_id == movie_id).filter(Rating.user_id == substmt.c.user_id).all()
        for use in stmt:
            if target_movie < movie_id:
                rating_total += -1 * averages[(movie_id, target_movie)] + ratingdictionary[(target_user, movie_id)]
                rating_count += 1
            else:
                rating_total += averages[(target_movie, movie_id)] + ratingdictionary[(target_user, movie_id)]
                rating_count += 1
    if rating_count <= 0:
        return 0
    recommend_rating = rating_total / rating_count
    return recommend_rating


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


def slope_one_testing(number):
    input_file = open("data/u{}.test.UnknownRating".format(number), 'r')
    line_count = sum(1 for li in input_file)
    if os.path.isfile("data/u{}.test.Prediction".format(number)):
        check_out = open("data/u{}.test.Prediction".format(number), 'r')
        out_count = sum(1 for li in check_out)
        if out_count >= line_count:
            print('Predictions already generated for file {}.'.format(number))
            check_out.close()
            input_file.close()
            return
        check_out.close()
    output_file = open("data/u{}.test.Prediction".format(number), 'w')
    print('Testing file {}...'.format(number))
    input_file.seek(0)
    output_file.seek(0)
    for input_line in input_file:
        result = input_line.strip('\n').split('\t')
        us = int(result[0])
        mo = int(result[1])
        rat = slope_one_recommend(us, mo)
        output_file.write('{}\t{}\t{}\n'.format(us, mo, rat))
        output_file.flush()
    print('Done testing file {}!'.format(number))


def performance_measure(test_file, prediction_file):
    test = open(test_file, 'r')
    prediction = open(prediction_file, 'r')
    sum_error = 0.0
    count_error = 0
    for line1, line2 in zip(test, prediction):
        # UID   MID    Rating
        a_rate_line = line1.strip('\n').split('\t')
        p_rate_line = line2.strip('\n').split('\t')
        actual_rating = float(a_rate_line[2])
        predict_rating = float(p_rate_line[2])
        sum_error += math.pow((predict_rating - actual_rating), 2)
        count_error += 1
    return sum_error / count_error

# Main Program execution
if not (engine.has_table('users')) or not (engine.has_table('movies')) or not (engine.has_table('genres')) \
        or not (engine.has_table('ratings')):
    print('need to add tables')
    # run this only once to create tables
    Base.metadata.create_all(engine)



# TODO: Everything inside this 1 - 5 loop: MSE
# TODO: Cumulative average MSE
file_nums = range(1, 6)
for count in file_nums:
    if count == 1:
        continue
    sum_mse = 0.0
    users = []
    movies = []
    genres = []
    rating = []
    averages.clear()
    ratingdictionary.clear()
    Base.metadata.drop_all(engine)
    session.expunge_all()
    Base.metadata.create_all(engine)
    print('Reading in ratings for file: {}'.format(count))
    try:
        user_read("data/u.user")
        print("User information read in")
        movie_read("data/u.item")
        print("Movie item information read in")
        rating_read('data/u{}.base'.format(count))
        print('Ratings from file {} read in'.format(count))
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))
    # print('\nWhat is average rating by age group, gender, and occupation of the reviewers?')
    # q = session.query(User.age, User.gender, User.occupation, func.avg(Rating.rating)).join(Rating)\
    #     .group_by(User.age, User.gender, User.occupation)
    # print('{:^5s}|{:^9s}|{:^15s}|{:^8s}'.format('Age', 'Gender', 'Occupation', 'Rating'))
    # print('-'*37)
    # for li in q:
    #     print('{:^5d} {:^9s} {:^15s} {:^8.3f}'.format(li[0], li[1], li[2], li[3]))
    # print('\nWhat is average rating by movie genre?')
    # print('{:^15s}|{:^8s}'.format('Genre', 'Rating'))
    # print('-'*24)
    # for r in session.query(Genre.genre, func.avg(Rating.rating)).join(Rating).group_by(Genre.genre).all():
    #     print('{:^15s} {:^8.3f}'.format(r[0], r[1]))
    moviesfromdb = session.query(Movie.id).all()
    usersfromdb = session.query(User.id).all()
    average_calc(moviesfromdb, usersfromdb, count)
    slope_one_unknown('data/u{}.test'.format(count), count)
    if count < 3:
        slope_one_testing(count)
    print('Done with file {}'.format(count))
    # TODO: Every file performance measure
    mse = performance_measure('data/u1.test', 'data/u1.test.Prediction')
    print("MSE for file {} = {:.3f}".format(1, mse))
