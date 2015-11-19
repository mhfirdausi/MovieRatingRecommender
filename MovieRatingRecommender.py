import os

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship
from sqlalchemy.orm import sessionmaker

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

    def __repr__(self):
        return "<id = '%i', user_id='%i', movie_id='%i', rating='%i'>" % (self.id, self.user_id, self.movie_id, self.rating)


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
        return "<Movie(id = '%i' title ='%s' releasedate = '%s' vidreleasedate='%s' imdburl='%s)>" % (self.id,
                                                                                                      self.title,
                                                                                                      self.releasedate,
                                                                                                      self.videoreleasedate,
                                                                                                      self.imdburl)


class Genre(Base):
    __tablename__ = 'genres'
    # Have unique identifier for each genre, save space, avoid putting genre in movies table
    genreid = Column(Integer, primary_key=True)
    movieid = Column(Integer, ForeignKey('movies.id'))
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

def userread(filename):
    usersfile = open(filename, 'r')
    for line in usersfile:
        user = line.strip('\n').split('|')
        newuser = User(id=int(user[0]), age=int(user[1]), gender=user[2], occupation=user[3], zipcode=user[4])
        users.append(newuser)

    session.add_all(users)
    session.commit()


def movieread(filename):
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

def ratingread(filename):
    ratingfile = open(filename, 'r')
    for line in ratingfile:
        ratingline = line.strip('\n').split('\t')
        newrating = Rating(id=len(ratings), user_id=int(ratingline[0]), movie_id=int(ratingline[1]),
                           rating=int(ratingline[2]))
        ratings.append(newrating)
    session.add_all(ratings)
    session.commit()
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
        userread("data/u.user")
        print("User information read in")
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))

if session.query(Movie).count() == 0 and(session.query(Genre).count() == 0):
    print("Movie table empty. Reading in movie data...")
    try:
        movieread("data/u.item")
        print("Movie item information read in")
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))

if session.query(Rating).count() == 0:
    print("Rating table empty, reading in ratings...")
    try:
        ratingread('data/u1.base')
        print("Rating information read in")
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))

# for m in session.query(Rating).all():
#     print(m)