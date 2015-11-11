import sqlalchemy

from sqlalchemy import create_engine

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

from sqlalchemy.ext.declarative import declarative_base
Base = declarative_base()

from sqlalchemy import Column, Integer, String
from sqlalchemy import ForeignKey
from sqlalchemy.orm import relationship


class Rating(Base):
    __tablename__ = 'ratings'
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id'))
    movie_id = Column(Integer, ForeignKey('movies.id'))
    rating = Column(Integer)

    user = relationship("User", back_populates="ratings")
    movie = relationship("Movie", back_populates="ratings")

    def __repr__(self):
        return "<%i, %i, %i>" % (self.user_id, self.movie_id, self.rating)


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
    name = Column(String(50))
    releasedate = Column(String(15))
    videoreleasedate = Column(String(15))
    imdburl = Column(String(100))

    ratings = relationship("Rating", back_populates="movie")
    moviegenre = relationship("Genre", back_populates="movie_genres")

    def __repr__(self):
        return "<Movie(name ='%s')>" % self.name

class Genre():
    __tablename__ = 'movie_genres'
    movieid = Column(Integer, ForeignKey('movies.id'), primary_key=True )
    genre = Column(String(20))

    movie = relationship("Movie", back_populates="movie")

    def __repr__(self):
        return "<MovieGenre(id ='%i' genre='%s')>" % (self.movieid, self.genre)

# run this only once to create tables
#Base.metadata.create_all(engine)

from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()

# run this only once to load data into tables
# users = [User(age=20, occupation='student'), User(age=40, occupation='engineer')]
# session.add_all(users)
# movies = [Movie(name='star wars III'), Movie(name='Forrest Gump')]
# session.add_all(movies)
# ratings = [Rating(user=users[0], movie=movies[0], rating=2),
#            Rating(user=users[0], movie=movies[1], rating=3),
#            Rating(user=users[1], movie=movies[0], rating=1),
#            Rating(user=users[1], movie=movies[1], rating=4)]
# session.add_all(ratings)
# session.commit()

#
# for r in session.query(Rating).all():
#     print(r)
#
# for r in session.query(User).all():
#     print(r)
#
# for r in session.query(Movie).all():
#     print(r)
#
# for r in session.query(User).all()[0].ratings:
#     print(r.rating)
#
# for r in session.query(Movie).all()[0].ratings:
#     print(r.rating)

users = []
movies = []
def userRead(fileName):
    usersFile = open('data/' + fileName, 'r')
    for line in usersFile:
        user = line.strip('\n').split('|')
        newuser = User(id=int(user[0]), age=int(user[1]), gender=user[2], occupation=user[3], zipcode=user[4])
        users.append(newuser)

def movieRead(fileName):
    movieFile = open('data/' + fileName, 'r')
    for line in movieFile:
        movie = line.strip('\n').split('|')
        newmovie = movie
        movies.append(newmovie)

# 
# session.add_all(users)
# session.commit()

for u in session.query(User).all():
    print(u.zipcode)

