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
    title = Column(String(50))
    releasedate = Column(String(15))
    videoreleasedate = Column(String(15))
    imdburl = Column(String(100))

    ratings = relationship("Rating", back_populates="movie")
   # moviegenre = relationship("Genre", back_populates="movie")

    def __repr__(self):
        return "<Movie(id = '%i' title ='%s' releasedate = '%s' vidreleasedate='%s' imdburl='%s)>" % (self.id,
                                                                                                     self.title,
                                                                                                     self.releasedate,
                                                                                                     self.videoreleasedate,
                                                                                                     self.imdburl)


class Genre(Base):
    __tablename__ = 'genres'
    movieid = Column(Integer, ForeignKey('movies.id'), primary_key=True )
    genre = Column(String(20))

    #movie = relationship("Movie", back_populates="genres")

    def __repr__(self):
        return "<MovieGenre(id ='%i' genre='%s')>" % (self.movieid, self.genre)


from sqlalchemy.orm import sessionmaker
Session = sessionmaker(bind=engine)
session = Session()


# TODO: Error Checking

users = []
movies = []


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
        newmovie = Movie(id=int(movie[0]), title=movie[1], releasedate=movie[2], videoreleasedate=movie[3], imdburl=movie[4])
        movies.append(newmovie)
        #print(newmovie)

    session.add_all(movies)
    session.commit()


# Main Program execution
if not(engine.has_table('users')) or not(engine.has_table('movies')) or not(engine.has_table('genres')) or not(engine.has_table('ratings')):
    print('need to add tables')
    # run this only once to create tables
    # TODO: Create seperate function
    Base.metadata.create_all(engine)

if session.query(User).count() == 0:
    print("User table empty. Reading in user data...")
    try:
        userread("data/u.user")
        print("User information read in")
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))


if session.query(Movie).count() == 0:
    print("Movie table empty. Reading in movie data...")
    try:
        movieread("data/u.item")
        print("Movie item information read in")
    except IOError as e:
        print("I/O error({0}): {1}".format(e.errno, e.strerror))

for u in session.query(User).all():
    print(u)
for m in session.query(Movie).all():
    print(m)
