
# -*- coding: utf-8 -*-
"""
Created on Mon May 29 10:30:55 2023

@author: luis.caro
"""

from contextlib import contextmanager
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
# from defaultapp.config import Config


@contextmanager
def session_scope():
    # SQLAlchemy usage with a different Session as Flask app, to use outside flask context calls

    # DB_name = 'oahubDB'
    # DB_name = 'evaluacionDB'
    # DB_name = 'colegio'
    # DB_host = '40.84.58.139'
    # pwd = 'QqZ3x4rq%408*&6H)+Jp(e'
    # connection_string = 'mysql://readonly_user:'+pwd+'@'+DB_host+':3306/'+DB_name

    #.-.-.-.- nuevo -AYJ
    # DB_host = '20.119.225.168'
    # DB_name = 'ayjDB'
    # pwd = 'QqZ3x4rq%408*&6H)+Jp(e'
    # connection_string = 'mysql://readonly_user:'+pwd+'@'+DB_host+':3306/'+DB_name
    # Usuario readonly_user
    #:_:_:_:_:_:_:

    #.-.-.-.- evaluación colegio
    DB_host = '20.119.225.168'
    # DB_name = 'evaluacionDB'
    DB_name = 'estudiantesDB'
    pwd = 'QqZ3x4rq%408*&6H)+Jp(e'
    connection_string = 'mysql://readonly_user:'+pwd+'@'+DB_host+':3306/'+DB_name
    # Usuario readonly_user
    #:_:_:_:_:_:_:


    some_engine = create_engine(connection_string)

    SessionEng = sessionmaker(bind=some_engine)
    session = SessionEng()
    session.expire_on_commit = False
    try:
        # this is where the "work" happens!
        yield session
        # always commit changes!
        session.commit()
    except:
        # if any kind of exception occurs, rollback transaction
        session.rollback()
        raise
    finally:
        session.close()
