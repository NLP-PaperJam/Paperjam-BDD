from pymongo import MongoClient
from logging import config

import logging
import sys

config.fileConfig('logging.conf')
logger = logging.getLogger('mongoClient')

def connect_mongo(url):
    """
        Connect to mongodb server.
        Check if connection is alive (or wait timeout).
        Log any errors.

        Parameters
        ----------
        url : str, mongodb url to connect.

        Returns
        -------
        client : pymongo.MongoClient, mongodb client with an active connection.
    """
    try:
        client = MongoClient(f'{url}', connect=True)
        client.is_mongos # Wait connection timeout if can't connect
        logger.debug(f'connect mongodb at {url}')
        return client
    except Exception as e:
        logger.exception(f'can\'t connect mongodb at {url}')
        sys.exit()


def get_db(client, name):
    """
        Get a mongodb database by name. 
        Raise exception if not found.
        Log any errors.

        Parameters
        ----------
        client : pymongo.MongoClient, mongodb client with an active connection.
        name : str, name of database to get.
        
        Returns
        -------
        db : pymongo.database.Database, mongodb database with an active connection.
    """
    try:
        if name not in client.list_database_names():
            raise
        logger.debug(f'database {name} found')
        return client[name]
    except Exception as e:
        logger.exception(f'database  `{name}` not found')
        sys.exit()


def get_collection(db, name):
    """
        Get a mongodb collection by name. 
        Raise exception if not found.
        Log any errors.

        Parameters
        ----------
        db : pymongo.database.Database, mongodb database with an active connection.
        name : str, name of collection to get.
        
        Returns
        -------
        collection : pymongo.collection.Collection, mongodb collection, with an active connection.
    """
    try:
        if name not in db.list_collection_names():
            raise
        logger.debug(f'collection {name} found')
        return db[name]    
    except Exception as e:
        logger.exception(f'collection  `{name}` not found')
        sys.exit()

