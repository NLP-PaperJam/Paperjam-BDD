from typing import Any, Dict, List, Mapping, Optional
from gridfs import ClientSession
from pymongo import MongoClient
from logging import config
from datetime import datetime

import pymongo
import logging
import os, sys

config.fileConfig('logging.conf')
logger = logging.getLogger('mongoClient')

MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'pwj-db')
MONGO_REGISTER_COLLECTION = os.getenv('MONGO_REGISTER_COLLECTION', 'register')
MONGO_DOCUMENTS_COLLECTION = os.getenv('MONGO_DOCUMENTS_COLLECTION', 'documents')

def connect_mongo():
    """
        Connect to mongodb server.
        Check if connection is alive (or wait timeout).
        Log any errors.

        Returns
        -------
        client : pymongo.MongoClient, mongodb client with an active connection.
    """
    try:
        host = os.getenv('MONGO_URL', 'localhost:27017')
        username = os.getenv('MONGO_USERNAME', '')
        password = os.getenv('MONGO_PASSWORD', '')

        print(host)

        if username != '' and password != '':
            url = f'mongodb://{username}:{password}@{host}'
        else:
            url = f'mongodb://{host}'

        client = MongoClient(url, connect=True)
        client.is_mongos # Wait connection timeout if can't connect
        logger.debug(f'connect mongodb at {host}')
        return client
    except Exception as e:
        logger.exception(f'can\'t connect mongodb at {host}')
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


def add_meta_date(to_add):
    timestamp = datetime.now().timestamp()
    to_add.update({'insert_date':timestamp, 'last_update_date':timestamp})
    return to_add


def update_meta_date(to_update):
    timestamp = datetime.now().timestamp()
    if '$set' not in to_update:
        to_update['$set'].update({'last_update_date':timestamp}) 
    return to_update


def insert_one(
    document: dict,
    collection: pymongo.collection.Collection,
    bypass_document_validation: bool = False, 
    session: Optional[ClientSession] = None, 
    comment: Optional[Any] = None
    ):

    collection.insert_one(
        add_meta_date(document), 
        bypass_document_validation=bypass_document_validation, 
        session=session, 
        comment=comment)


def insert_many(
    documents: List[Dict], 
    collection: pymongo.collection.Collection, 
    ordered: bool = True, 
    bypass_document_validation: bool = False, 
    session: Optional[ClientSession] = None, 
    comment: Optional[Any] = None
    ):
    
    collection.insert_many(
        [add_meta_date(document) for document in documents],
        ordered = ordered,
        bypass_document_validation=bypass_document_validation, 
        session=session, 
        comment=comment)


def update_one(
    filter:Mapping[str, Any],
    update:dict,
    collection,
    upsert = False,
    bypass_document_validation = False,
    collation = None,
    array_filters = None,
    hint = None,
    session = None,
    let = None,
    comment = None):
    
    collection.update_one(
        filter, 
        update_meta_date(update), 
        upsert=upsert, 
        bypass_document_validation=bypass_document_validation, 
        collation=collation, 
        array_filters=array_filters, 
        hint=hint, 
        session=session, 
        let=let, 
        comment=comment)


# TODO : bulkWrite and use it in update_register and update_documents