from client import *


MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'pwj-db')
MONGO_REGISTER_COLLECTION = os.getenv('MONGO_REGISTER_COLLECTION', 'register')
MONGO_DOCUMENTS_COLLECTION = os.getenv('MONGO_DOCUMENTS_COLLECTION', 'documents')


config.fileConfig('logging.conf')
logger = logging.getLogger('mongoClient')


if __name__ == '__main__':
    db = connect_mongo()
    
    db[MONGO_DB_NAME]
    db = get_db(db, MONGO_DB_NAME)

    db[MONGO_REGISTER_COLLECTION]
    get_collection(db, MONGO_REGISTER_COLLECTION)

    db[MONGO_DOCUMENTS_COLLECTION]
    get_collection(db, MONGO_DOCUMENTS_COLLECTION)

    logger.info(f'all process ended')
