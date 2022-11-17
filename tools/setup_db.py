from client import *


MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'pwj-db')
MONGO_REGISTER_COLLECTION = os.getenv('MONGO_REGISTER_COLLECTION', 'register')
MONGO_DOCUMENTS_COLLECTION = os.getenv('MONGO_DOCUMENTS_COLLECTION', 'documents')


config.fileConfig('logging.conf')
logger = logging.getLogger('mongoClient')


if __name__ == '__main__':
    host = os.getenv('MONGO_URL', 'localhost:27017')
    username = os.getenv('MONGO_USERNAME', '')
    password = os.getenv('MONGO_PASSWORD', '')

    url = f"mongodb://{username}:{password}@{host}"
    print(url)

    client = MongoClient(url)
    db = client['pwj-db']
    # print(db.list_collection_names())
    print(db['documents'].count_documents({}))

    # db[MONGO_DB_NAME]
    # db = get_db(db, MONGO_DB_NAME)

    # db[MONGO_REGISTER_COLLECTION]
    # print(get_collection(db, MONGO_REGISTER_COLLECTION).count_documents({}))

    # db[MONGO_DOCUMENTS_COLLECTION]
    # print(get_collection(db, MONGO_DOCUMENTS_COLLECTION).count_documents({}))

    # logger.info(f'all process ended')
