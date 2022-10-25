from datetime import datetime
from enum import Enum
from pathlib import Path
from time import sleep
from typing import Iterable
from client import *
import tempfile
from grobid_client.grobid_client import GrobidClient

import requests
import os


MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'pwj-db')
MONGO_REGISTER_COLLECTION = os.getenv('MONGO_REGISTER_COLLECTION', 'register')
MONGO_DOCUMENTS_COLLECTION = os.getenv('MONGO_DOCUMENTS_COLLECTION', 'documents')


BATCH_DEFAULT_FILTER = {'close': False}
BATCH_DEFAULT_SIZE = 100


S2_API_URL = 'https://api.semanticscholar.org/graph/v1/paper/'
S2_API_FIELDS = 'paperId,externalIds,url,title,abstract,venue,year,referenceCount,citationCount,influentialCitationCount,isOpenAccess,fieldsOfStudy,s2FieldsOfStudy,publicationTypes,publicationDate,journal,authors,authors.externalIds,authors.url,authors.name,authors.aliases,authors.affiliations,authors.homepage,authors.paperCount,authors.citationCount,authors.hIndex,citations,citations.corpusId,citations.externalIds,citations.url,citations.title,citations.abstract,citations.venue,citations.year,citations.referenceCount,citations.citationCount,citations.influentialCitationCount,citations.isOpenAccess,citations.fieldsOfStudy,citations.s2FieldsOfStudy,citations.publicationTypes,citations.publicationDate,citations.journal,citations.authors,references,references.externalIds,references.url,references.title,references.abstract,references.venue,references.year,references.referenceCount,references.citationCount,references.influentialCitationCount,references.isOpenAccess,references.fieldsOfStudy,references.s2FieldsOfStudy,references.authors,references.publicationTypes,references.publicationDate,references.journal'
ACL_PDF_URL= 'https://aclanthology.org/'


config.fileConfig('logging.conf')
logger = logging.getLogger('updateDocument')


class StepCode(Enum):
    SUCCESS = 1
    TRASHED = 101
    ERROR = 102


def get_batch(register, batch_size, register_filter):
    try:
        register_entries = list(register.find(register_filter, limit=batch_size))
        batch = register_entries, [{'acl_id':entry['acl_id']} for entry in register_entries]
        logger.debug(f'success for get_batch with : batch_size = {batch_size}, register_filter = {register_filter}')
        return batch
    except:
        logger.exception(f'failure for get_batch with : batch_size = {batch_size}, register_filter = {register_filter}')
        sys.exit()


def update_register_steps(entry, name, code, msg, close=False):
    now = datetime.now()
    # TODO : If step already exist raise error
    entry['steps'].append({'name':name, 'timestamp':now.timestamp(), 'code':code.value, 'msg':msg})
    entry['close'] = close


def get_s2_api(batch, url : str, fields : str):
    
    for entry, document in zip(*batch):
        
        if entry['close']:
            continue

        try:
            acl_id = entry['acl_id']
            name = 's2_api'
            code = StepCode.SUCCESS
            msg = ''
            close = False

            res = requests.get(f'{url}ACL:{acl_id}?fields={fields}')  # TODO : move url
            if res.status_code == 200:
                document['s2'] = res.json()
                logger.debug(f'success for get_s2_api with : acl_id = {acl_id}')
                msg = '200'
            elif res.status_code == 404:
                logger.warning(f'404 for get_s2_api with : acl_id = {acl_id}')
                code = StepCode.TRASHED
                close = True
                msg = res.json()['error']
            else :
                logger.error(f'{res.status_code} for get_s2_api with : acl_id = {acl_id}')
                code = StepCode.ERROR
                close = True
                msg = res.json()['error'] if 'error' in res.json() else 'Unknow status code triggered. Check logs.'
        except Exception as e:
            logger.exception(f'failure for get_s2_api with : acl_id = {acl_id}')
            code = StepCode.ERROR
            close = True
            msg = 'Unknow exception triggered. Check logs.'
        finally:
            update_register_steps(entry, name, code, msg, close)


def get_acl_pdf(batch : Iterable, dir_path : Path, url : str):
    name = 'acl_pdf'

    for entry, _ in zip(*batch):
        
        if entry['close']:
            continue

        try:
            acl_id = entry['acl_id']
            code = StepCode.SUCCESS
            msg = ''
            close = False

            res = requests.get(f'{url}/{acl_id}.pdf') # TODO : move url
            if res.status_code == 200:
                (dir_path / f'{acl_id}.pdf').write_bytes(res.content)
                logger.debug(f'success for get_acl_pdf with : acl_id = {acl_id}, dir_path = {dir_path}, url = {url}')
            elif res.status_code == 404:
                logger.warning(f'404 for get_acl_pdf with : acl_id = {acl_id}, dir_path = {dir_path}, url = {url}')
                code = StepCode.TRASHED
                close = True
            else :
                logger.error(f'{res.status_code} for get_acl_pdf with : acl_id = {acl_id}, dir_path = {dir_path}, url = {url}')
                code = StepCode.ERROR
                close = True
            msg = str(res.status_code)
        except Exception as e:
            logger.exception(f'failure for get_acl_pdf with : acl_id = {acl_id}, dir_path = {dir_path}, url = {url}')
            code = StepCode.ERROR
            close = True
            msg = 'Unknow exception triggered. Check logs.'
        finally:
            update_register_steps(entry, name, code, msg, close)


def post_grobid_api(batch : Iterable, config_path : Path, dir_path : Path):    
    name = 'grobid_api'

    if len(list(dir_path.glob('*.pdf'))) == 0:
        return

    try:
        code = StepCode.SUCCESS
        close = False
        msg = ''

        grobid_client = GrobidClient(config_path=config_path)
        grobid_client.process( # TODO : Move configuration
            "processFulltextDocument", 
            dir_path, 
            dir_path, 
            consolidate_header=True,
            consolidate_citations=True,
            segment_sentences=True,
            include_raw_affiliations=True,
            include_raw_citations=True
        )
    except:
        logger.exception(f'failure for post_grobid_api with : dir_path = {dir_path}, url = {config_path}')
        code = StepCode.ERROR
        close = True
        msg = 'Unknow exception triggered during grobid processing. Check logs.' if msg == '' else msg
        for entry in batch[0]:
            update_register_steps(entry, name, code, msg, close)
        return

    for entry, document in zip(*batch):

        if entry['close']:
            continue

        try:

            acl_id = entry['acl_id']
            close = False
            msg = ''

            if (dir_path / f'{acl_id}.tei.xml').is_file():
                # TODO : should we test file content is parsable (bs4, lxml ...)
                document['grobid'] = (dir_path / f'{acl_id}.tei.xml').read_text(encoding='utf-8')
                logger.debug(f'success for post_grobid_api with : acl_id = {acl_id}, dir_path = {dir_path}, url = {config_path}')
            elif (dir_path / f'{acl_id}.txt').is_file():
                msg = (dir_path / f'{acl_id}.tei.xml').read_text(encoding='utf-8') 
                raise
            else:
                msg = f'file not found : {acl_id}(.tei.xml|.txt)'
                raise
        except:
            logger.exception(f'failure for post_grobid_api with : acl_id = {acl_id}, dir_path = {dir_path}, url = {config_path}')
            code = StepCode.ERROR
            close = True
            msg = 'Unknow exception triggered during grobid result handling. Check logs.' if msg == '' else msg
        finally:
            update_register_steps(entry, name, code, msg, close)


def process_batch():
    t = datetime.now().timestamp()

    logger.info(f'start connecting mongodb and retrieve register and documents ...')
    db = get_db(connect_mongo(), MONGO_DB_NAME)
    register = get_collection(db, MONGO_REGISTER_COLLECTION)
    documents = get_collection(db, MONGO_DOCUMENTS_COLLECTION)
    # TODO : Check if register has same closed entries count than documents count
    
    logger.info(f'create register entries batch ...')
    batch = get_batch(register, BATCH_DEFAULT_SIZE, BATCH_DEFAULT_FILTER)
    if len(batch[0]) == 0:
        return False

    logger.info(f'start requesting s2 api ...')
    get_s2_api(batch, S2_API_URL, S2_API_FIELDS)

    with tempfile.TemporaryDirectory() as tmp:
        logger.info(f'start downloading acl pdf ...')
        get_acl_pdf(batch, Path(tmp), ACL_PDF_URL)
        logger.info(f'start requesting grobid api ...')
        post_grobid_api(batch, Path('tools/grobid_config.json'), Path(tmp))

    logger.info(f'start updating database ...')

    docs = [doc for entry, doc in zip(*batch) if entry['close'] == False]
    insert_many(docs, documents)
    logger.debug(f'success for insert documents batch ({len(docs)}/{BATCH_DEFAULT_SIZE}) ...')
    
    for entry in batch[0]:
        update_one({'acl_id':entry['acl_id']}, {'$set': {'close':True, 'steps':entry['steps']}}, register)
    logger.debug(f'success updating register entries ...')
    
    t = datetime.now().timestamp() - t
    sleep(max(0, 5 * 60 - t))
    register.database.client.close()
    logger.info(f'batch process ended in {t} ({t/BATCH_DEFAULT_SIZE} by entry)')
    return True

if __name__ == '__main__':
    loop = True
    while loop:
        loop = process_batch()

    logger.info(f'all process ended')
    
