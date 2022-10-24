from datetime import datetime
from enum import Enum
from pathlib import Path
from client import *
import tempfile
from grobid_client.grobid_client import GrobidClient

import requests
import os

MONGO_URL = os.getenv('MONGO_URL', 'localhost:27017')
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


def get_batch(batch_size, register_filter):
    try:
        register_entries = list(register.find(register_filter, limit=batch_size))
        batch = zip(register_entries, [{'acl_id':entry['acl_id']} for entry in register_entries])
        logger.debug(f'success for get_batch with : batch_size = {batch_size}, register_filter = {register_filter}')
        return batch
    except:
        logger.exception(f'failure for get_batch with : batch_size = {batch_size}, register_filter = {register_filter}')
        sys.exit()


def update_register_steps(register, entry, name, code, msg, close=False):
    try:
        now = datetime.now()
        acl_id = entry['acl_id']
        steps = entry['steps']
        steps.append({'name':name, 'timestamp':now.timestamp(), 'code':code.value, 'msg':msg})
        # TODO : If step already exist raise error
        register.update_one({'acl_id':acl_id}, {'$set':{'close':close, 'steps':steps}})
        logger.debug(f'success for update_register_steps with : acl_id = {acl_id}, close = {close}, new_step = {steps[-1]}')
    except:
        logger.exception(f'failure for update_register_steps with : acl_id = {acl_id}, close = {close}, new_step = {steps[-1]}')


def get_s2_api(entry, document, url, fields):

    acl_id = entry['acl_id']
    name = 's2_api'
    code = StepCode.SUCCESS
    msg = ''
    close = False

    try:
        url = f'{url}ACL:{acl_id}?fields={fields}'
        res = requests.get(url) # TODO : move url
        if res.status_code == 200:
            document['s2'] = res.json()
            logger.debug(f'success for get_s2_api with : acl_id = {acl_id}, url = {res.url}')
            msg = '200'
        elif res.status_code == 404:
            logger.warning(f'404 for get_s2_api with : acl_id = {acl_id}, url = {res.url}')
            code = StepCode.TRASHED
            close = True
            msg = res.json()['error']
        else :
            logger.error(f'{res.status_code} for get_s2_api with : acl_id = {acl_id}, url = {res.url}')
            code = StepCode.ERROR
            close = True
            msg = res.json()['error']
    except Exception as e:
        logger.exception(f'failure for get_s2_api with : acl_id = {acl_id}, url = {res.url}')
        code = StepCode.ERROR
        close = True
        msg = 'Unknow exception triggered. Check logs.'
    finally:
        update_register_steps(register, entry, name, code, msg, close)
        return code

def get_acl_pdf(entry, document, dir_path : Path, url):

    acl_id = entry['acl_id']
    name = 'acl_pdf'
    code = StepCode.SUCCESS
    msg = ''
    close = False

    try:
        url = f'{url}/{acl_id}.pdf'
        res = requests.get(url) # TODO : move url
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
        update_register_steps(register, entry, name, code, msg, close)
        return code


def post_grobid_api(entry : dict, document : dict, config_path : Path, dir_path : Path):    
    
    acl_id = entry['acl_id']
    name = 'grobid_api'
    code = StepCode.SUCCESS
    close = False
    msg = ''

    try:
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

        if (dir_path / f'{acl_id}.tei.xml').is_file():
            # TODO : should we test file content is parsable (bs4, lxml ...)
            document['grobid'] = (dir_path / f'{acl_id}.tei.xml').read_text(encoding='utf-8')
            msg = '200'
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
        msg = 'Unknow exception triggered. Check logs.' if msg == '' else msg
    finally:
        update_register_steps(register, entry, name, code, msg, close)
        return code

if __name__ == '__main__':
    
    logger.info(f'start connecting mongodb and retrieve register and documents ...')
    db = get_db(connect_mongo(MONGO_URL), MONGO_DB_NAME)
    register = get_collection(db, MONGO_REGISTER_COLLECTION)
    documents = get_collection(db, MONGO_DOCUMENTS_COLLECTION)
    # TODO : Check if register has same closed entries count than documents count
    
    logger.info(f'create register entries batch ...')
    batch = get_batch(BATCH_DEFAULT_SIZE, BATCH_DEFAULT_FILTER)

    entry, doc = list(batch)[0]
    url = S2_API_URL
    fields = S2_API_FIELDS

    get_s2_api(entry, doc, S2_API_URL, S2_API_FIELDS)
    with tempfile.TemporaryDirectory() as tmp:
        get_acl_pdf(entry, doc, Path(tmp), ACL_PDF_URL)
        post_grobid_api(entry, doc, Path('tools/grobid_config.json'), Path(tmp))
    # update_register_steps(register, entry, 's2', StepCode.SUCCESS, '')
    # register.update_one({'acl_id':entry['acl_id']}, ) 


    register.database.client.close()
    logger.info(f'all process ended')
