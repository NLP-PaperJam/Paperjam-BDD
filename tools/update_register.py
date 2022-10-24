from client import *

from tqdm import tqdm
import requests
import logging
from logging import config
import os, re, sys
import gzip

MONGO_URL = os.getenv('MONGO_URL', 'localhost:27017')
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'pwj-db')
MONGO_REGISTER_COLLECTION = os.getenv('MONGO_REGISTER_COLLECTION', 'register')

ACL_ANTHOLOGY_URL = 'https://aclanthology.org/anthology.bib.gz'
ACL_ID_PATTERN = re.compile(r"(?:url = \".*/)([\w\-\.]*?)(?:(?:\.pdf)?\",)")

config.fileConfig('logging.conf')
logger = logging.getLogger('updateRegister')


def get_url_content(url):
    """
        Get content from http(s) request for given url. 
        Raise exception if response don't have status code equal to 200.
        Log any errors.

        Parameters
        ----------
        url : str, http(s) url to request.
        
        Returns
        -------
        content : bytes, content of http(s) response.
    """
    res = None
    try:
        res = requests.get(url)
        if res.status_code != 200:
            raise
        logger.debug(f'GET requests at {res.url} ended with success ({res.status_code})')
        return res.content
    except Exception as e:
        code = None
        if res != None:
            code = res.status_code
            url = res.url
        logger.exception(f'GET requests at {url} failed with status code {code}')
        sys.exit()


def gz_to_text(content, encoding='utf-8'):
    """
        Decompress gz content and decode it. 
        Log any errors.

        Parameters
        ----------
        content : bytes, gz content to decompress.
        encoding : str, encoding to use to decode.
        
        Returns
        -------
        text : str, result from decompress and decode.
    """
    try:
        txt = gzip.decompress(content)
        txt = txt.decode(encoding)
        logger.debug(f'success for content decompression ({encoding})')
        return txt
    except Exception as e:
        logger.exception(f'failure during content decompression ({encoding})')
        sys.exit()


def find_all_in_text(pattern, text):
    """
        Apply regex pattern to a text.
        Return only unique matchs. 
        Log any errors.

        Parameters
        ----------
        pattern : str or re.Pattern, regex pattern to use.
        text : str, text to apply pattern.
        
        Returns
        -------
        matchs : set, unique matchs.
    """

    try:
        pattern = re.compile(pattern) if isinstance(pattern, str) else pattern
        matchs = pattern.findall(text)
        c = len(matchs) # store match count with duplicates
        matchs = set(matchs)
        logger.debug(f'success for find all in text. {c} matchs ({len(matchs)} unique) with pattern : {pattern.pattern}')
        return matchs
    except Exception as e:
        logger.exception(f'failure during find all in text. pattern : {pattern.pattern}')
        sys.exit()


def create_register_entry(acl_id, close=False, steps=None):
    """
        Create a new register entry.

        Parameters
        ----------
        acl_id : str, acl_id of register entry.
        close : bool, default=True is entry closed or opened. 
        steps : dict, default=None step info list, create empty one if None.
        
        Returns
        -------
        entry : dict, register entry data.
    """
    return {
        'acl_id': acl_id,
        'close': close,
        'steps': steps if steps else [] 
    }


def update_register(register, acl_ids):
    """
        Add in register collection a new default entry for each acl_id given.
        Only add entry if the acl_id is not already in register.
        Log any errors.

        Parameters
        ----------
        register : pymongo.collection.Collection, register collection.
        acl_ids : list[str], acl_ids to add in register.        
    """
    try:
        start_count = register.count_documents({})
        for acl_id in tqdm(acl_ids, desc='Update register entries :'):
            if not register.find_one({'acl_id':acl_id}):
                register.insert_one(create_register_entry(acl_id))
        end_count = register.count_documents({})
        logger.debug(f'success for update register. added {end_count-start_count} entries, from {start_count} to {end_count}')
    except Exception as e:
        logger.exception(f'failure during update register. added {end_count-start_count} entries, from {start_count} to {end_count}')
        sys.exit()


if __name__ == '__main__':

    N = None
    if len(sys.argv) > 1:
        if str.isnumeric(sys.argv[1]):
            N = int(sys.argv[1])
            logger.info(f'Only {N} acl_ids will be process')
        else:
            logger.warning(f'given `n` argument is not numeric. All of acl_ids will be process')
    else:
        logger.info(f'No `n` arguments. All of acl ids will be process')
    

    logger.info(f'start getting acl_ids from ACL anthology ...')
    acl_ids = find_all_in_text(ACL_ID_PATTERN, gz_to_text(get_url_content(ACL_ANTHOLOGY_URL)))
    acl_ids = list(acl_ids)[:N] if N else list(acl_ids) # Select only N acl_ids if `n` is given in args

    logger.info(f'start connecting mongodb and retrieve register ...')
    register = get_collection(get_db(connect_mongo(MONGO_URL), MONGO_DB_NAME), MONGO_REGISTER_COLLECTION)
    # TODO : Check if register has same closed entries count than documents count

    logger.info(f'start updating register with new acl_ids ...')
    update_register(register, acl_ids)

    register.database.client.close()
    logger.info(f'all process ended successfuly')

