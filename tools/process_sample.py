import re
import json
import logging

import spacy
from tqdm import tqdm

from client import *
from helpers import write_jsonl

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('processSample')

#faire une version/adapter pour lire les ids ACL aussi ?
def get_grobides(ids:list):
    """
        Pour une liste d'identifiants semantic scholar passée en paramètre, renvoie les éléments 'grobid' correspondants

        Paramètres
        ----------
        ids : une liste d'identifiants semantic scholar
    """
    if type(ids) is list:
        docs = get_collection(get_db(connect_mongo(), MONGO_DB_NAME), MONGO_DOCUMENTS_COLLECTION)
        return [doc['grobid'] for doc in docs.find({'s2.paperId':{'$in':ids}})]

    logger.error(f'function needs a list {type(ids)}') #si l'élément passé en paramètre n'est pas une liste, renvoie une erreur
    exit(1) # TODO : Maybe raise ?

def process_doc(doc:str, tokenizer):
    """
        Pour un élément grobid passé en paramètre, renvoie les spans des sections, des phrases et les mots contenus dans le document

        Paramètres
        ----------
        doc : une liste de documents au format XML générés par Grobid
    """
    sections = re.findall('(?:ead.*?</head>)(.*?)(?:<h)',doc,re.DOTALL)
    sections.insert(0,'<s>{}</s>{}'.format(re.search('(?:<title .*?>)(.*?)(?:</title>)',doc).group(1), re.search('(?:abstract>)(.*)(?:</abstract)',doc,re.DOTALL).group(1))) #abstract+titre
    len_section = 0; len_sentence = 0 #compteurs de tokens
    len_sections = []; len_sentences = []; words = [] #tableau span sections, tableau span phrases, tableau mots
    for section in sections: #pour chaque section
        section=re.sub('<ref type="bibr.*?</ref>','',section) #retire les références bibliographiques
        section=re.sub('<ref.*?</ref>','[reference]',section) #remplace les références non bibliographiques
        sentences = re.findall('(?:<s>)(.*?)(?:</s>)',section) #récupère les phrases de la section
        len_section = len_sentence #enregistre le span de la 1ère phrase de la section 
        for sentence in sentences:
            sentence = [token.orth_ for token in tokenizer(sentence)] #tokenise la phrase, récupère le résultat avec chaque token en str
            for word in sentence:
                words.append(word)
            len_sentences.append([len_sentence,len_sentence+len(sentence)]) #ajoute le span de la phrase
            len_sentence+=len(sentence) #incrémente le compteur
        len_sections.append([len_section,len_sentence]) #ajoute les spans des phrases de la section
    return({'sections':len_sections,'sentences':len_sentences,'words':words})

def process_docs(docs:list):
    nlp = spacy.load('en_core_web_sm') #mettre dans une fonction
    results = []
    for doc in tqdm(docs): #pour chaque document grobid
        results.append(process_doc(doc, nlp.tokenizer)) #fait la tokenisation, etc
    return results

if __name__ == '__main__':
    
    import sys
    from dotenv import load_dotenv

    if len(sys.argv) != 3: #vérifie que 2 arguments ont été fournis
        logger.error('two (2) arguments (input path and output path) needed')
        exit (1)

    if sys.argv[2][-6:]!='.jsonl': #vérifie que le chemin du fichier sortie par .jsonl
        logger.error('path provided for output does not end in .jsonl')
        exit(1)

    try:
        open(sys.argv[1],'r') #teste si le fichier entrée existe déjà
    except IOError:
        logger.error('invalid input path')
        exit(1)

    try:
        open(sys.argv[2],'r') #teste si le fichier sortie existe déjà
    except IOError:
        try:
            open(sys.argv[2], 'w') #teste si le fichier sortie peut être créé
        except IOError:
            logger.error('invalid output path')
            exit(1)

    load_dotenv() #charge le .env pour la connexion à la BDD
 

    with open(sys.argv[1]) as f:
        ids = json.load(f)

    docs = get_grobides(ids) #récupère les documents grobid
    logger.info(f'successfully retrieved {len(docs)}/{len(ids)} documents from the database')
    write_jsonl(sys.argv[2], process_docs(docs)) #sauvegarde les résultats