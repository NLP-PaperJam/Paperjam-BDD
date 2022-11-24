import logging

from tqdm import tqdm

from helpers import read_jsonl

logging.config.fileConfig('logging.conf')
logger = logging.getLogger('processSample')

def build_tsv(json_doc):
    """
        Creates the elements of the .tsv file

        Parameters
        ----------
        json_doc : json document to turn into .tsv
    """

    sentences = json_doc['sentences']
    words = json_doc['words']
    named_entities = json_doc['ner']
    corefs = json_doc['coref']
    relations = json_doc['n_ary_relations']

    len_words = -1 #compteur pour le nombre de caractères dans chaque token
    c_entities = 1

    d_coref = {i:1 for i in corefs.keys()} #dict avec compteurs pour occurences de chaque coréférence pour les IDs dans le tsv
    tableau = [json_doc['doc_id']] #sert à stocker chaque ligne du futur fichier
    d_pos = {} #sert à stocker la position du premier élément de chaque groupe (cluster) de coréférences

    named_entities.sort()

    for v in corefs.values():
        v.sort()

    for c,e in enumerate(sentences): #pour chaque phrase
        #print('#Text={}'.format(' '.join(words[e[0]:e[1]]))) #reconstitue la phrase
        tableau.append('#Text={}'.format(' '.join(words[e[0]:e[1]]))) #reconstitue la phrase
        i_start = e[0] #conserve la position (en nombre de tokens) du premier token de la phrase pour l'utiliser pour calculer la position du token dans la phrase

        for i in range(*e): #déballe le contenu de e (un tableau avec le span de la phrase du genre [0,7]) et applique range dessus; boucle se fait donc pour chaque token de la phrase
            entity='_' #initialise entity à _ (valeur si le token ne correspond pas à une entité)

            if named_entities and i in range(named_entities[0][0],named_entities[0][1]): #si la valeur de i est comprise entre les bornes du le 1er élément de la liste d'entités
                entity=named_entities[0][2] #récupère le nom de l'entité

                if (named_entities[0][0]-named_entities[0][1])<-1: #si l'entité est un groupe de tokens (c-à-d qu'elle a un span < à -1)
                    entity=f'{entity}[{c_entities}]' #ent : nom de l'entité, c_entities : ID WebAnno

                if i+1==named_entities[0][1]: #si la prochaine valeur de i est égale à la borne supérieure, c-à-d qu'on se trouve à la fin du/des éléments à marquer
                    named_entities.pop(0) #retire l'élément de la liste. note : retire aussi les éléments de jsonl[0]['ner'] (normal)
                    c_entities+=1 #fin du/des éléments à marquer : incrémente le compteur d'ID WebAnno

            coref='_\t_' #initialise coref à _ (valeur si le token n'a pas de chaîne)
            for c_c,v in enumerate(corefs.items()): #pour chaque liste de tableaux de coréférences
                if v[1] and i in range(*v[1][0]): #vérifie que tableau de spans des corefs n'est pas vide, puis génère une étendue sur le 1er tableau de la liste
                    coref = f'*->{c_c+1}-{d_coref[v[0]]}\t[{c+1}]'

                    if i+1==v[1][0][1]: #si la prochaine valeur de i est égale à la borne supérieure
                        if d_coref[v[0]]==1:
                            d_pos[v[0]]=f'{c+1}-{(i+1-i_start)}'

                        v[1].pop(0)
                        d_coref[v[0]]+=1
                    break #sort de la boucle puisque ne peut pas appartenir à plusieurs chaînes de coréférences
                
        #     print('{}-{}\t{}-{}\t{}\t{}\t{}\t{}'.format(c+1,(i+1-i_start),len_words+1,lenl[_words+len(f'{words[i]} '),words[i],entity,coref,relation))
            tableau.append({'p':f'{c+1}-{(i+1-i_start)}','l1':len_words+1,'l2':len_words+len(f'{words[i]} '),'mot':words[i],'entite':entity,'corf':coref}) #ajoute token en dict dans tab
            len_words+=len(f'{words[i]} ') #incrémente le nombre de caractères total avec la longueur du token
        # print('\r')
        # tableau.append('\r') #entre chaque phrase ajoute un retour à la ligne dans le tableau

    w=[] #crée liste de listes avec noms de chaque éléments dans relations, moins score
    for r in relations:
        u=[]
        for x in r.items():
            if x[0]!='score':
                u.append(x[1])
        w.append(u)

    resW=[]
    for o in w:
        for idx, a in enumerate(o): #crée toutes les paires possibles
            for b in o[idx + 1:]:
                if [a,b] not in resW:
                    resW.append([a, b])

    for line in tableau: #pour chaque élément du tableau
        if type(line) is not str: #si la ligne tu tableau n'est pas une chaîne
            line['relation']='_' #initialise relation à _ (valeur si le token n'a pas de relation)
            if line['p'] in d_pos.values():
                for v in d_pos.items():
                    if line['p']==v[1]:
                        for r in resW:
                            if r[0]==v[0] and r[1] in d_pos.keys():
                                if line['relation']=='_' : #pas de relation existante pour la ligne :
                                    line['relation']=f'{d_pos[r[1]]}' #crée l'élément
                                else:
                                    line['relation']=f'{line["relation"]}|{d_pos[r[1]]}' #concatène la nouvelle relation à l'existante
    
    return(tableau)

def make_tsv(tableau:list, chemin:str):
    """
        Creates a .tsv file based off the element of the list 

        Parameters
        ----------
        tableau : list of elements to create the .tsv file
        chemin : string of the path of the folder where the files will be created
    """
    
    with open(f'{chemin}{tableau[0]}.tsv', 'w') as f:
        f.write('#FORMAT=WebAnno TSV 3.3\r')
        f.write('#T_SP=webanno.custom.Test|value\r')
        f.write('#T_CH=webanno.custom.TestCorefLink|referenceRelation|referenceType\r')
        f.write('#T_RL=webanno.custom.TestRelation|BT_webanno.custom.Test\r')
        for line in tableau[1:]:
            if type(line) is str:
                f.write(f'\r\r{line}')
            else:
                f.write('\r{}\t{}-{}\t{}\t{}\t{}\t{}'.format(line['p'],line['l1'],line['l2'],line['mot'],line['entite'],line['corf'],line['relation']))

def build_tsvs(jsonls:list, chemin):
    """
        Creates then writes the elements of the .tsv file for every element in the list

        Parameters
        ----------
        jsonls : list of json files
        chemin : path of the folder where the files will be created
    """
    for json_doc in jsonls:
        tab = build_tsv(json_doc)
        make_tsv(tab, chemin)

if __name__ == '__main__':
    
    import sys

    if len(sys.argv) != 3: #vérifie que 2 arguments ont été fournis
        logger.error('two (2) arguments (path to input file and output path) needed')
        exit (1)

    if sys.argv[1][-6:]!='.jsonl': #vérifie que le chemin du fichier entrée se finit par .jsonl
        logger.error('path provided for input does not end in .jsonl')
        exit(1)

    try:
        open(sys.argv[1],'r') #teste si le fichier entrée existe
    except IOError:
        logger.error('invalid input path')
        exit(1)

    #ajouter vérification que chemin de sortie est valide + existe
 

    jsonls = read_jsonl(sys.argv[1])
    build_tsvs(jsonls,sys.argv[2])