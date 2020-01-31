import sqlite3 
import constantes 
from random import randint
import json

#inicializa os bancos
MeSHconn = sqlite3.connect(constantes.BD_SQL_MESH)
MeSHcursor = MeSHconn.cursor()

SnomedConn = sqlite3.connect(constantes.BD_SQL_SNOMED) 
SnomedCursor = SnomedConn.cursor()
SnomedCursorUpdate = SnomedConn.cursor()

#Considera sucesso se o termo procurado eh o descritor principal ou termo de entrada
#deprecated
def procuraInMeSH(desc):
    dataset = MeSHcursor.execute("""   select count(descritores.iddesc) as cont 
                                        from descritores 
                                        left join termos on descritores.iddesc = termos.iddesc
                                        left join hierarquia on descritores.iddesc = hierarquia.iddesc
                                        where (descritores.namedesc like ?) OR (termos.nameterm like ?) """, (desc, desc, )
                                    ).fetchone()
    return dataset[0]

def procuraInSnomedTratado(desc):
    dataSet = SnomedCursor.execute(""" SELECT count(term) FROM description d WHERE (d.term LIKE ?) """, (desc, )
                                    ).fetchone() 
    return dataSet[0]

def procuraInSnomedOriginal(desc):
    """Recebe um texto descritor e verifica se estah presente no SNOMED
    
    Arguments:
        desc {str} -- Texto descritor
    
    Returns:
        int -- Quantidade de ocorrencias do texto no campo - termOriginal
    """    
    dataSet = SnomedCursor.execute(""" SELECT count(termOriginal) FROM description d WHERE (d.termOriginal LIKE ?) """, (desc, )
                                    ).fetchone() 
    return dataSet[0]

def procuraSnomedFromMeshTerms():
    """A partir de modo randomico, sao escolhidos os termos comuns entre MeSH e SNOMED
    
    Arguments:
        none
    
    Returns:
        [none] -- []
    """
    quantDescritores = MeSHcursor.execute(""" SELECT max(rowid) 
                                              FROM descritores d  """).fetchone() 
    
    quantTermos = MeSHcursor.execute(""" SELECT max(rowid) 
                                         FROM termos t """).fetchone() 

    tIguais = []

    #encontra 100 descritores comuns, de forma aleatoria
    quantDescComuns = 0
    while (quantDescComuns < 100):
        rand = randint(1, quantDescritores[0])
        descritor = MeSHcursor.execute(""" select d.namedesc
                                           from descritores d
                                           where rowid = ? """, (rand,) ).fetchone()
        quantSNOMEDTermoOriginal = procuraInSnomedOriginal(descritor[0])
        if (quantSNOMEDTermoOriginal > 0):
            if (descritor[0] not in tIguais):
                tIguais.append(descritor[0])
                quantDescComuns += 1
                print(str(quantDescComuns) + " " + descritor[0] + " rowid: " + str(rand)) 

    #encontra 100 termos comuns, de forma aleatoria 
    quantDescComuns = 0
    while (quantDescComuns < 100):
        rand = randint(1, quantTermos[0]) 
        descritor = MeSHcursor.execute(""" select t.nameterm 
                                           from termos t 
                                           where t.rowid = ? """, (rand,) ).fetchone() 
        quantSNOMEDTermoOriginal = procuraInSnomedOriginal(descritor[0]) 
        if (quantSNOMEDTermoOriginal > 0): 
            if (descritor[0] not in tIguais): 
                tIguais.append(descritor[0]) 
                quantDescComuns += 1
                print(str(quantDescComuns) + " " + descritor[0] + " rowid: " + str(rand)) 

    #grava o array dos termos comuns em json
    with open(constantes.TERMOS_COMUNS_JSON, 'w') as f:
        json.dump(tIguais, f, ensure_ascii=False, indent=4)


def main():
    procuraSnomedFromMeshTerms()


if __name__ == "__main__":
    main()
