import sqlite3 
import constantes
import json

#inicializa os bancos
MeSHconn = sqlite3.connect(constantes.BD_SQL_MESH)
MeSHcursor = MeSHconn.cursor()

SnomedConn = sqlite3.connect(constantes.BD_SQL_SNOMED) 
SnomedCursor = SnomedConn.cursor()
SnomedCursorUpdate = SnomedConn.cursor()

def procuraNoMeSH(desc):
    """ Considera sucesso se o termo procurado é o descritor principal ou termo de entrada

        deprecated
    
    Arguments:
        desc {str} -- termo / descritor procurado
    
    Returns:
        int -- quantidade encontrada
    """    
    dataset = MeSHcursor.execute("""   select count(descritores.iddesc) as cont 
                                        from descritores 
                                        left join termos on descritores.iddesc = termos.iddesc
                                        left join hierarquia on descritores.iddesc = hierarquia.iddesc
                                        where (descritores.namedesc like ?) OR (termos.nameterm like ?) """, (desc, desc, )
                                    ).fetchone()
    return dataset[0]

def procuraNoSnomedDescritorTratado(desc):
    """ Verifica se há correspondencia exata do termo / descritor procurado no SNOMED em sua forma TRATADA 
    
    Arguments:
        desc {str} -- termo / descritor do MeSH
    
    Returns:
        int -- quantidade maior que zero se encontrado
    """    
    dataSet = SnomedCursor.execute(""" SELECT count(term) FROM description d WHERE (d.term LIKE ?) """, (desc, )
                                    ).fetchone() 
    return dataSet[0]

def procuraNoSnomedDescritorOriginal(desc):
    """ Verifica se há correspondencia exata do termo / descritor procurado no SNOMED em sua forma ORIGINAL
    
    Arguments:
        desc {str} -- termo / descritor do MeSH
    
    Returns:
        int -- quantidade maior que zero se encontrado
    """    
    dataSet = SnomedCursor.execute(""" SELECT count(termOriginal) FROM description d WHERE (d.termOriginal LIKE ?) """, (desc, )
                                    ).fetchone() 
    return dataSet[0]

def procuraNoSnomedDeHieraquiaMesh(codHierarquico):
    """ Dado um código hierárquico do MeSH, procura no Snomed os termos correspondentes
    
    Arguments:
        codHierarquico {str} -- Codigo do MeSH que representa uma estrutura hierarquica
    """    
    registro = 0
    tOriginais = set()
    #tTratados = set()
    vocabulario = set()
    
    dataSet = MeSHcursor.execute(""" SELECT d.namedesc, t.nameterm FROM hierarquia h
                                    JOIN descritores d on d.iddesc = h.iddesc
                                    JOIN termos t on t.iddesc = d.iddesc
                                    WHERE idhierarq LIKE (?)
                                    GROUP BY d.namedesc """, (codHierarquico + '%', )).fetchall()
    
    for desc, term in dataSet:
        vocabulario.add(desc)
        vocabulario.add(term)

    quant = len(vocabulario)

    for termo in vocabulario:
        registro += 1
        # encontradoTermoTratado = procuraNoSnomedDescritorTratado(termo)
        encontradoTermoOriginal = procuraNoSnomedDescritorOriginal(termo)
        if (encontradoTermoOriginal > 0):
            tOriginais.add(termo)
            print("Termo original: " + termo)

        print(str(registro) + " de "  + str(quant)) 

    #grava o array dos termos comuns em json
    with open(constantes.TERMOS_COMUNS_JSON, 'w') as f:
        json.dump(tOriginais, f, ensure_ascii=False, indent=4)


if __name__ == "__main__":
    procuraNoSnomedDeHieraquiaMesh('D')
