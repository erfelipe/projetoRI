import multiprocessing
from multiprocessing.dummy import Process
import sqlite3 
import constantes
from MeSHbancoEstrutura import BDMeSH

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

def procuraNoSnomedPelaHierarquiaDeTermosMesh(codHierarquico):
    """ Dado um código hierárquico do MeSH, procura no Snomed os termos correspondentes
        a partir de termos do MeSH
    
    Arguments:
        codHierarquico {str} -- Codigo do MeSH que representa uma estrutura hierarquica
    """    
    registro = 0
    termosOriginais = set()
    #tTratados = set()
    vocabulario = set()

    bancoMeSH = BDMeSH(constantes.BD_SQL_MESH)
    with bancoMeSH:
        dataSetTermos = bancoMeSH.selecionarTodosTermos()

    for desc in dataSetTermos:
        vocabulario.add(desc[0])

    quant = len(vocabulario)

    for termo in vocabulario:
        registro += 1
        # encontradoTermoTratado = procuraNoSnomedDescritorTratado(termo)
        encontradoTermoOriginal = procuraNoSnomedDescritorOriginal(termo)
        if (encontradoTermoOriginal > 0):
            termosOriginais.add(termo)
            print("Termo original: " + termo)
        print("Termo: " + str(registro) + " de "  + str(quant)) 
    
    #grava o array dos termos comuns em txt
    with open(constantes.TERMOS_COMUNS_ORIGINAIS, "w") as f:
        for item in termosOriginais:
            f.write("%s\n" % item)

def procuraNoSnomedPelaHierarquiaDeDescritoresMesh(codHierarquico):
    """ Dado um código hierárquico do MeSH, procura no Snomed os termos correspondentes
        a partir de descritores do MeSH
    
    Arguments:
        codHierarquico {str} -- Codigo do MeSH que representa uma estrutura hierarquica
    """    
    registro = 0
    descritoresOriginais = set()
    #tTratados = set()
    vocabulario = set()

    bancoMeSH = BDMeSH(constantes.BD_SQL_MESH)
    with bancoMeSH:
        dataSetDescritores = bancoMeSH.selecionarTodosDescritores()

    for desc in dataSetDescritores:
        vocabulario.add(desc[0])

    quant = len(vocabulario)

    for termo in vocabulario:
        registro += 1
        # encontradoTermoTratado = procuraNoSnomedDescritorTratado(termo)
        encontradoTermoOriginal = procuraNoSnomedDescritorOriginal(termo)
        if (encontradoTermoOriginal > 0):
            descritoresOriginais.add(termo)
            print("Descritor original: " + termo)
        print("Descritor: " + str(registro) + " de "  + str(quant)) 
    
    #grava o array dos termos comuns em txt
    with open(constantes.DESCRITORES_COMUNS_ORIGINAIS, "w") as f:
        for item in descritoresOriginais:
            f.write("%s\n" % item)

if __name__ == "__main__":
    # a categoria Diseases [C] - https://meshb.nlm.nih.gov/treeView 

    proc1 = multiprocessing.Process(target= procuraNoSnomedPelaHierarquiaDeDescritoresMesh, args=('C',))
    proc2 = multiprocessing.Process(target= procuraNoSnomedPelaHierarquiaDeTermosMesh, args=('C',)) 

    proc1.start()
    proc2.start()

    proc1.join()
    proc2.join()
