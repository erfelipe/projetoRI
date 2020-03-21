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
    dataSet = SnomedCursor.execute(""" SELECT count(termTratado) FROM description d WHERE (d.termTratado LIKE ?) """, (desc, )
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
    termosTratados = set()
    vocabularioOriginal = set()
    vocabularioTratado = set()

    bancoMeSH = BDMeSH(constantes.BD_SQL_MESH)
    with bancoMeSH:
        dataSetNomesTermos = bancoMeSH.selecionarNomesTermosPorIdHierarquia(codHierarquico)

    for tn, tt in dataSetNomesTermos:
        vocabularioOriginal.add(tn)
        vocabularioTratado.add(tt)

    quant = len(vocabularioOriginal)

    for termo in vocabularioOriginal:
        registro += 1
        encontradoTermoOriginal = procuraNoSnomedDescritorOriginal(termo)
        if (encontradoTermoOriginal > 0):
            termosOriginais.add(termo)
            print("Termo original: " + termo)
        print("Termo: " + str(registro) + " de "  + str(quant)) 
    
    registro = 0
    quant = len(vocabularioTratado)

    for termo in vocabularioTratado:
        registro += 1
        encontradoTermoTratado = procuraNoSnomedDescritorTratado(termo)
        if (encontradoTermoTratado > 0):
            termosTratados.add(termo)
            print("Termo tratado: " + termo)
        print("Termo: " + str(registro) + " de " + str(quant))

    #grava o array dos termos comuns em txt
    with open(constantes.MESH_TERMOS_COMUNS_ORIGINAIS, "w") as f:
        for item in termosOriginais:
            f.write("%s\n" % item)

    #grava o array dos termos tratados em txt
    with open(constantes.MESH_TERMOS_COMUNS_TRATADOS, "w") as f:
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
    descritoresTratados = set()
    vocabularioOriginal = set()
    vocabularioTratado = set()

    bancoMeSH = BDMeSH(constantes.BD_SQL_MESH)
    with bancoMeSH:
        dataSetNomesDescritores = bancoMeSH.selecionarNomesDescritoresPorIdHierarquia(codHierarquico)

    for dn, dt in dataSetNomesDescritores:
        with bancoMeSH:
            vocabularioOriginal.add(dn)
            vocabularioTratado.add(dt) 

    quant = len(vocabularioOriginal)

    #localiza e seleciona os termos comuns sem tratamento
    for termo in vocabularioOriginal:
        registro += 1
        encontradoTermoOriginal = procuraNoSnomedDescritorOriginal(termo)
        if (encontradoTermoOriginal > 0):
            descritoresOriginais.add(termo)
            print("Descritor original: " + termo)
        print("Descritor: " + str(registro) + " de "  + str(quant)) 
    
    registro = 0
    quant = len(vocabularioTratado)

    #localiza e seleciona os termos comuns com tratamento
    for termo in vocabularioTratado:
        registro += 1
        encontradoTermoTratado = procuraNoSnomedDescritorTratado(termo)
        if (encontradoTermoTratado > 0):
            descritoresTratados.add(termo)
            print("Descritor tratado: " + termo)
        print("Descritor tratado: " + str(registro) + " de "  + str(quant)) 

    #grava o array dos termos comuns em txt
    with open(constantes.MESH_DESCRITORES_COMUNS_ORIGINAIS, "w") as f:
        for item in descritoresOriginais:
            f.write("%s\n" % item)

    #grava o array dos termos comuns em txt
    with open(constantes.MESH_DESCRITORES_COMUNS_TRATADOS, "w") as f:
        for item in descritoresTratados:
            f.write("%s\n" % item)

if __name__ == "__main__":
    # a categoria Diseases [C] - https://meshb.nlm.nih.gov/treeView 

    procuraNoSnomedPelaHierarquiaDeDescritoresMesh('C')

    # proc1 = multiprocessing.Process(target= procuraNoSnomedPelaHierarquiaDeDescritoresMesh, args=('C',))
    # proc2 = multiprocessing.Process(target= procuraNoSnomedPelaHierarquiaDeTermosMesh, args=('C',)) 

    # proc1.start()
    # proc2.start()

    # proc1.join()
    # proc2.join()
