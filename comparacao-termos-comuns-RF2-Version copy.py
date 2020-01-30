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
    dataSet = SnomedCursor.execute(""" SELECT count(termOriginal) FROM description d WHERE (d.termOriginal LIKE ?) """, (desc, )
                                    ).fetchone() 
    return dataSet[0]

def procuraSnomedFromMeshTerms():
    quantDescritores = MeSHcursor.execute(""" SELECT max(rowid) 
                                              FROM descritores d  """).fetchone() 
    
    # quantTermos = MeSHcursor.execute(""" SELECT max(rowid) 
    #                                      FROM termos t """).fetchone() 

    tIguais = []

    #encontra 100 descritores comuns, de forma aleatoria
    quantDescComuns = 0
    while (quantDescComuns < 101):
        rand = randint(1, quantDescritores[0])
        descritor = MeSHcursor.execute(""" select d.namedesc
                                           from descritores d
                                           where rowid = ? """, (rand,) ).fetchone()
        quantSNOMEDTermoOriginal = procuraInSnomedOriginal(descritor[0])
        if (quantSNOMEDTermoOriginal > 0):
            tIguais.append(descritor[0])
            quantDescComuns += 1
            print(str(quantDescComuns) + " " + descritor[0] + " rowid: " + str(rand))
    
    
    #encontra 100 termos comuns, de forma aleatoria 


    #grava o array em json
    with open('termosComuns.json', 'w') as f:
        json.dump(tIguais, f, ensure_ascii=False, indent=4)


#Procura de MeSH para Snomed, os termos correspondentes
def procuraInSnomedFromMeshTerms(termos):
    registro = 0
    tIguais = set()
    tOriginais = set()
    tTratados = set()

    #percorre o MeSH 
    quant = MeSHcursor.execute(""" SELECT count(d.namedesc) FROM hierarquia h
                                    JOIN descritores d on d.iddesc = h.iddesc
                                    JOIN termos t on t.iddesc = d.iddesc
                                    WHERE idhierarq LIKE ('M01060116%')
                                    GROUP BY d.namedesc """).fetchone()

    MeSHcursor.execute(""" SELECT d.namedesc FROM hierarquia h
                            JOIN descritores d on d.iddesc = h.iddesc
                            JOIN termos t on t.iddesc = d.iddesc
                            WHERE idhierarq LIKE ('M01060116%')
                            GROUP BY d.namedesc """)
    for linha in MeSHcursor:
        registro += 1
        termo = linha[0]

        encontradoTermoTratado = procuraInSnomedTratado(termo)
        encontradoTermoOriginal = procuraInSnomedOriginal(termo)
        if ((encontradoTermoTratado > 0) and (encontradoTermoOriginal > 0)):
            SnomedCursorUpdate.execute("UPDATE description SET correspondenciaMeSH = ?, correspondenciaMeSHoriginal = ? WHERE term like ? and termOriginal like ?", ('S', 'S', termo, termo))
            SnomedConn.commit()
            tIguais.add(termo)
            print("Termos iguais: " + termo)
        else: 
            if (encontradoTermoTratado > 0):
                SnomedCursorUpdate.execute("UPDATE description SET correspondenciaMeSH = ? WHERE term = ?", ('S', termo))
                SnomedConn.commit()
                tTratados.add(termo)
                print("Termo tratado: " + termo)
                
                encontradoTermoOriginal = procuraInSnomedOriginal(termo)
                if (encontradoTermoOriginal > 0):
                    SnomedCursorUpdate.execute("UPDATE description SET correspondenciaMeSHoriginal = ? WHERE termOriginal = ?", ('S', termo))
                    SnomedConn.commit()
                    tOriginais.add(termo)
                    print("Termo original: " + termo)
            else:
                if (not ((encontradoTermoTratado > 0) and (encontradoTermoOriginal > 0)) ):
                    encontradoTermoOriginal = procuraInSnomedOriginal(termo)
                    if (encontradoTermoOriginal > 0):
                        SnomedCursorUpdate.execute("UPDATE description SET correspondenciaMeSHoriginal = ? WHERE termOriginal = ?", ('S', termo))
                        SnomedConn.commit()
                        tOriginais.add(termo)
                        print("Termo original: " + termo)

        print(str(registro) + " de "  + str(quant[0])) 

    with open('comparacao-tIguais.txt', 'w') as f:
        for item in tIguais:
            f.write("%s\n" % item)

    with open('comparacao-tOriginais.txt', 'w') as f:
        for item in tOriginais:
            f.write("%s\n" % item)

    with open('comparacao-tTratados.txt', 'w') as f:
        for item in tTratados:
            f.write("%s\n" % item)


def main():
    procuraSnomedFromMeshTerms()


if __name__ == "__main__":
    main()
