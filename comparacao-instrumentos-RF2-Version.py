
import sqlite3 
import pickle

#inicializa os bancos
MeSHDB = 'db-MeSH.sqlite3'
MeSHconn = sqlite3.connect(MeSHDB)
MeSHcursor = MeSHconn.cursor()

SnomedDB = 'db-snomed-RF2.sqlite3' 
SnomedConn = sqlite3.connect(SnomedDB) 
SnomedCursor = SnomedConn.cursor()
SnomedCursorUpdate = SnomedConn.cursor()

#Considera sucesso se o termo procurado é o descritor principal ou termo de entrada
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
    print("resolver a leitura de quais termos MeSH primeiro")


if __name__ == "__main__":
    main()
