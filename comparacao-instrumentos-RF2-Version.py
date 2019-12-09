
import sqlite3 
import json

#inicializa os bancos
MeSHDB = 'db-MeSH.sqlite3'
MeSHconn = sqlite3.connect(MeSHDB)
MeSHcursor = MeSHconn.cursor()

SnomedDB = 'db-snomed-RF2.sqlite3' 
SnomedConn = sqlite3.connect(SnomedDB) 
SnomedCursor = SnomedConn.cursor()
SnomedCursorUpdate = SnomedConn.cursor()

#Considera sucesso se o termo procurado é o descritor principal ou termo de entrada
def procuraInMeSH(desc):
    dataset = MeSHcursor.execute("""   select count(descritores.iddesc) as cont 
                                        from descritores 
                                        left join termos on descritores.iddesc = termos.iddesc
                                        left join hierarquia on descritores.iddesc = hierarquia.iddesc
                                        where (descritores.namedesc like ?) OR (termos.nameterm like ?) """, (desc, desc, )
                                    ).fetchone()
    return dataset[0]

#Procura de SNOMED para MeSH, os termos correspondentes
def main():
    registro = 0
    tIguais = {}
    tOriginais = {}
    tTratados = {}

    #percorre o SNOMED (RF2) 
    quant = SnomedCursor.execute("SELECT count(term) FROM description").fetchone()

    SnomedCursor.execute("SELECT term, termOriginal FROM description")
    for linha in SnomedCursor:
        registro += 1
        termoSnomed = linha[0]
        termoSnomedOriginal = linha[1]
        termosIguais = termoSnomed == termoSnomedOriginal

        encontradoTermo = procuraInMeSH(termoSnomed)
        if ((encontradoTermo > 0) and (termosIguais)):
            SnomedCursorUpdate.execute("UPDATE description SET correspondenciaMeSH = ?, correspondenciaMeSHoriginal = ? WHERE term like ? and termOriginal like ?", ('S', 'S', termoSnomed, termoSnomedOriginal))
            SnomedConn.commit()
            print("Termos iguais: " + termoSnomed)
        else: 
            if (encontradoTermo > 0):
                SnomedCursorUpdate.execute("UPDATE description SET correspondenciaMeSH = ? WHERE term = ?", ('S', termoSnomed))
                SnomedConn.commit()
                print("Termo tratado: " + termoSnomed)
                encontradoTermoOriginal = procuraInMeSH(termoSnomedOriginal)
                if (encontradoTermoOriginal > 0):
                    SnomedCursorUpdate.execute("UPDATE description SET correspondenciaMeSHoriginal = ? WHERE termOriginal = ?", ('S', termoSnomedOriginal))
                    SnomedConn.commit()
                    print("Termo original: " + termoSnomedOriginal)
            else:
                if (not termosIguais):
                    encontradoTermoOriginal = procuraInMeSH(termoSnomedOriginal)
                    if (encontradoTermoOriginal > 0):
                        SnomedCursorUpdate.execute("UPDATE description SET correspondenciaMeSHoriginal = ? WHERE termOriginal = ?", ('S', termoSnomedOriginal))
                        SnomedConn.commit()
                        print("Termo original: " + termoSnomedOriginal)

        print(str(registro) + " de "  + str(quant[0])) 

if __name__ == "__main__":
    main()