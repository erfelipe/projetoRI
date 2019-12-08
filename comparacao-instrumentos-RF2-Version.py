
from owlready2 import *
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

    #percorre o SNOMED (RF2) 
    SnomedCursor.execute("SELECT term, termOriginal FROM description")
    for linha in SnomedCursor:
        termoSnomed = linha[0]
        termoSnomedOriginal = linha[1]
        encontradoTermo = procuraInMeSH(termoSnomed)
        encontradoTermoOriginal = procuraInMeSH(termoSnomedOriginal) 
        if (encontradoTermo > 0):
            SnomedCursorUpdate.execute("UPDATE description SET correspondenciaMeSH = ? WHERE term = ?", ('S', termoSnomed))
            print("Termo tratado: " + termoSnomed)
        if (encontradoTermoOriginal > 0):
            SnomedCursorUpdate.execute("UPDATE description SET correspondenciaMeSHoriginal = ? WHERE termOriginal = ?", ('S', termoSnomedOriginal))
            print("Termo original: " + termoSnomedOriginal)

if __name__ == "__main__":
    main()