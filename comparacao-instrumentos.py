
from owlready2 import *
import sqlite3 
import json

nameDB = 'db-MeSH.sqlite3'
conn = sqlite3.connect(nameDB)
cursor = conn.cursor()

#Considera sucesso se o termo procurado é o descritor principal ou termo de entrada
def procuraInMeSH(desc):
    dataset = cursor.execute("""   select count(descritores.iddesc) as cont 
                                        from descritores 
                                        left join termos on descritores.iddesc = termos.iddesc
                                        left join hierarquia on descritores.iddesc = hierarquia.iddesc
                                        where (descritores.namedesc like ?) OR (termos.nameterm like ?) """, (desc, desc, )
                                    ).fetchone()
    return dataset[0]

#Procura de SNOMED para MeSH, os termos correspondentes
def main():
    default_world.set_backend(filename = "db-snomed.sqlite3")
    PYM = get_ontology("http://PYM/").load()
    SNOMEDCT_US = PYM["SNOMEDCT_US"]

    totalitems = 0
    totalcomom = 0
    listaComum = {}
    for concept in SNOMEDCT_US.descendant_concepts(): 
        totalitems += 1
        conceitoSnomed = concept.label.first().lower()
        encontrado = procuraInMeSH(conceitoSnomed)
        print(concept.label)
        if (encontrado > 0):
            listaComum[concept.name] = conceitoSnomed
            totalcomom += 1
            print(conceitoSnomed + " - total de itens lidos: " + str(totalitems) )
    
    listaComum['totalItems'] = totalitems
    listaComum['totalComom'] = totalcomom

    jsonlist = json.dumps(listaComum)
    f = open("comparacao-instrumentos.json","w")
    f.write(jsonlist)
    f.close()

if __name__ == "__main__":
    main()