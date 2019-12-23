import elasticsearch
from MeSHbancoEstrutura import BD


def searchElasticMeSH(termoComum):
    bancoMeSH = BD("db-MeSH.sqlite3")
    with bancoMeSH:
        resposta = bancoMeSH.selecionarIdDescritor_NomeDescritor(termoComum)
        print(resposta)
        iddescritor = str(resposta[0])
        nomeprincipal = str(resposta[1])        








def searchElasticSnomed(termoComum):
    print(termoComum)



if __name__ == "__main__":
    searchElasticMeSH("heart attack")