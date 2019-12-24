import elasticsearch
from MeSHbancoEstrutura import BDMeSH
from elasticBancoEstrutura import BDelastic


def searchElasticMeSH(termoProcurado):
    # Procura os todos os termos relacionados
    bancoMeSH = BDMeSH("db-MeSH.sqlite3")
    with bancoMeSH:
        resposta = bancoMeSH.selecionarIdDescritor_NomeDescritor(termoProcurado) 
        idDescritor = str(resposta[0])
        descritorPrincipal = str(resposta[1])

        resposta = bancoMeSH.selecionarIdsHierarquiaPorIdDescritor(idDescritor)
        termosHierarquicos = set()
        for h in resposta:
                data = bancoMeSH.selecionarTermosPorIdHierarquico(h[0])
                for item in data:
                        termosHierarquicos.add(item)

        resposta = bancoMeSH.selectionarTermosDeEntrada(idDescritor)
        termosEntrada = set()
        for ent in resposta:
                if (ent[0] != descritorPrincipal and ent[0] != termoProcurado):
                        termosEntrada.add(ent[0])
                if (ent[0] in termosHierarquicos):
                        termosHierarquicos.remove(ent[0])
        
        if (termoProcurado in termosHierarquicos):
                termosHierarquicos.remove(termoProcurado)
        if (descritorPrincipal in termosHierarquicos):
                termosHierarquicos.remove(descritorPrincipal)

    # Procura os termos no elastic 
    es = elasticsearch.Elasticsearch()
    es.indices.open("articles")
    quantTermoProcurado = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termoProcurado, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']
    quantDescritorPrincipal = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": descritorPrincipal, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']
    
    print(quantTermoProcurado)
    print(quantDescritorPrincipal)

    bancoElastic = BDelastic("db-elastic.sqlite3")
    with bancoElastic:
        idBancoTermoProcurado = bancoElastic.insereTermoProcurado(termoProcurado, quantTermoProcurado, idDescritor, descritorPrincipal, quantDescritorPrincipal )
        print(idBancoTermoProcurado)



def searchElasticSnomed(termoComum):
    print(termoComum)

if __name__ == "__main__":
    bancoElastic = BDelastic("db-elastic.sqlite3")
    with bancoElastic:
       bancoElastic.criarBancoDeDados()

    searchElasticMeSH("heart attack") 

