from MeSHbancoEstrutura import BD
import elasticsearch 
import json

bancodedados = BD("db-MeSH.sqlite3")
es = elasticsearch.Elasticsearch()
es.indices.open("articles")
compMeSH = {}
itensTotais = 0
itensDescritores = 0
itensTermosEntrada = 0

with bancodedados:
    descs = bancodedados.selecionarTodosDescritores()
    for d in descs:
        itensTotais += 1
        descritor = d[0]
        #result = es.search(index="articles", body={"track_total_hits": True, "query": {"match_phrase" : {"dcDescription" : descritor}}})
        result = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": descritor, "type": "phrase_prefix", "fields": [ "dcTitle", "dcDescription" ]}}})
        if (result['hits']['total']['value'] > 0):
            itensDescritores += 1
            compMeSH[descritor] = result['hits']['total']['value']
            print(descritor)
            print(result['hits']['total']['value'])
            print("---------")
    
    descs = bancodedados.selecionarTodosTermos()
    for d in descs:
        itensTotais += 1
        termo = d[0]
        result = es.search(index="articles", body={"track_total_hits": True, "query": {"match_phrase" : {"dcDescription" : termo}}})
        if (result['hits']['total']['value'] > 0):
            itensTermosEntrada += 1
            compMeSH[termo] = result['hits']['total']['value']
            print(termo)
            print(result['hits']['total']['value'])
            print("---------")

    compMeSH['itensTotais'] = itensTotais
    compMeSH['itensDescritores'] = itensDescritores
    compMeSH['itensTermosEntrada'] = itensTermosEntrada
    
    jsonlist = json.dumps(compMeSH)
    f = open("comparacao-MeSHelastic.json","w")
    f.write(jsonlist)
    f.close()
