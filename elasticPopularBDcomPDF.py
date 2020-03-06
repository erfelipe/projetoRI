import elasticsearch 
import preProcessamentoTextual
import constantes

es = elasticsearch.Elasticsearch()

es.indices.open("articles")
#es.cluster.health(wait_for_status="yellow")

#results = es.search(index="articles", body={"query": {"match_all": {}}})

#for hit in results['hits']['hits']:
#    print("%(dcDescription)s %(dcLanguage)s: %(dcTitle)s" % hit["_source"])

arquivos = preProcessamentoTextual.list_PDFs(constantes.PDF_ARTIGOS)

for arq in arquivos:
    chavehash = preProcessamentoTextual.calc_hash(arq)
    print("verificando...")
    results = es.search(index="articles", body={"query": {"match": {"dcIdentifier": chavehash } }})
    if (results['hits']['total']['value'] <= 0):
        print('Tratando: ' + arq)
        texto = preProcessamentoTextual.extraiPDF(arq)
        corpo = preProcessamentoTextual.limparTudo(texto[0])
        metadados = (texto[1])
        docJSON  = preProcessamentoTextual.montaDocJSONporTexto(chavehash, corpo, metadados)
    
        es.index(index="articles", body=docJSON)
        print("Arquivo gravado: " + arq + " | " + chavehash)
    else:
        print(arq + " já gravado com a chave: " + chavehash)
