import elasticsearch 
import tratamentoPDF

es = elasticsearch.Elasticsearch()

es.indices.open("articles")
#es.cluster.health(wait_for_status="yellow")

#results = es.search(index="articles", body={"query": {"match_all": {}}})

#for hit in results['hits']['hits']:
#    print("%(dcDescription)s %(dcLanguage)s: %(dcTitle)s" % hit["_source"])

arquivos = tratamentoPDF.list_PDFs("/Volumes/SD-64-Interno/artigosPDFbmc")

for arq in arquivos:
    chavehash = tratamentoPDF.calc_hash(arq)
    print("verificando...")
    results = es.search(index="articles", body={"query": {"match": {"dcIdentifier": chavehash } }})
    if (results['hits']['total']['value'] <= 0):
        print('Tratando: ' + arq)
        texto = tratamentoPDF.extraiPDF(arq)
        corpo = tratamentoPDF.limparTudo(texto[0])
        metadados = (texto[1])
        docJSON  = tratamentoPDF.montaDocJSONporTexto(chavehash, corpo, metadados)
    
        es.index(index="articles", body=docJSON)
        print("Arquivo gravado: " + arq + " | " + chavehash)
    else:
        print(arq + " já gravado com a chave: " + chavehash)
