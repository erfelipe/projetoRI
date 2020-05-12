import elasticsearch 
import preProcessamentoTextual
import constantes
import os 

def processarPDFparaElastic():
    es = elasticsearch.Elasticsearch()
    es.indices.open("articles")
    arquivos = preProcessamentoTextual.list_PDFs(constantes.PDF_ARTIGOS)

    for arq in arquivos:
        chavehash = preProcessamentoTextual.calc_hash(arq)
        print("verificando...", arq)
        results = es.search(index="articles", body={"query": {"match": {"dcIdentifier": chavehash } }})
        if (results['hits']['total']['value'] <= 0):
            print('Tratando: ' + arq)
            doc = preProcessamentoTextual.extraiPDFpyMuPdf(arq)
            corpo = preProcessamentoTextual.limparTudo(doc[0])
            metadados = (doc[1])
            nomeArq = os.path.basename(arq)
            docJSON  = preProcessamentoTextual.montaDocJSONporTextoUsingPyMuPdf(chavehash, corpo, metadados, nomeArq)
        
            es.index(index="articles", body=docJSON)
            print("Arquivo gravado: " + arq + " | " + chavehash)
        else:
            print(arq + " já gravado com a chave: " + chavehash)

if __name__ == "__main__":
    processarPDFparaElastic()