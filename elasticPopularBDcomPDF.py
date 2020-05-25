import elasticsearch 
import preProcessamentoTextual
import constantes
import os 
import logging

logging.basicConfig(level=logging.DEBUG, filename=constantes.LOG_PREPROCESSAMENTOTEXTUAL, filemode='w', format='%(process)d - %(name)s - %(levelname)s - %(message)s')

def processarPDFparaElastic():
    """Popula o banco de dados do Elasticsearch com os textos e metadados
        extraidos dos arquivos PDF
    """    
    es = elasticsearch.Elasticsearch()
    es.indices.open("articles")
    arquivos = preProcessamentoTextual.list_PDFs(constantes.PDF_ARTIGOS)

    for arq in arquivos:
        chavehash = preProcessamentoTextual.calc_hash(arq)
        logging.info('Verificando... ' + arq) 
        results = es.search(index="articles", body={"query": {"match": {"dcIdentifier": chavehash } }})
        if (results['hits']['total']['value'] <= 0):
            logging.info('Tratando: ' + arq) 
            doc = preProcessamentoTextual.extraiPDFpyMuPdf(arq)
            corpo = preProcessamentoTextual.limparTudo(doc[0])
            metadados = (doc[1])
            nomeArq = os.path.basename(arq)
            docJSON  = preProcessamentoTextual.montaDocJSONporTextoUsingPyMuPdf(chavehash, corpo, metadados, nomeArq)
        
            es.index(index="articles", body=docJSON)
            logging.info("Arquivo gravado: " + arq + " | " + chavehash)
        else:
            logging.info(arq + " já gravado com a chave: " + chavehash)

if __name__ == "__main__":
    processarPDFparaElastic()