import multiprocessing 
import elasticsearch 
from MeSHbancoEstrutura import BDMeSH 
from snomedRF2bancoEstrutura import BDSnomed 
from elasticBancoEstrutura import BDelastic 
import constantes 
import logging 
import itertools
import json 

logging.basicConfig(level=logging.DEBUG)

def searchElasticMeSH(termoProcurado, tipoTermo, idioma):
	"""Realiza a pesquisa com todas as caracteristicas do instrumento terminologico MeSH
	   considerando termos hierarquicos (este instrumento nao possui termos relacionados)
	
	Arguments:
		termoProcurado {str} -- Termo comum selecionado previamente em uma lista comum OU por entrada de linguagem natural (nova versao) 
		tipoTermo {str} -- O = original e T = tratado 
		idioma {str} -- 'en' = ingles

	Returns:
		dict -- Um dicionario com uma lista de conjunto complexo da pesquisa de descritores e termos do MeSH 
	"""
	bancoMeSH = BDMeSH(constantes.BD_SQL_MESH)
	with bancoMeSH:
		descritoresHierarquicos = bancoMeSH.selecionarDescritoresHierarquicos(termoProcurado, tipoTermo, idioma)
		termosEntradaHierarquicos = bancoMeSH.selecionarTermosDeEntradaHierarquicos(termoProcurado, tipoTermo, idioma)

	es = elasticsearch.Elasticsearch()
	es.indices.open("articles")
	itensEncontrados = []
	for desc in descritoresHierarquicos:
		resultSet = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": desc, "type": "phrase", "fields": [ "dcTitle", "textBody" ]}}})['hits']
		for item in resultSet['hits']:
			itensEncontrados.append([desc, item["_source"]["dcIdentifier"], item["_source"]["dcSource"], item["_source"]["dcTitle"] ])

	for termo in termosEntradaHierarquicos:
		resultSet = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termo, "type": "phrase", "fields": [ "dcTitle", "textBody" ]}}})['hits']
		for item in resultSet['hits']:
			itensEncontrados.append([termo, item["_source"]["dcIdentifier"], item["_source"]["dcSource"], item["_source"]["dcTitle"] ])
	resposta = {}
	resposta["MeSH"] = itensEncontrados

	with open('MeSH.json', 'w') as f:
		json.dump(resposta, f, indent=4)

	return resposta
	
def searchElasticSnomed(termoProcurado, tipoTermo, idioma):
	"""Realiza a pesquisa com todas as caracteristicas do instrumento terminologico SNOMED CT
	   considerando termos hierarquicos e relacionados (este instrumento possui o conceito de termos relacionados)
	
	Arguments:
		termoProcurado {str} -- Termo comum selecionado previamente em uma lista comum
		tipoTermo {str} -- O = original e T = tratado 
		idioma {str} -- en = ingles e es = espanhol 
	"""
	bancoSNOMED = BDSnomed(constantes.BD_SQL_SNOMED) 
	with bancoSNOMED:
		iDPrincipal = bancoSNOMED.selecionarIdPrincipalDoTermo(termoProcurado)
		if iDPrincipal:
			termosHierarquicos = bancoSNOMED.selecionarTermosHierarquicos(iDPrincipal, 'O', idioma)
			termosProximosConceitualmente =bancoSNOMED.selecionarTermosProximosConceitualmente(iDPrincipal, 'O', idioma)
		else:
			logging.error("SNOMED - idPrincipal: %s nao identificado (!)", str(iDPrincipal), exc_info=True) 
			termosProximosConceitualmente = []
			termosHierarquicos = [] 
		
		es = elasticsearch.Elasticsearch() 
		es.indices.open("articles") 
		itensEncontrados = [] 
		for termoHierq in termosHierarquicos:
			resultSet = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termoHierq, "type": "phrase", "fields": [ "dcTitle", "textBody" ]}}})['hits']
			for item in resultSet['hits']:
				itensEncontrados.append([termoHierq, item["_source"]["dcIdentifier"], item["_source"]["dcSource"], item["_source"]["dcTitle"] ])

		for termopConceitual in termosProximosConceitualmente:
			resultSet = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termopConceitual, "type": "phrase", "fields": [ "dcTitle", "textBody" ]}}})['hits']
			for item in resultSet['hits']:
				itensEncontrados.append([termopConceitual, item["_source"]["dcIdentifier"], item["_source"]["dcSource"], item["_source"]["dcTitle"] ])
	resposta = {}
	resposta["SNOMED"] = itensEncontrados

	with open('Snomed.json', 'w') as f:
		json.dump(resposta, f, indent=4)

	return resposta

if __name__ == "__main__":

	# mesh = BDMeSH(constantes.BD_SQL_MESH)
	# with mesh:
	# 	mesh.identificarTermosPelaPLN('how to avoid a heart attack today?', 'eng')

	# print( " ======== ")

	searchElasticSnomed('heart attack', 'O', 'en')
	# searchElasticMeSH('heart attack', 'O', 'eng')
    
	# with multiprocessing.Pool(processes=2) as pool:
	# 	pool.starmap(searchElasticMeSH, zip(meshDescritoresOriginais, itertools.repeat('O')))
	# 	pool.starmap(searchElasticSnomed, zip(meshDescritoresOriginais, itertools.repeat('O')))

	print(" ** CONCLUIDO ** ") 