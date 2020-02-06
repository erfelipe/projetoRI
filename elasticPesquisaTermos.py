import elasticsearch
from MeSHbancoEstrutura import BDMeSH
from snomedRF2bancoEstrutura import BDSnomed
from elasticBancoEstrutura import BDelastic
import constantes
import json
import logging

logging.basicConfig(filename=constantes.LOG_FILE, filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

def searchElasticMeSH(termoProcurado):
	# Procura os todos os termos relacionados
	bancoMeSH = BDMeSH(constantes.BD_SQL_MESH)
	with bancoMeSH:
		resposta = bancoMeSH.selecionarIdDescritor_NomeDescritor(termoProcurado) 
		idDescritor = str(resposta[0])
		#DUVIDA aqui, como sei o descritor principal se pode haver varios
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

	# Duas listas set necessarias:
	# termosEntrada e termosHierarquicos

	# Dois termos importantes:
	# termoProcurado e descritorPrincipal
    # Procura os termos no elastic e grava no banco
	es = elasticsearch.Elasticsearch()
	es.indices.open("articles")
	quantTermoProcurado = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termoProcurado, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']
	quantDescritorPrincipal = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": descritorPrincipal, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']

	# Grava no sqlite 
	bancoElastic = BDelastic(constantes.BD_SQL_ELASTIC)
	with bancoElastic:
		idBancoTermoProcurado = bancoElastic.insereTermoProcurado('M', termoProcurado, quantTermoProcurado, idDescritor, descritorPrincipal, quantDescritorPrincipal )
		# Procura as listas no elastic e grava no banco
		for tE in termosEntrada:
			quantTe = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": tE, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']
			bancoElastic.insereTermoAssociado(idBancoTermoProcurado, tE, quantTe, 'E')
		for tH in termosHierarquicos:
			quantTh = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": tH, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']
			bancoElastic.insereTermoAssociado(idBancoTermoProcurado, tH, quantTh, 'H')


def searchElasticSnomed(termoProcurado):
	"""Realiza a pesquisa com todas as caracteristicas necessarias no SNOMED
	   considerando termos relacionados e termos hierarquicos
	
	Arguments:
		termoProcurado {str} -- Termo comum já selecionado pelo processo randomico e que serah procurado no MeSH tambem
	"""
	#pelo nome do termo, identifica se o seu código de conceito 
	bancoSNOMED = BDSnomed(constantes.BD_SQL_SNOMED) 
	with bancoSNOMED:
		#pelo código de conceito, encontra se as descriçoes associadas (labels)
		iDsRelacionados = bancoSNOMED.selecionarConceptIdsPorTermo(termoProcurado) 
		logging.info("SNOMED - termo: %s - idsRelacionados: %s" , str(termoProcurado), str(iDsRelacionados))

		#dos vários conceitos, procura se um principal	
		iDPrincipal = bancoSNOMED.selecionarIdPrincipal(iDsRelacionados)
		if iDPrincipal:
			termosProximosConceitualmente = bancoSNOMED.selecionarDescricoesPorIDsConcept(iDPrincipal)
			logging.info("SNOMED - idPrincipal: %s - termos proximos conceitualmente: %s", str(iDPrincipal), str(termosProximosConceitualmente))
			iDsHierarquicos = bancoSNOMED.hierarquiaDeIDsPorIdConcept(iDPrincipal)
			logging.info("SNOMED - idPrincipal: %s - termos hierarquicos: %s", str(iDPrincipal), str(iDsHierarquicos))
			termosHierarquicos = bancoSNOMED.selecionarDescricoesPorIDsConcept(iDsHierarquicos)
		else:
			logging.error("SNOMED - idPrincipal: %s nao identificado iDPrincipal (!)", str(iDPrincipal), exc_info=True) 
			termosProximosConceitualmente = []
			termosHierarquicos = [] 

		#pesquisa no elastic e grava no sqlite 
		es = elasticsearch.Elasticsearch() 
		es.indices.open("articles") 
		quantTermoProcurado = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termoProcurado, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']
		#quantDescritorPrincipal = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": descritorPrincipal, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']

		# Grava no sqlite 
		bancoElastic = BDelastic(constantes.BD_SQL_ELASTIC) 
		with bancoElastic:
			idBancoTermoProcurado = bancoElastic.insereTermoProcurado('S', termoProcurado, quantTermoProcurado, iDPrincipal, 'a descobrir', quantTermoProcurado )
			# Procura as listas no elastic e grava no banco
			for termopConceitual in termosProximosConceitualmente:
				quant = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termopConceitual, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']
				#termos conceitualmente relacionados a entrada
				bancoElastic.insereTermoAssociado(idBancoTermoProcurado, termopConceitual, quant, 'E')

			for termoHierq in termosHierarquicos:
				quant = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termoHierq, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']
				#termos hierarquicamente relacionados com o termo da entrada
				bancoElastic.insereTermoAssociado(idBancoTermoProcurado, termoHierq, quant, 'H')


if __name__ == "__main__":

	searchElasticSnomed("aspartate-ammonia ligase")

	# with open(constantes.TERMOS_COMUNS_JSON, 'r') as f:
	# 	termosComuns = json.load(f)
	
	# for termo in termosComuns: 
	# 	print('* Processando: ' + termo) 
	# 	searchElasticSnomed(termo) 
	# 	#searchElasticMeSH(termo) 

	print(" ** CONCLUIDO ** ")