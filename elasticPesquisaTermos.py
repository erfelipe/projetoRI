import multiprocessing 
import elasticsearch 
from MeSHbancoEstrutura import BDMeSH 
from snomedRF2bancoEstrutura import BDSnomed 
from elasticBancoEstrutura import BDelastic 
import constantes 
import logging 
import itertools

logging.basicConfig(filename=constantes.LOG_FILE, filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

def searchElasticMeSH(termoProcurado, tipoTermo):
	"""Realiza a pesquisa com todas as caracteristicas do instrumento terminologico MeSH
	   considerando termos hierarquicos (este instrumento nao possui termos relacionados)
	
	Arguments:
		termoProcurado {str} -- Termo comum selecionado previamente em uma lista comum
		tipoTermo {str} -- O = original e T = tratado
	"""
	# Procura os todos os termos relacionados
	bancoMeSH = BDMeSH(constantes.BD_SQL_MESH)
	with bancoMeSH:
		resposta = bancoMeSH.selecionarIdDescritorPeloNomeDescritor(termoProcurado) 
		idDescritor = str(resposta[0]) 
		descritorPrincipal = str(resposta[1]) 
		
		print("MeSH processando: " + termoProcurado + " id " + str(idDescritor) + " principal: " + descritorPrincipal)
		
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

    # Procura os termos no elastic e grava no banco
	es = elasticsearch.Elasticsearch()
	es.indices.open("articles")
	quantTermoProcurado = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termoProcurado, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']
	quantDescritorPrincipal = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": descritorPrincipal, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']

	# Grava no sqlite 
	bancoElastic = BDelastic(constantes.BD_SQL_ELASTIC)
	with bancoElastic:
		idBancoTermoProcurado = bancoElastic.insereTermoProcurado('M', tipoTermo, termoProcurado, quantTermoProcurado, idDescritor, descritorPrincipal, quantDescritorPrincipal )
		# Procura as listas no elastic e grava no banco
		for tE in termosEntrada:
			quantTe = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": tE, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']
			bancoElastic.insereTermoAssociado(idBancoTermoProcurado, tE, quantTe, 'E')
		for tH in termosHierarquicos:
			quantTh = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": tH, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']
			bancoElastic.insereTermoAssociado(idBancoTermoProcurado, tH, quantTh, 'H')

def searchElasticSnomed(termoProcurado, tipoTermo):
	"""Realiza a pesquisa com todas as caracteristicas do instrumento terminologico SNOMED CT
	   considerando termos hierarquicos e relacionados (este instrumento possui o conceito de termos relacionados)
	
	Arguments:
		termoProcurado {str} -- Termo comum selecionado previamente em uma lista comum
		tipoTermo {str} -- O = original e T = tratado
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
			termoPrincipal = bancoSNOMED.selecionarTermoOriginalPrincipal(iDPrincipal, 'en')
			termosProximosConceitualmente = bancoSNOMED.selecionarDescricoesPorIDsConcept(iDPrincipal)
			logging.info("SNOMED - idPrincipal: %s - termos proximos conceitualmente: %s", str(iDPrincipal), str(termosProximosConceitualmente))

			iDsHierarquicos = bancoSNOMED.hierarquiaDeIDsPorIdConcept(iDPrincipal)
			logging.info("SNOMED - idPrincipal: %s - termos hierarquicos: %s", str(iDPrincipal), str(iDsHierarquicos))
			termosHierarquicos = bancoSNOMED.selecionarDescricoesPorIDsConcept(iDsHierarquicos)
		else:
			termoPrincipal = ""
			logging.error("SNOMED - idPrincipal: %s nao identificado iDPrincipal (!)", str(iDPrincipal), exc_info=True) 
			termosProximosConceitualmente = []
			termosHierarquicos = [] 
		
		print("SNOMED processando: " + termoProcurado + " id principal: " + str(iDPrincipal) )
		
		#pesquisa no elastic e grava no sqlite 
		es = elasticsearch.Elasticsearch() 
		es.indices.open("articles") 
		quantTermoProcurado = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termoProcurado, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']
		#quantDescritorPrincipal = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": descritorPrincipal, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']

		# Grava no sqlite 
		bancoElastic = BDelastic(constantes.BD_SQL_ELASTIC) 
		with bancoElastic:
			idBancoTermoProcurado = bancoElastic.insereTermoProcurado('S', tipoTermo, termoProcurado, quantTermoProcurado, iDPrincipal, termoPrincipal, quantTermoProcurado )
			# Procura as listas no elastic e grava no banco
			for termopConceitual in termosProximosConceitualmente:
				quant = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termopConceitual, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']
				#termos 'C'onceitualmente relacionados a entrada
				bancoElastic.insereTermoAssociado(idBancoTermoProcurado, termopConceitual, quant, 'C')

			for termoHierq in termosHierarquicos:
				quant = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termoHierq, "type": "phrase", "fields": [ "dcTitle", "dcDescription" ]}}})['hits']['total']['value']
				#termos 'H'ierarquicamente relacionados com o termo da entrada
				bancoElastic.insereTermoAssociado(idBancoTermoProcurado, termoHierq, quant, 'H')


if __name__ == "__main__":

	meshDescritoresOriginais = []

	# meshDescritoresOriginais.append('haloferax')
	# meshDescritoresOriginais.append('bartonella')
	# meshDescritoresOriginais.append('bartonella')
	# meshDescritoresOriginais.append('toxascaris')
	# meshDescritoresOriginais.append('platelet factor 4')
	# meshDescritoresOriginais.append('perioperative care')

	with open(constantes.MESH_DESCRITORES_COMUNS_ORIGINAIS) as f:
		linhas =  f.read().splitlines()
	f.close()

	for linha in linhas:
		meshDescritoresOriginais.append(linha)

	# print(tuple(zip(meshDescritoresOriginais, itertools.repeat('O'))))
	# searchElasticSnomed('insecta', 'O')

	total = str(len(meshDescritoresOriginais)) 
	i = 1 
	for descritor in meshDescritoresOriginais: 
		print(str(i) + " de " + str(total) + ' processando: ' + descritor) 
		searchElasticSnomed(descritor, 'O') 
		i += 1 

	# with multiprocessing.Pool(processes=2) as pool:
	# 	pool.starmap(searchElasticMeSH, zip(meshDescritoresOriginais, itertools.repeat('O')))
	# 	pool.starmap(searchElasticSnomed, zip(meshDescritoresOriginais, itertools.repeat('O')))

	print(" ** CONCLUIDO ** ") 