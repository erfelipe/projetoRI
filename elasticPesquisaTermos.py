import elasticsearch
from MeSHbancoEstrutura import BDMeSH
from snomedRF2bancoEstrutura import BDSnomed
from elasticBancoEstrutura import BDelastic


def searchElasticMeSH(termoProcurado):
	# Procura os todos os termos relacionados
	bancoMeSH = BDMeSH("/Volumes/SD-64-Interno/BancosSQL/db-MeSH.sqlite3")
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
	bancoElastic = BDelastic("db-elastic.sqlite3")
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
    #pelo nome do termo, identifica se o seu código de conceito 
	bancoSNOMED = BDSnomed("/Volumes/SD-64-Interno/BancosSQL/db-snomed-RF2.sqlite3") 
	with bancoSNOMED:
		#pelo código de conceito, encontra se as descriçoes associadas (labels)
		iDsRelacionados = bancoSNOMED.selecionarConceptIdsPorTermo(termoProcurado) 
		print('* selecionarConceptIdsPorTermo')
		print(iDsRelacionados)

		#dos vários conceitos, procura se um principal	
		iDPrincipal = bancoSNOMED.selecionarIdPrincipal(iDsRelacionados)
		print('* selecionarIdPrincipal')
		print(iDPrincipal)

		#pelo código de conceito principal, encontra se os termos hierarquicos
		iDsHierarquicos = bancoSNOMED.hierarquiaDeIDsPorIdConcept(iDPrincipal)
		print('* hierarquiaDeIDsPorIdConcept')
		print(iDsHierarquicos)
		termosHierarquicos = bancoSNOMED.selecionarDescricoesPorIDsConcept(iDsHierarquicos)
		print('* selecionarDescricoesPorIDsConcept')
		print(termosHierarquicos)
		#pesquisa no elastic e grava no sqlite 


if __name__ == "__main__":
	# bancoElastic = BDelastic("db-elastic.sqlite3")
	# with bancoElastic:
	# 	bancoElastic.criarBancoDeDados()

	# bancoMeSH = BDMeSH("db-MeSH.sqlite3")
	# with bancoMeSH:
	# 	lstTermos = bancoMeSH.selecionarTermosPorIdHierarquico('C')
	# 	for t in lstTermos:
	# 		searchElasticMeSH(t) 

	searchElasticSnomed("heart attack") 

	#searchElasticSnomed("Myositis") - funcionou bonito ! 

	# bancoSNOMED = BDSnomed("/Volumes/SD-64-Interno/BancosSQL/db-snomed-RF2.sqlite3")
	# with bancoSNOMED:
	# 	pass

