import logging
import concurrent.futures
import elasticsearch 
from MeSHbancoEstrutura import BDMeSH
import constantes
from estatisticaBancoEstrutura import BDestatistica 
from snomedRF2bancoEstrutura import BDSnomed 
import constantes 
import logging 
import json 
import MeSHutils

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
	termosComRevocacao = [] 

	for desc in descritoresHierarquicos:
		resultSet = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": desc, "type": "phrase", "fields": [ "dcTitle", "textBody" ]}}})['hits']
		for item in resultSet['hits']:
			itensEncontrados.append([desc, item["_source"]["dcIdentifier"], item["_source"]["dcSource"], item["_source"]["dcTitle"] ])
			termosComRevocacao.append(desc)

	for termo in termosEntradaHierarquicos:
		resultSet = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termo, "type": "phrase", "fields": [ "dcTitle", "textBody" ]}}})['hits']
		for item in resultSet['hits']:
			itensEncontrados.append([termo, item["_source"]["dcIdentifier"], item["_source"]["dcSource"], item["_source"]["dcTitle"] ])
			termosComRevocacao.append(termo)

	totalTermosPesquisados = list(descritoresHierarquicos.union(termosEntradaHierarquicos))
	totalTermosPesquisadosComRevocacao = list(set(termosComRevocacao))
	resposta = {}
	resposta["MeSH"] = itensEncontrados
	resposta["MeSHtotalTermosPesquisados"] = totalTermosPesquisados
	resposta["MeSHtotalTermosPesquisadosComRevocacao"] = totalTermosPesquisadosComRevocacao

	# with open('MeSH.json', 'w') as f:
	# 	json.dump(resposta, f, indent=4)

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
			termosProximosConceitualmente = bancoSNOMED.selecionarTermosProximosConceitualmente(iDPrincipal, 'O', idioma)
		else:
			logging.error("SNOMED - idPrincipal: %s nao identificado (!)", str(iDPrincipal), exc_info=True) 
			termosProximosConceitualmente = []
			termosHierarquicos = [] 
		
		es = elasticsearch.Elasticsearch() 
		es.indices.open("articles") 
		itensEncontrados = [] 
		termosComRevocacao = []

		for termoHierq in termosHierarquicos:
			resultSet = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termoHierq, "type": "phrase", "fields": [ "dcTitle", "textBody" ]}}})['hits']
			for item in resultSet['hits']:
				itensEncontrados.append([termoHierq, item["_source"]["dcIdentifier"], item["_source"]["dcSource"], item["_source"]["dcTitle"] ])
				termosComRevocacao.append(termoHierq)

		for termopConceitual in termosProximosConceitualmente:
			resultSet = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termopConceitual, "type": "phrase", "fields": [ "dcTitle", "textBody" ]}}})['hits']
			for item in resultSet['hits']:
				itensEncontrados.append([termopConceitual, item["_source"]["dcIdentifier"], item["_source"]["dcSource"], item["_source"]["dcTitle"] ])
				termosComRevocacao.append(termopConceitual)
	
	totalTermosPesquisados = []
	totalTermosPesquisados = list(termosHierarquicos) + list(termosProximosConceitualmente)
	totalTermosPesquisadosComRevocacao = list(set(termosComRevocacao))
	resposta = {}
	resposta["SNOMED"] = itensEncontrados
	resposta["SNOMEDtotalTermosPesquisados"] = totalTermosPesquisados
	resposta["SNOMEDtotalTermosPesquisadosComRevocacao"] = totalTermosPesquisadosComRevocacao

	# with open('Snomed.json', 'w') as f:
	# 	json.dump(resposta, f, indent=4)

	return resposta

def comparaResultadosDasTerminologias(mesh, snomed, termoProcurado):
	""" Prepara os dados para serem gravados no banco de dados 

	Arguments:
		mesh {dict} -- Dicionario com os dados processados da terminologia MeSH
		snomed {dict} -- Dicionario com os dados processados da terminologia SNOMED CT
		termoProcurado {str} -- Termo que deu origem aos dados estatisticos 
	"""
	listaMeSH = mesh["MeSH"] 
	totalTermosPesquisadosMesh = len(mesh["MeSHtotalTermosPesquisados"])
	totalTermosPesquisadosComRevocacaoMesh = len(mesh["MeSHtotalTermosPesquisadosComRevocacao"])
	listaSnomed = snomed["SNOMED"] 
	totalTermosPesquisadosSnomed = len(snomed["SNOMEDtotalTermosPesquisados"])
	totalTermosPesquisadosComRevocacaoSnomed = len(snomed["SNOMEDtotalTermosPesquisadosComRevocacao"])
	totalArtigosMeSH = len(listaMeSH) 
	totalArtigosSnomed = len(listaSnomed) 

	# Items comuns
	itemsIguais = set()
	for itemMesh in listaMeSH: 
		hashMesh = itemMesh[1] 
		for itemSnomed in listaSnomed: 
			hashSnomed = itemSnomed[1] 
			if hashSnomed == hashMesh: 
				itemsIguais.add(hashSnomed) 
	totalArtigosComuns = len(itemsIguais) 

	# Lista dos hashs do MeSH
	listaHashMeSH = []
	for item in listaMeSH:
		listaHashMeSH.append(item[1])
	totalArtigosUnicosMeSH = len(set(listaHashMeSH))

	# Lista dos hashs do SNOMED CT 
	listsaHashSnomed = []
	for item in listaSnomed:
		listsaHashSnomed.append(item[1])
	totalArtigosUnicosSnomed = len(set(listsaHashSnomed))

	totalArtigosRepetidosMesh = quantItemsRepetidosEmUmaLista(listaMeSH)
	totalArtigosRepetidosSnomed = quantItemsRepetidosEmUmaLista(listaSnomed)

	totalTermosComuns = len(list(set(mesh["MeSHtotalTermosPesquisados"]).intersection(snomed["SNOMEDtotalTermosPesquisados"])))

	resultado = {}
	resultado['termo'] = termoProcurado

	resultado['totalArtigosMesh'] = totalArtigosMeSH
	resultado['totalTermosPesquisadosMesh'] = totalTermosPesquisadosMesh
	resultado['totalTermosPesquisadosComRevocacaoMesh'] = totalTermosPesquisadosComRevocacaoMesh
	resultado['totalArtigosUnicosMesh'] = totalArtigosUnicosMeSH 
	resultado['totalArtigosRepetidosMesh'] = totalArtigosRepetidosMesh

	resultado['totalArtigosSnomed'] = totalArtigosSnomed 
	resultado['totalTermosPesquisadosSnomed'] = totalTermosPesquisadosSnomed
	resultado['totalTermosPesquisadosComRevocacaoSnomed'] = totalTermosPesquisadosComRevocacaoSnomed
	resultado['totalArtigosUnicosSnomed'] = totalArtigosUnicosSnomed 
	resultado['totalArtigosRepetidosSnomed'] = totalArtigosRepetidosSnomed

	resultado['totalArtigosComuns'] = totalArtigosComuns
	resultado['totalTermosComuns'] = totalTermosComuns

	banco = BDestatistica(constantes.BD_SQL_ESTATISTICA)
	with banco:
		pkEstatistica = banco.insereEstatistica(termoProcurado, len(termoProcurado.split()), totalArtigosMeSH, totalTermosPesquisadosMesh, totalTermosPesquisadosComRevocacaoMesh, totalArtigosUnicosMeSH, totalArtigosRepetidosMesh, totalArtigosSnomed, totalTermosPesquisadosSnomed, totalTermosPesquisadosComRevocacaoSnomed, totalArtigosUnicosSnomed, totalArtigosRepetidosSnomed, totalArtigosComuns, totalTermosComuns)
		# segue as iteracoes para cada terminologia e seus conjuntos de termos
		for termo in mesh["MeSHtotalTermosPesquisados"]:
			banco.insereTermosAssociados(pkEstatistica,"M", "P", str(termo), len(termo.split()))
		for termo in mesh["MeSHtotalTermosPesquisadosComRevocacao"]:
			banco.insereTermosAssociados(pkEstatistica, "M", "R", str(termo), len(termo.split()))

		for termo in snomed["SNOMEDtotalTermosPesquisados"]:
			banco.insereTermosAssociados(pkEstatistica, "S", "P", str(termo), len(termo.split()))
		for termo in snomed["SNOMEDtotalTermosPesquisadosComRevocacao"]:
			banco.insereTermosAssociados(pkEstatistica, "S", "R", str(termo), len(termo.split()))

	# with open('resultadoComparacao.json', 'w') as f:
	# 	json.dump(resultado, f, indent=4)

def quantItemsRepetidosEmUmaLista(listaTratada):
	""" Realiza uma contagem de itens repetidos em uma lista (array) 
		Considerando uma lista unica (set) da propria lista, exclui-se o item pesquisado para que nao seja contado duas vezes

	Arguments:
		lista {list} -- Listagem de itens (array)

	Returns:
		int -- Quantidade de itens repetidos em uma determinada lista
	"""	
	listaTotal = []
	for item in listaTratada:
		listaTotal.append(item[1])
	quantItemsRepetidos = 0
	listaUnica = set(listaTotal) 
	for item in listaUnica:
		if item in listaTotal:
			listaTotal.remove(item)
		for itemTotal in listaTotal:
			if (item == itemTotal):
				quantItemsRepetidos += 1
		listaSemItemAtual = [it for it in listaTotal if it != item]
		listaTotal = listaSemItemAtual
	return quantItemsRepetidos

def iniciaPesquisaEmAmbasTerminologias(termos):
	""" Permite procurar os termos da lista nas terminologias e gerar o JSON e estatistica de BD

	Args:
		termoProcurado (list): Recebe uma lista (array) de termos para serem procurados nas terminologias
	"""	
	with concurrent.futures.ThreadPoolExecutor() as executor:
		for termoProcurado in termos:
			print(termoProcurado)
			f1 = executor.submit(searchElasticSnomed, termoProcurado, 'O', 'en')
			f2 = executor.submit(searchElasticMeSH, termoProcurado, 'O', 'eng')
			listaSnomed = f1.result()
			listaMeSH = f2.result()
			listaFinal = {**listaSnomed, **listaMeSH}
			print("Funçao compara resultados")
			comparaResultadosDasTerminologias(listaMeSH, listaSnomed, termoProcurado)

			# with open('search.json', 'w') as f:
			# 	json.dump(listaFinal, f, indent=4)

if __name__ == "__main__":

	# mesh = BDMeSH(constantes.BD_SQL_MESH)
	# with mesh:
	# 	mesh.identificarTermosPelaPLN('how to avoid a heart attack today?', 'eng')
	
	#termosComuns = ["plagiocephaly", "intermediate uveitis", "pulmonary hypertension", "coffin-lowry syndrome", "pleurisy"]
	
	descritoresComuns = []
	descritoresComuns = MeSHutils.carregarDescritoresComunsOriginaisMeSH(0, 0)
	#descritoresComuns.append("heart attack")

	#passe uma lISTA PARA A FUNCAO, pelamor!
	iniciaPesquisaEmAmbasTerminologias(descritoresComuns)
