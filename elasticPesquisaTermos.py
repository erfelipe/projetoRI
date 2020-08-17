from datetime import date
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
import datetime as dt
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
	t1 = dt.datetime.now()
	with bancoMeSH:
		descritoresHierarquicos = bancoMeSH.selecionarDescritoresHierarquicos(termoProcurado, tipoTermo, idioma)
		termosEntradaHierarquicos = bancoMeSH.selecionarTermosDeEntradaHierarquicos(termoProcurado, tipoTermo, idioma)

	es = elasticsearch.Elasticsearch()
	es.indices.open("articles")
	itensEncontrados = []
	termosERevocacao = {} 
	itensEncontrados.clear()
	termosERevocacao.clear()

	if (termoProcurado not in descritoresHierarquicos) and (termoProcurado not in termosEntradaHierarquicos):
		resultSet = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termoProcurado, "type": "phrase", "fields": [ "dcTitle", "textBody" ]}}})['hits']
		termosERevocacao[termoProcurado] = len(resultSet['hits'])
		for item in resultSet['hits']:
			itensEncontrados.append([termoProcurado, item["_source"]["dcIdentifier"], item["_source"]["dcSource"], item["_source"]["dcTitle"] ])

	for desc in descritoresHierarquicos:
		resultSet = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": desc, "type": "phrase", "fields": [ "dcTitle", "textBody" ]}}})['hits']
		termosERevocacao[desc] = len(resultSet['hits'])
		for item in resultSet['hits']:
			itensEncontrados.append([desc, item["_source"]["dcIdentifier"], item["_source"]["dcSource"], item["_source"]["dcTitle"] ])

	for termo in termosEntradaHierarquicos:
		resultSet = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termo, "type": "phrase", "fields": [ "dcTitle", "textBody" ]}}})['hits']
		termosERevocacao[termo] = len(resultSet['hits'])
		for item in resultSet['hits']:
			itensEncontrados.append([termo, item["_source"]["dcIdentifier"], item["_source"]["dcSource"], item["_source"]["dcTitle"] ])
	
	t2 = dt.datetime.now()
	tempoGasto = (t2-t1).total_seconds()
	resposta = {}
	resposta["MeSH"] = itensEncontrados
	resposta["MeSHtotalTermosPesquisadosERevocacao"] = termosERevocacao
	resposta["MeSHTempoGasto"] = tempoGasto

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
	t1 = dt.datetime.now()
	termosProximosConceitualmente = []
	termosHierarquicos = [] 
	termosProximosConceitualmente.clear()
	termosHierarquicos.clear()
	with bancoSNOMED:
		iDPrincipal = bancoSNOMED.selecionarIdPrincipalDoTermo(termoProcurado)
		if iDPrincipal:
			termosHierarquicos = bancoSNOMED.selecionarTermosHierarquicos(iDPrincipal, tipoTermo, idioma)
			termosProximosConceitualmente = bancoSNOMED.selecionarTermosProximosConceitualmente(iDPrincipal, tipoTermo, idioma)
		else:
			logging.error("SNOMED - idPrincipal: %s nao identificado (!)", str(iDPrincipal), exc_info=True) 
		
		es = elasticsearch.Elasticsearch() 
		es.indices.open("articles") 
		itensEncontrados = [] 
		termosERevocacao = {}
		itensEncontrados.clear()
		termosERevocacao.clear()

		if (termoProcurado not in termosHierarquicos) and (termoProcurado not in termosProximosConceitualmente):
			resultSet = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termoProcurado, "type": "phrase", "fields": [ "dcTitle", "textBody" ]}}})['hits']
			termosERevocacao[termoProcurado] = len(resultSet['hits'])
			for item in resultSet['hits']:
				itensEncontrados.append([termoProcurado, item["_source"]["dcIdentifier"], item["_source"]["dcSource"], item["_source"]["dcTitle"] ])

		for termoHierq in termosHierarquicos:
			resultSet = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termoHierq, "type": "phrase", "fields": [ "dcTitle", "textBody" ]}}})['hits']
			termosERevocacao[termoHierq] = len(resultSet['hits'])
			for item in resultSet['hits']:
				itensEncontrados.append([termoHierq, item["_source"]["dcIdentifier"], item["_source"]["dcSource"], item["_source"]["dcTitle"] ])

		for termopConceitual in termosProximosConceitualmente:
			resultSet = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": termopConceitual, "type": "phrase", "fields": [ "dcTitle", "textBody" ]}}})['hits']
			termosERevocacao[termopConceitual] = len(resultSet['hits'])
			for item in resultSet['hits']:
				itensEncontrados.append([termopConceitual, item["_source"]["dcIdentifier"], item["_source"]["dcSource"], item["_source"]["dcTitle"] ])
	
	t2 = dt.datetime.now()
	tempoGasto = (t2-t1).total_seconds()
	resposta = {}
	resposta["SNOMED"] = itensEncontrados
	resposta["SNOMEDtotalTermosPesquisadosERevocacao"] = termosERevocacao
	resposta["SNOMEDtempoGasto"] = tempoGasto

	with open('Snomed.json', 'w') as f:
		json.dump(resposta, f, indent=4)

	return resposta

def comparaResultadosDasTerminologias(mesh, snomed, termoProcurado):
	""" Prepara os dados para serem gravados no banco de dados 

	Arguments:
		mesh {dict} -- Dicionario com os dados processados da terminologia MeSH
		snomed {dict} -- Dicionario com os dados processados da terminologia SNOMED CT
		termoProcurado {str} -- Termo que deu origem aos dados estatisticos 
	"""
	listaMeSH = mesh["MeSH"] 
	listaMeSHtotalTermosPesquisadosERevocacao = mesh["MeSHtotalTermosPesquisadosERevocacao"]
	tempoGastoMeSH = mesh["MeSHTempoGasto"]
	listaMeSHSomenteTermos = []
	contador = 0
	for key, value in listaMeSHtotalTermosPesquisadosERevocacao.items():
		listaMeSHSomenteTermos.append(key)
		if value > 0:
			contador += 1
	totalTermosPesquisadosComRevocacaoMesh = contador
	totalTermosPesquisadosERevocacaoMesh = len(listaMeSHtotalTermosPesquisadosERevocacao)

	# SNOMED 
	listaSnomed = snomed["SNOMED"] 
	listaSNOMEDtotalTermosPesquisadosERevocacao = snomed["SNOMEDtotalTermosPesquisadosERevocacao"]
	tempoGastoSnomed = snomed["SNOMEDtempoGasto"]
	listaSNOMEDSomenteTermos = []
	contador = 0
	for key, value in listaSNOMEDtotalTermosPesquisadosERevocacao.items():
		listaSNOMEDSomenteTermos.append(key)
		if value > 0:
			contador += 1
	totalTermosPesquisadosComRevocacaoSNOMED = contador
	totalTermosPesquisadosERevocacaoSNOMED = len(listaSNOMEDtotalTermosPesquisadosERevocacao)

	# Items comuns
	totalArtigosMeSH = len(listaMeSH) 
	totalArtigosSnomed = len(listaSnomed) 

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

	totalTermosComuns = len(list(set(listaMeSHSomenteTermos).intersection(listaSNOMEDSomenteTermos)))

	resultado = {}
	resultado['termo'] = termoProcurado
	#MeSH
	resultado['totalArtigosMesh'] = totalArtigosMeSH
	resultado['totalTermosPesquisadosMesh'] = totalTermosPesquisadosERevocacaoMesh
	resultado['totalTermosPesquisadosComRevocacaoMesh'] = totalTermosPesquisadosComRevocacaoMesh
	resultado['totalArtigosUnicosMesh'] = totalArtigosUnicosMeSH 
	resultado['totalArtigosRepetidosMesh'] = totalArtigosRepetidosMesh
	resultado['totalTempoGastoMesh'] = tempoGastoMeSH 
	#SNOMED
	resultado['totalArtigosSnomed'] = totalArtigosSnomed 
	resultado['totalTermosPesquisadosSnomed'] = totalTermosPesquisadosERevocacaoSNOMED
	resultado['totalTermosPesquisadosComRevocacaoSnomed'] = totalTermosPesquisadosComRevocacaoSNOMED
	resultado['totalArtigosUnicosSnomed'] = totalArtigosUnicosSnomed 
	resultado['totalArtigosRepetidosSnomed'] = totalArtigosRepetidosSnomed
	resultado['totalTempoGastoSnomed'] = tempoGastoSnomed

	resultado['totalArtigosComuns'] = totalArtigosComuns
	resultado['totalTermosComuns'] = totalTermosComuns

	banco = BDestatistica(constantes.BD_SQL_ESTATISTICA)
	with banco:
		pkEstatistica = banco.insereEstatistica(termoProcurado, len(termoProcurado.split()), totalArtigosMeSH, totalTermosPesquisadosERevocacaoMesh, totalTermosPesquisadosComRevocacaoMesh, totalArtigosUnicosMeSH, totalArtigosRepetidosMesh, tempoGastoMeSH, totalArtigosSnomed, totalTermosPesquisadosERevocacaoSNOMED, totalTermosPesquisadosComRevocacaoSNOMED, totalArtigosUnicosSnomed, totalArtigosRepetidosSnomed, tempoGastoSnomed, totalArtigosComuns, totalTermosComuns)
		# MeSH
		for termo, quant in listaMeSHtotalTermosPesquisadosERevocacao.items():
			banco.insereTermosAssociados(pkEstatistica, "M", str(termo), len(termo.split()), quant)
		# SNOMED
		for termo, quant in listaSNOMEDtotalTermosPesquisadosERevocacao.items():
			banco.insereTermosAssociados(pkEstatistica, "S", str(termo), len(termo.split()), quant)

	with open('resultadoComparacao.json', 'w') as f:
		json.dump(resultado, f, indent=4)

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
	i = 0
	with concurrent.futures.ThreadPoolExecutor() as executor:
		for termoProcurado in termos:
			i += 1
			print(str(i)+" -> "+termoProcurado)
			f1 = executor.submit(searchElasticSnomed, termoProcurado, 'O', 'en')
			f2 = executor.submit(searchElasticMeSH, termoProcurado, 'O', 'eng')
			listaSnomed = f1.result()
			listaMeSH = f2.result()
			listaFinal = {**listaSnomed, **listaMeSH}
			comparaResultadosDasTerminologias(listaMeSH, listaSnomed, termoProcurado)

			with open('search.json', 'w') as f:
				json.dump(listaFinal, f, indent=4)
			
			return listaFinal

if __name__ == "__main__":

	# mesh = BDMeSH(constantes.BD_SQL_MESH)
	# with mesh:
	# 	mesh.identificarTermosPelaPLN('how to avoid a heart attack today?', 'eng')
	
	#termosComuns = ["abdominal abscess", "plagiocephaly", "intermediate uveitis", "pulmonary hypertension", "coffin-lowry syndrome", "pleurisy", "hearing loss"]
	
	descritoresComuns = []
	#descritoresComuns = ["diabetes insipidus", "plagiocephaly", "hearing loss", "abdominal abscess", "intermediate uveitis", "pulmonary hypertension", "coffin-lowry syndrome", "pleurisy", "hearing loss", "heart attack", "hearing loss", "plagiocephaly"]
	#descritoresComuns = MeSHutils.carregarDescritoresComunsOriginaisMeSH(0, 0) 
	descritoresComuns.append("heart attack")
	
	#passe uma lISTA PARA A FUNCAO, pelamor!
	t1 = dt.datetime.now()
	iniciaPesquisaEmAmbasTerminologias(descritoresComuns)
	t2 = dt.datetime.now() 
	tempoGasto = (t2-t1).total_seconds() 
	print("tempo gasto hs ", str(tempoGasto/60/60)) 


