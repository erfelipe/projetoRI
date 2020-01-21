import snomedRF2bancoEstrutura as BancoSnomed 
import json, requests
# EquivalentClasses(:22298006 ObjectIntersectionOf(:64572001 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:116676008 :55641003) ObjectSomeValuesFrom(:363698007 :74281007)))))
# EquivalentClasses(:115451000119100 ObjectIntersectionOf(:243796009 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:246090004 :1847009) ObjectSomeValuesFrom(:408729009 :410515003) ObjectSomeValuesFrom(:408731000 :410513005) ObjectSomeValuesFrom(:408732007 :410604004)))))
# 'Associated finding' --246090004
# 'is modification of' --738774007
# 'occurrence' --246454002

def hierarquiaDeIDsPorIdConcept(IdConcept, resp = []):
    #a primeira query recupera os axiomas que possuem este conceptId
    #para cada axioma, eh analisado se eh uma hierarquia do termo
    #exemplo 
    # dataSetAxioma.append('EquivalentClasses(:238594009 ObjectIntersectionOf(:247446008 :418363000 :64572001 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:116676008 :409777003) ObjectSomeValuesFrom(:363698007 :39937001)))))')
    # dataSetAxioma.append('ClassOf(:247446008 ObjectIntersectionOf(:404684003 ObjectSomeValuesFrom(:609096000 ObjectSomeValuesFrom(:363698007 :39937001))))')
    # dataSetAxioma.append('EquivalentClasses(:201077008 ObjectIntersectionOf(:247446008 :418363000 :89105000 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:116676008 :409777003) ObjectSomeValuesFrom(:363698007 :39937001)))))')

    bancoDeDados = BancoSnomed.BDSnomed("/Volumes/SD-64-Interno/BancosSQL/db-snomed-RF2.sqlite3")
    with bancoDeDados:
        dataSetAxioma = bancoDeDados.selecionarAxiomasPorConceptID(IdConcept)

    if len(dataSetAxioma) <= 0:
        return resp

    codigos = []
    objInterSize = len('ObjectIntersectionOf(') 
    for ax in dataSetAxioma: 
        codigos.clear()
        pParentesis = ax[0].find('(')

        introAxioma = ax[0][0:pParentesis]
        if introAxioma == 'EquivalentClasses':
            espaco = ax[0].find(' ', len('EquivalentClasses(:'))
            axAbout = ax[0][len('EquivalentClasses(:') : espaco]
        else:
            if introAxioma == 'SubClassOf':
                espaco = ax[0].find(' ', len('SubClassOf(:'))
                axAbout = ax[0][len('SubClassOf(:') : espaco]

        ind = ax[0].find('ObjectIntersectionOf(') 
        ehNumero = True 
        if ind > -1: 
            ind = ind + objInterSize 
            while (ehNumero): 
                espaco = ax[0].find(' ', ind) 
                if espaco > -1: 
                    cod = ax[0][ind+1 : espaco]
                    ehNumero = cod.isdigit()
                    if (ehNumero): 
                        codigos.append(ax[0][ind+1:espaco]) 
                    ind = espaco + 1 
                else:
                    ehNumero = False
        isChild = False
        for cod in codigos:
            if (cod == IdConcept):
                isChild = True
        if isChild:
            resp.append(axAbout) 
            temMaisFilhos = hierarquiaDeIDsPorIdConcept(axAbout)
            if (len(temMaisFilhos) > 0) and (type(temMaisFilhos) == str):
                resp.append(temMaisFilhos)
    return resp


def teste():
    bancoDeDados = BancoSnomed.BDSnomed("/Volumes/SD-64-Interno/BancosSQL/db-snomed-RF2.sqlite3")
    with bancoDeDados:
        listaDeTermos = bancoDeDados.selecionarListaDeTermosPorNome("Advance healthcare directive status")
        print('termo buscado: ', listaDeTermos)

    #para cada termo, encontrar os axiomas pelo ID
    for item in listaDeTermos:
        with bancoDeDados:
            print("Termo procurado: " + str(item))
            termosHierarq = bancoDeDados.extrairTermosHierarquicosPorTermoOriginal(item[1])
            print("termos hierarquivos originais: " + str(termosHierarq))

            # axioma = bancoDeDados.selecionarAxiomaPorID(item[1])
            # if len(axioma) > 0:
            #     print("Axioma: " + str(axioma[0]))
            #     lstCodigosAssociacaoDoAxioma = bancoDeDados.extrairConceitoRelacionadoDoAxioma(str(axioma[0]), item[1])
            #     print("------ lista de associacao: " , lstCodigosAssociacaoDoAxioma)
            #     listaDeTermosDosAxiomas = bancoDeDados.selecionarListaDeTermosPorCodigo(lstCodigosAssociacaoDoAxioma)
            #     print("------ listas de conceitos axiomaticos: ", listaDeTermosDosAxiomas) 

def requisicaoREST():
    link = 'http://api.springernature.com/meta/v2/json?q=title:skin&api_key=df74b53a74acc1bf8bd3fb285696b415'
    resposta = requests.get(link)
    dadosFormatados = resposta.json() #json.loads(resposta.content)
    print(dadosFormatados)

if __name__ == "__main__":
    hierq = hierarquiaDeIDsPorIdConcept('247446008')
    print('------')
    print (hierq)

    #requisicaoREST()
