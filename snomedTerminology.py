import snomedRF2bancoEstrutura as BancoSnomed 
import json, requests
# EquivalentClasses(:22298006 ObjectIntersectionOf(:64572001 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:116676008 :55641003) ObjectSomeValuesFrom(:363698007 :74281007)))))
# EquivalentClasses(:115451000119100 ObjectIntersectionOf(:243796009 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:246090004 :1847009) ObjectSomeValuesFrom(:408729009 :410515003) ObjectSomeValuesFrom(:408731000 :410513005) ObjectSomeValuesFrom(:408732007 :410604004)))))
# 'Associated finding' --246090004
# 'is modification of' --738774007
# 'occurrence' --246454002

def hierarquiaPorTermo(IdConcept):
    #a primeira query recupera os axiomas que possuem este conceptId
    #para cada axioma, eh analisado se eh uma hierarquia do termo
    #exemplo
    dataSetAxioma = []
    dataSetAxioma.append('EquivalentClasses(:238594009 ObjectIntersectionOf(:247446008 :418363000 :64572001 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:116676008 :409777003) ObjectSomeValuesFrom(:363698007 :39937001)))))')
    dataSetAxioma.append('ClassOf(:247446008 ObjectIntersectionOf(:404684003 ObjectSomeValuesFrom(:609096000 ObjectSomeValuesFrom(:363698007 :39937001))))')
    dataSetAxioma.append('EquivalentClasses(:201077008 ObjectIntersectionOf(:247446008 :418363000 :89105000 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:116676008 :409777003) ObjectSomeValuesFrom(:363698007 :39937001)))))')

    codigos = []
    objInterSize = len('ObjectIntersectionOf(') 
    for ax in dataSetAxioma: 
        pParentesis = ax.find('(')
        introAxioma = ax[0:pParentesis]
        if introAxioma == 'EquivalentClasses':
            pass
        else:
            if introAxioma == 'ClassOf':
                pass

        ind = ax.find('ObjectIntersectionOf(') 
        ehNumero = True 
        if ind > -1: 
            ind = ind + objInterSize 
            while (ehNumero): 
                espaco = ax.find(' ', ind) 
                if espaco > -1: 
                    cod = ax[ind+1:espaco]
                    ehNumero = cod.isdigit()
                    if (ehNumero): 
                        codigos.append(ax[ind+1:espaco]) 
                    ind = espaco + 1 
        print(codigos)


def teste():
    bancoDeDados = BancoSnomed.BDSnomed("db-snomed-RF2.sqlite3")
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
    hierarquiaPorTermo('247446008')
    #requisicaoREST()
