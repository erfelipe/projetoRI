import snomedRF2banco as banco 
# EquivalentClasses(:22298006 ObjectIntersectionOf(:64572001 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:116676008 :55641003) ObjectSomeValuesFrom(:363698007 :74281007)))))
# EquivalentClasses(:115451000119100 ObjectIntersectionOf(:243796009 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:246090004 :1847009) ObjectSomeValuesFrom(:408729009 :410515003) ObjectSomeValuesFrom(:408731000 :410513005) ObjectSomeValuesFrom(:408732007 :410604004)))))
# 'Associated finding' --246090004
# 'is modification of' --738774007
# 'occurrence' --246454002

def extrairConceitoRelacionadoDoAxioma(axioma, objectproperyID, posini=0, listFinding = []):
    pos = axioma.find(objectproperyID, posini)
    if (pos == -1):
        return listFinding
    else:
        posiniconcept = axioma.find(':', pos)
        posfimconcept = axioma.find(')', pos)
        if (posiniconcept > 0 and posfimconcept > 0):
            concept = axioma[posiniconcept+1:posfimconcept]
            if concept.isnumeric():
                listFinding.append(concept)
                return extrairConceitoRelacionadoDoAxioma(axioma, objectproperyID, posfimconcept)
        else:
            return listFinding

if __name__ == "__main__":
    bancoDeDados = banco.BD("db-snomed-RF2.sqlite3")
    with bancoDeDados:
        listaDeTermos = bancoDeDados.selecionarListaDeTermosPorNome("Method")
        print('termo buscado: ', listaDeTermos)

    #para cada termo, encontrar os axiomas pelo ID
    for item in listaDeTermos:
        with bancoDeDados:
            axioma = bancoDeDados.selecionarAxiomaPorID(item[1])
            if len(axioma) > 0:
                lstCodigosAssociacaoDoAxioma = extrairConceitoRelacionadoDoAxioma(str(axioma[0]), item[1])
                print("------ lista de associacao: " , lstCodigosAssociacaoDoAxioma)
                listaDeTermosDosAxiomas = bancoDeDados.selecionarListaDeTermosPorCodigo(lstCodigosAssociacaoDoAxioma)
                print("------ listas de conceitos axiomaticos: ", listaDeTermosDosAxiomas) 
