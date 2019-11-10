import snomedRF2banco as banco 
# EquivalentClasses(:22298006 ObjectIntersectionOf(:64572001 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:116676008 :55641003) ObjectSomeValuesFrom(:363698007 :74281007)))))
# EquivalentClasses(:115451000119100 ObjectIntersectionOf(:243796009 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:246090004 :1847009) ObjectSomeValuesFrom(:408729009 :410515003) ObjectSomeValuesFrom(:408731000 :410513005) ObjectSomeValuesFrom(:408732007 :410604004)))))
# 'Associated finding' --246090004
# 'is modification of' --738774007
# 'occurrence' --246454002

def extrair_associated_finding(axioma, objectproperyID, posini=0, listFinding = []):
    pos = axioma.find(objectproperyID, posini)
    if (pos == -1):
        return listFinding
    else:
        posiniconcept = axioma.find(':', pos)
        posfimconcept = axioma.find(')', pos)
        if (posiniconcept > 0 and posfimconcept > 0):
            concept = axioma[posiniconcept+1:posfimconcept]
            listFinding.append(concept)
            return extrair_associated_finding(axioma, objectproperyID, posfimconcept)
        else:
            return listFinding

if __name__ == "__main__":
    bancoDeDados = banco.BD("db-snomed-RF2.sqlite3")
    with bancoDeDados:
        listaDeTermos = bancoDeDados.selecionarListaDeTermosPorNome("heart attack")
        print(listaDeTermos)

    axioma = 'EquivalentClasses(:115451000119100 ObjectIntersectionOf(:243796009 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:246090004 :1847009) ObjectSomeValuesFrom(:408729009 :410515003) ObjectSomeValuesFrom(:408731000 :410513005) ObjectSomeValuesFrom(:408732007 :410604004))))ObjectSomeValuesFrom(:246090004 :66514008))'
    lista = extrair_associated_finding(axioma, '246090004')
    print(lista)
    with bancoDeDados:
        termosDeAxiomas = bancoDeDados.selecionarListaDeTermosPorCodigo(lista)
        print(termosDeAxiomas)