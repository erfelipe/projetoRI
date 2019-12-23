from MeSHbancoEstrutura import BD
import MeSHutils as utils

# Exemplo fixo para identificar os termos a serem pesquisados no MeSH
# Funcionalidades básicas para a chamada das principais funções 

def main():
	bancodedados = BD("db-MeSH.sqlite3")
	with bancodedados:
                termoProcurado = "heart attack"
                resposta = bancodedados.selecionarIdDescritor_NomeDescritor(termoProcurado)
                print(resposta)
                idDescritor = str(resposta[0])
                termoPrincipal = str(resposta[1])
                print('iddescritor: ')
                print(idDescritor)
                print('termoPrincipal: ')
                print(termoPrincipal)
                resposta = bancodedados.selecionarIdsHierarquiaPorIdDescritor(idDescritor)
                print('resposta:')
                print(resposta)
                
                termosUnificados = set()
                for h in resposta:
                        data = bancodedados.selecionarTermosPorIdHierarquico(h[0])
                        for item in data:
                                termosUnificados.add(item)

                dsTermosDeEntada = bancodedados.selectionarTermosDeEntrada(idDescritor)
                termosEntrada = set()
                for ent in dsTermosDeEntada:
                        if (ent[0] != termoPrincipal and ent[0] != termoProcurado):
                                termosEntrada.add(ent[0])
                        if (ent[0] in termosUnificados):
                                termosUnificados.remove(ent[0])
                
                print('termos de entrada: ')
                print(termosEntrada)

                print('termos unificados: ')
                print(termosUnificados)

                if (termoProcurado in termosUnificados):
                        termosUnificados.remove(termoProcurado)
                if (termoPrincipal in termosUnificados):
                        termosUnificados.remove(termoPrincipal)

        
if __name__ == "__main__":
    main()
