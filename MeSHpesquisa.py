from MeSHbancoEstrutura import BD
import MeSHutils as utils

def main():
	bancodedados = BD("db-MeSH.sqlite3")
	with bancodedados:
                resposta = bancodedados.selecionarDescritor_e_Termo("heart")
                print(resposta)
                iddescritor = str(resposta[0])
                idhierqprincipal = str(resposta[1])
                nomeprincipal = str(resposta[2])
                print('iddescritor')
                print(iddescritor)
                print('idhierqprincipal')
                print(idhierqprincipal)
                print('nomeprincipal')
                print(nomeprincipal)
                resposta = bancodedados.selecionarHierarquia(idhierqprincipal)
                
                hieraqDescritores = []
                for item in resposta:
                        id = item[0]
                        nome = bancodedados.nomeDoDescritor(id)
                        hieraqDescritores.append(nome)
                print('hieraqDescritores')
                print(hieraqDescritores)
                
                termosDeEntrada = []
                entradas = bancodedados.selectionarTermosDeEntrada(iddescritor)
                for ent in entradas:
                        termosDeEntrada.append(ent[0])
                print('termosDeEntrada')
                print(termosDeEntrada)

        
if __name__ == "__main__":
    main()
