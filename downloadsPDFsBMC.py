# coding=UTF-8
import requests 
from requests.exceptions import HTTPError
import time 
from bs4 import BeautifulSoup
import os
import random

# preencher as constantes antes de seguir 
quantPaginas = 52
urlBasePagina = 'https://bmcgeriatr.biomedcentral.com/articles?searchType=journalSearch&sort=PubDate&page='
urlBasePdf = 'https://bmcgeriatr.biomedcentral.com/track/pdf/'
caminhoDestino = '/Volumes/SD-64-Interno/artigosPDFbmc/'

def gravarArquivo(texto, arq):
    f = open(os.path.join(caminhoDestino, arq), "w+")
    f.write(texto)
    print('Arquivo gravado: ' + arq)
    f.close()

def gravarArquivoPDF(texto, arq):
    f = open(os.path.join(caminhoDestino, arq), "wb")
    f.write(texto)
    print('PDF gravado: ' + arq)
    f.close()

def abrirArquivo(arq):
    f = open(os.path.join(caminhoDestino, arq), "r")
    if f.mode == "r":
        texto = f.read()
    else:
        texto = ""    
    return texto

#retorna um array com os arquivos html
def listarPDFs():
    # method getcwd() returns current working directory of a process
    #path = os.getcwd()
    path = caminhoDestino
    files = [] 
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.html' in file:
                files.append(os.path.join(r, file))
    print('Arquivos lidos: ' + str(files))
    return files

def extrairURLSdeArqsLocais():
    arquivos = listarPDFs()
    links = []
    for arq in arquivos:
        texto = abrirArquivo(arq)
        soup = BeautifulSoup(texto, 'html.parser')
        listahref = soup.find_all('a')
        os.remove(arq)
        for item in listahref:
            endereco = item.get('data-track-label')
            if endereco:
                links.append(endereco)
    resp = set(links)
    return resp

def downloadPaginasHTML():
    urls = {}
    for i in range(1, quantPaginas, 1):
        urls[i] = urlBasePagina + str(i)
    
    paginas = {}
    try:
        for i in range(1, quantPaginas, 1):
            r = requests.get(urls[i])
            time.sleep(3)
            if (r.status_code == 200):
                paginas[i] = r.text
            else:
                print('Erro no download da pagina: ' + r.url)
    except HTTPError as http_err:
        print(f'HTTP error: {http_err}')  # Python 3.6
    except Exception as err:
        print(f'Other error: {err}')  # Python 3.6
    else:
        print('Sem exceção na página: ' + r.url)
    
    for p in paginas:
        gravarArquivo(paginas[p], str(p) + ".html")

def arqExisteNoDestino(arq):
    pathCompleto = os.path.join(caminhoDestino, arq) 
    return os.path.exists(pathCompleto)

def downloadPdfs(lista):
    for l in lista:
        urlCompleta = urlBasePdf + l
        try:
            nomeArq = l.partition("/")[2] + ".pdf"
            if not arqExisteNoDestino(nomeArq):
                tempo = random.randint(1,21)
                print('Delay: ' + str(tempo) + ' | ' + urlCompleta)
                time.sleep(tempo)
                r = requests.get(urlCompleta)
                if (r.status_code == 200): 
                    gravarArquivoPDF(r.content, nomeArq)
                    # with open(nomeArq, 'wb') as f: #tirar a gravaçao daqui ... refatorar esse codigo
                    #     f.write(r.content)
                    #     print('Gravando conteúdo: ' + nomeArq)
                else: 
                    print("Erro na url: " + nomeArq) 
            else:
                print('Arquivo já existe no destino: ' + str(nomeArq))
        except HTTPError as http_err:
            print(f'HTTP error: {http_err}')  # Python 3.6
        except Exception as err:
            print(f'Other error: {err}')  # Python 3.6

if __name__ == "__main__":
    downloadPaginasHTML()
    urlsPdfs = extrairURLSdeArqsLocais()
    downloadPdfs(urlsPdfs)

