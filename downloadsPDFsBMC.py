import requests 
from requests.exceptions import HTTPError
import time 
from bs4 import BeautifulSoup
import os
import random

quantPaginas = 50
urlBasePagina = 'https://bmcgeriatr.biomedcentral.com/articles?searchType=journalSearch&sort=PubDate&page='
urlBasePdf = 'https://bmcgeriatr.biomedcentral.com/track/pdf/'

def gravarArquivo(texto, arq):
    f = open(arq, "w+")
    f.write(texto)
    f.close()

def abrirArquivo(arq):
    f = open(arq, "r")
    if f.mode == "r":
        texto = f.read()
    return texto

#retorna um array com os arquivos html
def listarPDFs():
    path = os.getcwd()
    files = [] 
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if '.html' in file:
                files.append(os.path.join(r, file))
    return files

def extrairURLSdeArqsLocais():
    arquivos = listarPDFs()
    links = []
    for arq in arquivos:
        texto = abrirArquivo(arq)
        soup = BeautifulSoup(texto, 'html.parser')
        listahref = soup.find_all('a')
        for item in listahref:
            endereco = item.get('data-track-label')
            if endereco:
                links.append(endereco)
    resp = set(links)
    return resp

def downloadPaginas():
    urls = {}
    for i in range(1, quantPaginas, 1):
        urls[i] = urlBasePagina + str(i)
    
    paginas = {}
    try:
        for i in range(1, quantPaginas, 1):
            r = requests.get(urls[i])
            time.sleep(5)
            if (r.status_code == 200):
                paginas[i] = r.text
            else:
                print('Erro no download da página: ' + r.url)
    except HTTPError as http_err:
        print(f'HTTP error: {http_err}')  # Python 3.6
    except Exception as err:
        print(f'Other error: {err}')  # Python 3.6
    else:
        print('Sem exceção na página: ' + r.url)
    
    for p in paginas:
        gravarArquivo(paginas[p], str(p) + ".html")

def downloadPdfs(lista):
    for l in lista:
        tempo = random.randint(1,31)
        time.sleep(tempo)
        urlCompleta = urlBasePdf + l
        print('Delay: ' + str(tempo) + ' | ' + urlCompleta)
        try:
            nomeArq = l.partition("/")[2] + ".pdf"
            if not os.path.exists(nomeArq):
                r = requests.get(urlCompleta)
                if (r.status_code == 200): 
                    with open(nomeArq, 'wb') as f:
                        f.write(r.content)
                else: 
                    print("Erro na url: " + l.url) 
        except HTTPError as http_err:
            print(f'HTTP error: {http_err}')  # Python 3.6
        except Exception as err:
            print(f'Other error: {err}')  # Python 3.6
        else:
            print('Arquivo gravado: ' + nomeArq)

if __name__ == "__main__":
    urlsPdfs = extrairURLSdeArqsLocais()
    downloadPdfs(urlsPdfs)

