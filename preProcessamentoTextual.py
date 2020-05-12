# leitura de sistema de arquivo
import os
# extracao do texto em pdf para texto puro
from tika import parser
# Tkinter help the dialog box
import tkinter as tk
from tkinter import filedialog
# Fnmatch help to filter files with mask
import fnmatch
# hash creation
import hashlib
import json
# trocar aspas simples para duplas no JSON
import ast
# limpar o texto com diversas funcoes
from cleantext import clean
# limpar o texto com caracteres especiais
from string import punctuation
# limpar o texto de espaços repetidos com regex
import re
# extracao do texto em pdf para texto puro 
import fitz

# Calcula o hash de uma arquivo
def calc_hash(f):
    BLOCKSIZE = 65536
    hasher = hashlib.sha1()
    with open(f, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
    return hasher.hexdigest()

def extraiPDFtika(arq):
    """Extrai texto do arquivo PDF usando a biblioteca tika-python

    Arguments:
        arq {str} -- Arquivo PDF com path

    Returns:
        list -- Um array contendo duas posicoes: conteudo(texto) e metadados
    """    
    resultado = []
    #tika.TikaClientOnly = True
    raw = parser.from_file(arq)
    metadados = raw["metadata"]
    conteudo  = raw["content"] 
    #conteudo  = conteudo.encode('utf-8', errors='ignore')
    conteudo  = (conteudo).replace('\n', '').replace('\r\n', '').replace('\r', '').replace('\\', '').replace('\t', ' ')
    resultado.append(conteudo)
    resultado.append(metadados)
    return resultado

def extraiPDFpyMuPdf(arq):
    """Extrai texto do arquivo PDF usando a biblioteca pyMuPdf

    Arguments:
        arq {str} -- Arquivo PDF com path

    Returns:
        list -- Um array contendo duas posicoes: conteudo(texto) e metadados
    """    
    resultado = [] 
    raw = fitz.open(arq)
    textoCompleto = ""
    for page in raw:
        texto = page.getText("text")
        textoCompleto = textoCompleto + texto
    
    resultado.append(textoCompleto)
    resultado.append(raw.metadata)
    return resultado

# List of files from directory choosed 
def list_PDFs(dir):
    lstarquivos = []
    lstarquivos.clear()
    for file in os.listdir(dir):
        if fnmatch.fnmatch(file, '*.pdf'):
            lstarquivos.append(os.path.join(dir, file))
    return lstarquivos

def lista_sem_duplicidade2(lista):
    return set(lista)

def montaDocJSONporTextoUsingTika(chavehash, corpo, metadados, nomeArq):
    """Organiza os dados no formato JSON, apropriado para a gravacao no elasticSearch

    Arguments:
        chavehash {str} -- codigo hash em sha1
        corpo {str} -- texto do corpo do artigo
        metadados {dict} -- dicionario com os metadados do artigo
        nomeArq {str} -- nome do arquivo pdf

    Returns:
        str -- uma string em formato JSON
    """    
    descricao = ''
    assunto = ''
    titulo = ''
    autores = ''
    data = ''
    idioma = ''
    formato = ''
    palavraChave = ''
    if "dc:description" in metadados:
        descricao = metadados["dc:description"]
    if "dc:subject" in metadados:
        assunto = metadados["dc:subject"]
    if "dc:title" in metadados:
        titulo = metadados["dc:title"]
    if "dc:creator" in metadados:
        autores = metadados["dc:creator"]
    if "date" in metadados:
        data = metadados["date"]
    if "dc:language" in metadados:
        idioma = metadados["dc:language"]
    if "Content-Type" in metadados:
        formato = metadados["Content-Type"]
    if "Keywords" in metadados:
        palavraChave = metadados["Keywords"]
    estrutura = { 	"dcIdentifier" : chavehash,
                    "dcDate" : data,
                    "dcLanguage" : idioma,
                    "dcCreator" : autores, 
                    "dcTitle" : titulo,
                    "dcSubject" : assunto,
                    "dcDescription" : descricao,
                    "dcSource" : nomeArq,
                    "dcFormat" :  formato,
                    "keywords" :  palavraChave,
                    "textBody" : corpo}
    return estrutura 

def montaDocJSONporTextoUsingPyMuPdf(chavehash, corpo, metadados, nomeArq):
    """Organiza os dados no formato JSON, apropriado para a gravacao no elasticSearch

    Arguments:
        chavehash {str} -- codigo hash em sha1
        corpo {str} -- texto do corpo do artigo
        metadados {dict} -- dicionario com os metadados do artigo
        nomeArq {str} -- nome do arquivo pdf

    Returns:
        str -- uma string em formato JSON
    """    
    data = ''
    autores = ''
    titulo = ''
    assunto = ''
    descricao = ''
    idioma = 'en'
    formato = ''
    palavraChave = ''
    if "creationDate" in metadados:
        data = metadados["creationDate"]
    if "author" in metadados:
        autores = metadados["author"]
    if "title" in metadados:
        titulo = metadados["title"]
    if "subject" in metadados:
        assunto = metadados["subject"]
    if "dc:description" in metadados:
        descricao = metadados["dc:description"]
    if "format" in metadados:
        formato = metadados["format"]
    if "Keywords" in metadados:
        palavraChave = metadados["keywords"]
    estrutura = { 	"dcIdentifier" : chavehash,
                    "dcDate" : data,
                    "dcLanguage" : idioma,
                    "dcCreator" : autores, 
                    "dcTitle" : titulo,
                    "dcSubject" : assunto,
                    "dcDescription" : descricao,
                    "dcSource" : nomeArq,
                    "dcFormat" :  formato,
                    "keywords" :  palavraChave,
                    "textBody" : corpo}
    return estrutura 

def cleanEspecialsChars(text):
    for c in punctuation:
        text = text.replace(c, ' ')
    text = text.replace('–', '').replace('•', '') #caracter especial que nao eh o hifen e nao estah em punctuation 
    #text = text.replace('(', ' ').replace(')', '').replace('[', '').replace(']', '').replace('<', '').replace('>', '').replace('+', '').replace('=', '')   
    return text 

def cleanLineBreaks(text):
    text = text.replace('\n', ' ')
    return text

def cleanStopWords(text): 
	text = text.replace('fig', '') 
	return text 

# O tratamento do hifen deve ser feito por ultimo. Apos a quebra de linha.
def cleanHifen(text):
	text = text.replace('- ', ' ')
	return text

def cleanURLText(text):
    resp = clean(text,
    fix_unicode=True,               # fix various unicode errors
    to_ascii=True,                  # transliterate to closest ASCII representation
    lower=True,                     # lowercase text
    no_line_breaks=False,           # fully strip line breaks as opposed to only normalizing them
    no_urls=True,                   # replace all URLs with a special token
    no_emails=True,                 # replace all email addresses with a special token
    no_phone_numbers=True,          # replace all phone numbers with a special token
    no_numbers=True,                # replace all numbers with a special token
    no_digits=True,                 # replace all digits with a special token
    no_currency_symbols=True,       # replace all currency symbols with a special token
    no_punct=False,                 # fully remove punctuation
    replace_with_url=" ",
    replace_with_email=" ",
    replace_with_phone_number=" ",
    replace_with_number=" ",
    replace_with_digit="",
    replace_with_currency_symbol=" ",
    lang="en"                       # set to 'de' for German special handling
    )
    return resp

def cleanMoreSpaces(text):
    resp = re.sub("[ ]{2,}", " ", text)
    return resp

def limparTudo(text):
    #na verdade alguns termos que podem ser suprimidos como fig, image
    text =  cleanStopWords(text)
    #a exclusão de quebra de linha afeta os termos hifenizados
    text =  cleanLineBreaks(text)
    #na sequencia da quebra de linha, devem ser tratados os hifens 
    text =  cleanHifen(text)
    #antes de retirar os caracteres especiais, deve ser tratado o clean para urls, emails, etc.. 
    text =  cleanURLText(text)
    #apos retirar urls deve se tratar os numeros e caracteres especiais: pontuacao, etc...
    text =  cleanEspecialsChars(text)
    #retirar o exceço de espaçoes entre os termos
    text =  cleanMoreSpaces(text)
    return text

def test():
    files = []
    #files.append('/home/eduardo/Documentos/workspace/ArtigosBMC/s12877-019-1235-7.pdf')
    #files.append('/home/eduardo/Documentos/workspace/ArtigosBMC/s12877-019-1238-4.pdf')
	#files.append('/home/eduardo/Documentos/workspace/ArtigosBMC/s12877-019-1248-2.pdf')
    files.append('/home/eduardo/Documentos/workspace/ArtigosBMC/1-s2.0-S0167739X15001028-main.pdf')

    for f in files:
		# Parse data from file
        file_data = parser.from_file(f)
        # Get files text content
        text = file_data['content']
        print('------------------')
        #na verdade alguns termos que podem ser suprimidos como fig, image
        text = cleanStopWords(text)
        #a exclusão de quebra de linha afeta os termos hifenizados
        text = cleanLineBreaks(text)
        #na sequencia da quebra de linha, devem ser tratados os hifens 
        text = cleanHifen(text)
        #antes de retirar os caracteres especiais, deve ser tratado o clean para urls, emails, etc.. 
        text = cleanURLText(text)
        #apos retirar urls deve se tratar os numeros e caracteres especiais: pontuacao, etc...
        text = cleanEspecialsChars(text)
        text = cleanMoreSpaces(text)
        print(text)
        print('++++++++++++++++++')

def suprimirSubstringComLimitadores(text, ini, fim):
    """ Recebe um texto e simbolos como  () [] para retornar apenas o texto FORA dos delimitadores

        Args: 
            param1 (str): todo o texto a ser tratado
            param2 (str): simbolo inicial -> ( [ {
            param3 (str): simbolo final -> ) ] }

        Returns:
            str: string fora dos delimitadores
    """
    if not text:
        return 'V A Z I O'
    if (ini == fim):
        posini = text.find(ini)
        posfim = text.rfind(fim, posini+1)
    else:
        posini = text.find(ini)
        posfim = text.rfind(fim)
    if (posini < 0 or posfim < 0):
        return text.strip()
    elif ((posini >= 0 and posfim >= 0) and (posfim+1 > posini)):
        text = text[:posini] + text[posfim+1:]
        return suprimirSubstringComLimitadores(text, ini, fim)
    else: 
        return text

def suprimirHifenInicioeFim(text):
    """ Recebe um texto com hifen e retorna o mesmo texto sem o simbolo

        Args:
            param1 (str): texto com hifen nas extremidades

        Returns:
            str: texto sem hifen nas extremidades
    """
    if not (text.startswith('-') or text.endswith('-')):
        return text.strip()
    elif text.startswith('-'):
        text = text[1:]
        return suprimirHifenInicioeFim(text)
    elif text.endswith('-'):
        text = text[0:len(text)-1]
        return suprimirHifenInicioeFim(text)

def trataDescricao(text): 
    """ Recebe a descricao do conceito e chama outras funcoes para tratar detalhes como:
        excesso de espacoes, caracteres especiais, separadores, etc. 

        Args: 
            param1 (str): texto da descricao do termo 

        Returns: 
            str: texto da descricao do termo tratado 
    """
    #selecionar apenas o primeiro termo da lista com separador virgula 
    resp = ''
    listastr = text.split(',')
    i = 0
    for it in listastr:
        if not (it.isnumeric()):
            resp = listastr[i].strip()
            break
        i+=1

    #suprimir palavras entre parenteses, entre ^^, entre ><, entre [] 
    resp = suprimirSubstringComLimitadores(resp, '(', ')')
    resp = suprimirSubstringComLimitadores(resp, '[', ']')
    resp = suprimirSubstringComLimitadores(resp, '>', '<')
    resp = suprimirSubstringComLimitadores(resp, '^', '^')

    #suprimir caracteres numericos 
    resp = ''.join(i for i in resp if not i.isdigit())

    #suprimir termos particulares (-RETIRED-  ; NOS; O/E) 
    #replace('mm', '') nao pode pq afetou palavras que possuem mm
    resp = resp.replace('-RETIRED-', '').replace('NOS', '').replace('O/E', '').replace('&/or', '')

    #tratamento de caracteres especiais (^, <, >, :, ',', ';', &, '/', '%') [exceto hifen]
    #suprimir a palavra que contem esses caracteres? 
    resp = resp.replace('#', '').replace('%', '').replace('\'-', '').replace('/', '').replace('\'', '').replace('\"', '').replace('^', '').replace('<', '').replace('>', '').replace(':', '').replace(',', '').replace(';', '').replace('&', '').replace('(', '').replace(')', '').replace('*', '').replace('.', '').replace('-', '').replace('?', '').replace('+', '').replace('|', '')

    #retirar dois ou mais espacos em sequencia e dois ou mais hifens
    resp = re.sub("[ ]{2,}", " ", resp)
    resp = re.sub("[-]{2,}", " ", resp)

    #retirar espacos antes e apos (trimmer)
    resp = resp.strip()

    #termo nao pode comecar ou terminar com hifen ou caracter especial e nao pode ter dois ou mais hifens juntos 
    resp = suprimirHifenInicioeFim(resp)

    #apos todas as regras, validar se ha string vazia como resultado! 
    if not resp:
        return text
    else:
        return resp

def dateToTimeString(dt):
        """ Ao receber uma data sem separador, formata como yyyy-mm-dd para compatibilidade com sqLite
            
            Args:
                param1 (str): data a ser formatada (20170731)

            Returns: 
                str: data formatada (2017-07-31)
        """
        resp = dt[:4] + '-' + dt[4:6] + '-' + dt[6:]
        return resp