# leitura de sistema de arquivo
import os
# extracao do texto em pdf para raw
import tika
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

# Extract text from PDF
def extraiPDF(f):
    resultado = []
    tika.TikaClientOnly = True
    raw = parser.from_file(f)
    metadados = raw["metadata"]
    conteudo  = raw["content"] 
    #conteudo  = str(conteudo)
    #conteudo  = conteudo.encode('utf-8', errors='ignore')
    conteudo  = (conteudo).replace('\n', '').replace('\r\n', '').replace('\r', '').replace('\\', '').replace('\t', ' ')
    resultado.append(conteudo)
    resultado.append(metadados)
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

def montaDocJSONporTexto(hs, tx, mt):
    descricao = tx
    if "dc:description" in mt:
        assunto = mt["dc:description"]
    if "dc:title" in mt:
        titulo = mt["dc:title"]
    if "dc:creator" in mt:
        autores = mt["dc:creator"]
    if "date" in mt:
        data = mt["date"]
    
    estrutura = { 	"dcIdentifier" : hs,
                    "dcDate" : data,
                    "dcLanguage" : "en",
                    "dcCreator" : autores, 
                    "dcTitle" : titulo,
                    "dcSubject" : assunto,
                    "dcDescription" : descricao }
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

