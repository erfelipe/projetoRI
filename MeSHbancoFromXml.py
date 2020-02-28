import xml.etree.ElementTree as et
import re
import MeSHutils as utils
import constantes

from MeSHbancoEstrutura import BDMeSH
from MeSHdescritor import Descritor

def normalizaTreeNumberList(NList):
    numberList = re.sub('[.]', '', NList)
    return numberList

def validaConceptList(cList):
    if (cList.find(",") > -1):
        return False
    else:
        return True    

def main():
    tree = et.parse(constantes.TERMINOLOGIA_MESH_XML)  #desc2019-resumo.xml 
    raiz = tree.getroot() 
    lang = raiz.get("LanguageCode")
    
    for descRecords in raiz.findall("DescriptorRecord"):
        descUI = descRecords.findall("DescriptorUI")
        dr = descUI[0].text      

        descName = descRecords.findall("DescriptorName")
        nome = descName[0].findall("String")
        nomeDescritor = nome[0].text

        numerosHierarquicos = []
        treeList = descRecords.findall("TreeNumberList")
        for tList in treeList:
            tnumber = tList.findall("TreeNumber")
            for tn in tnumber:
                numerosHierarquicos.append(normalizaTreeNumberList(tn.text))

        conceitosRelacionados = []
        conceptList = descRecords.findall("ConceptList")
        for cl in conceptList:
            concept = cl.findall("Concept")
            for cp in concept:
                termList = cp.findall("TermList")
                for teList in termList:
                    term = teList.findall('Term')
                    for termo in term:
                        ui = termo.findall('TermUI')
                        nome = termo.findall('String')
                        if validaConceptList(nome[0].text):
                            conjunto = {}
                            conjunto[ui[0].text] = utils.textoNormalizado(nome[0].text)
                            conceitosRelacionados.append(conjunto)
        
        desc = Descritor(dr, nomeDescritor, numerosHierarquicos, conceitosRelacionados, lang) 
        
        bancoMESH = BDMeSH(constantes.BD_SQL_MESH)
        with bancoMESH:
            bancoMESH.inserirNoBancoDeDados(desc)
            print(desc.iddesc)

if __name__ == "__main__":
    main()
