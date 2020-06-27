import csv
import snomedRF2bancoEstrutura as banco
import constantes
import glob
import ntpath

def processarArquivoRF2(arquivo, tabela):
    """ Realiza a leitura do arquivo SNOMED no formato textual RF2 para processamento e gravacao no formato relacional
    
    Arguments:
        arquivo {str} -- Nome completo do arquivo com seu caminho (path)
        tabela {str} -- Nome da tabela a ser processada 
    """    
    bancoDeDados = banco.BDSnomed(constantes.BD_SQL_SNOMED)
    with open(arquivo) as arq:
        conteudo = csv.reader(arq, delimiter='\t')
        with bancoDeDados:
            if tabela == 'concept':
                for linha in conteudo:
                    bancoDeDados.inserirConcept(linha)
            elif tabela == 'description':
                for linha in conteudo:
                    bancoDeDados.inserirDescription(linha)
            elif tabela == 'relationship':
                print("Tabela relationship não precisa ser importada")
                # for linha in conteudo:
                #     bancoDeDados.inserirRelationShip(linha)
            elif tabela == 'srefset':
                for linha in conteudo:
                    bancoDeDados.inserirSrefSet(linha)
            elif tabela == 'statedrelationship':
                for linha in conteudo:
                    bancoDeDados.inserirStatedRelationShip(linha)
            elif tabela == 'textdefinition':
                print("Tabela textdefinition não precisa ser importada")
                # for linha in conteudo:
                #     bancoDeDados.inserirTextDefinition(linha)

def listaArquivosEmDiretorio(mascara):
    """ A partir de uma mascara (*.txt) por exemplo, retorna uma lista com os arquivos
    
    Arguments:
        mascara {str} -- Mascara que vai filtrar os arquivos
    
    Returns:
        array -- Lista com os arquivos encontrados e filtrados pela mascara
    """    
    arqs = []
    for arq in glob.glob(mascara):
        arqs.append(arq)
    return arqs

def tematicaDoArquivoSnomed(arquivo):
    """ Dado um nome de arquivo, retorna sua segunda parte separada pelo delimitador underline
    
    Arguments:
        arquivo {str} -- Exemplo: sct2_Concept_Snapshot_INT_20190731.txt
    
    Returns:
        str -- A segunda parte do nome, neste exemplo 'concept'
    """
    partes = arquivo.split('_') 
    return partes[1].lower()

def importaRF2ParaSqliteSNOMED():
    """ Permite definir os arquivos e seu caminho para processamento e gravacao no modelo relacional
    """    
    SNOMEDbaseDir = constantes.TERMINOLOGIA_SNOMED_PATH 
    arquivos = listaArquivosEmDiretorio(SNOMEDbaseDir + '*.txt')
    for arq in arquivos:
        tematica = tematicaDoArquivoSnomed(ntpath.basename(arq))
        processarArquivoRF2(arq, tematica)

if __name__ == "__main__":
    importaRF2ParaSqliteSNOMED()
