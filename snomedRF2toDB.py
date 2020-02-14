import csv
import snomedRF2bancoEstrutura as banco
import constantes

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
                for linha in conteudo:
                    bancoDeDados.inserirRelationShip(linha)
            elif tabela == 'srefset':
                for linha in conteudo:
                    bancoDeDados.inserirSrefSet(linha)
            elif tabela == 'statedrelationship':
                for linha in conteudo:
                    bancoDeDados.inserirStatedRelationShip(linha)
            elif tabela == 'textdefinition':
                for linha in conteudo:
                    bancoDeDados.inserirTextDefinition(linha)

def importaRF2ParaSqliteSNOMED():
    """ Permite definir os arquivos e seu caminho para processamento e gravacao no modelo relacional
    """    
    SNOMEDbaseDir = constantes.PATH_SNOMED_TERMINOLOGY 
    processarArquivoRF2(SNOMEDbaseDir+"sct2_Concept_Snapshot_INT_20190731.txt", 'concept')
    processarArquivoRF2(SNOMEDbaseDir+"sct2_Description_Snapshot-en_INT_20190731.txt", 'description')
    processarArquivoRF2(SNOMEDbaseDir+"sct2_Relationship_Snapshot_INT_20190731.txt", 'relationship')
    processarArquivoRF2(SNOMEDbaseDir+"sct2_sRefset_OWLExpressionSnapshot_INT_20190731.txt", 'srefset')
    processarArquivoRF2(SNOMEDbaseDir+"sct2_StatedRelationship_Snapshot_INT_20190731.txt", 'statedrelationship')
    processarArquivoRF2(SNOMEDbaseDir+"sct2_TextDefinition_Snapshot-en_INT_20190731.txt", 'textdefinition')

if __name__ == "__main__":
    importaRF2ParaSqliteSNOMED()