import csv
import snomedRF2bancoEstrutura as banco
import os 
from pathlib import Path

bancoDeDados = banco.BD("db-snomed-RF2.sqlite3")
SNOMEDbaseDir = "/Users/eduardofelipe/Documents/Ontologias/SNOMED_CT/SnomedCT_InternationalRF2_PRODUCTION_20190731T120000Z/Full/Terminology/"

def main():
    arqSQL = Path("db-snomed-RF2.sqlite3")
    if arqSQL.exists():
        os.remove(arqSQL)
    with bancoDeDados:
       bancoDeDados.criarBancoDeDados()

def ProcessarArquivoRF2(arquivo, tabela):
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

if __name__ == "__main__":
    main()
    ProcessarArquivoRF2(SNOMEDbaseDir+"sct2_Concept_Full_INT_20190731.txt", 'concept')
    ProcessarArquivoRF2(SNOMEDbaseDir+"sct2_Description_Full-en_INT_20190731.txt", 'description')
    ProcessarArquivoRF2(SNOMEDbaseDir+"sct2_Relationship_Full_INT_20190731.txt", 'relationship')
    ProcessarArquivoRF2(SNOMEDbaseDir+"sct2_sRefset_OWLExpressionFull_INT_20190731.txt", 'srefset')
    ProcessarArquivoRF2(SNOMEDbaseDir+"sct2_StatedRelationship_Full_INT_20190731.txt", 'statedrelationship')
    ProcessarArquivoRF2(SNOMEDbaseDir+"sct2_TextDefinition_Full-en_INT_20190731.txt", 'textdefinition')

