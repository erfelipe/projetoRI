import csv
import snomedRF2bancoEstrutura as banco
import os 
from pathlib import Path

bancoDeDados = banco.BDSnomed("/Volumes/SD-64-Interno/BancosSQL/db-snomed-RF2.sqlite3")
SNOMEDbaseDir = "/Volumes/SD-64-Interno/SnomedCT_InternationalRF2_PRODUCTION_20190731T120000Z/Snapshot/Terminology/" 

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
    ProcessarArquivoRF2(SNOMEDbaseDir+"sct2_Concept_Snapshot_INT_20190731.txt", 'concept')
    ProcessarArquivoRF2(SNOMEDbaseDir+"sct2_Description_Snapshot-en_INT_20190731.txt", 'description')
    ProcessarArquivoRF2(SNOMEDbaseDir+"sct2_Relationship_Snapshot_INT_20190731.txt", 'relationship')
    ProcessarArquivoRF2(SNOMEDbaseDir+"sct2_sRefset_OWLExpressionSnapshot_INT_20190731.txt", 'srefset')
    ProcessarArquivoRF2(SNOMEDbaseDir+"sct2_StatedRelationship_Snapshot_INT_20190731.txt", 'statedrelationship')
    ProcessarArquivoRF2(SNOMEDbaseDir+"sct2_TextDefinition_Snapshot-en_INT_20190731.txt", 'textdefinition')

