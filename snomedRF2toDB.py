import csv
import snomedRF2bancoEstrutura as banco

bancoDeDados = banco.BD("db-snomed-RF2.sqlite3")

def main():
    with bancoDeDados:
       bancoDeDados.criarBancoDeDados()

def ProcessarArquivoRF2(arquivo, tabela):
    with open(arquivo) as arq:
        conteudo = csv.reader(arq, delimiter='\t')
        for linha in conteudo:
            with bancoDeDados:
                bancoDeDados.inserirLinha(linha, tabela)

if __name__ == "__main__":
    main()
    ProcessarArquivoRF2('/home/eduardo/Documentos/workspace/snomed-Full/Terminology/sct2_Concept_Full_INT_20190731.txt', 'concept')
    ProcessarArquivoRF2('/home/eduardo/Documentos/workspace/snomed-Full/Terminology/sct2_Description_Full-en_INT_20190731.txt', 'description')
    ProcessarArquivoRF2('/home/eduardo/Documentos/workspace/snomed-Full/Terminology/sct2_Relationship_Full_INT_20190731.txt', 'relationship')
    ProcessarArquivoRF2('/home/eduardo/Documentos/workspace/snomed-Full/Terminology/sct2_sRefset_OWLExpressionFull_INT_20190731.txt', 'srefset')
    ProcessarArquivoRF2('/home/eduardo/Documentos/workspace/snomed-Full/Terminology/sct2_StatedRelationship_Full_INT_20190731.txt', 'statedrelationship')
    ProcessarArquivoRF2('/home/eduardo/Documentos/workspace/snomed-Full/Terminology/sct2_TextDefinition_Full-en_INT_20190731.txt', 'textdefinition')

