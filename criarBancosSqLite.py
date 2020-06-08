import estatisticaBancoEstrutura
from MeSHbancoEstrutura import BDMeSH
import MeSHbancoFromXml
from estatisticaBancoEstrutura import BDestatistica 
from snomedRF2bancoEstrutura import BDSnomed
import snomedRF2toDB
import constantes
import os

def criarBancoEstatistica():
    """ Exclui e cria novo banco sqLite para armazenar as estatisticas das consultas expandidas
    """    
    resp = input("Criar sqLite ESTATISTICA? (S/N): ")
    if ((resp == 'S') or (resp == 's')):
        if os.path.exists(constantes.BD_SQL_ESTATISTICA):
            os.remove(constantes.BD_SQL_ESTATISTICA)        
        banco = BDestatistica(constantes.BD_SQL_ESTATISTICA)
        with banco:
            banco.criarBancoDeDados()

def criarBancoSNOMED():
    """ Exclui e cria novo banco sqLite para armazenar a terminologia SNOMED em formato relacional
    """    
    resp = input("Criar sqLite SNOMED CT? (S/N): ")
    if ((resp == 'S') or (resp == 's')):
        resp = input("EXCLUIR arq sqLite SNOMED CT? (S/N): ")
        if ((resp == 'S') or (resp == 's')):
            if os.path.exists(constantes.BD_SQL_SNOMED):
                os.remove(constantes.BD_SQL_SNOMED)
        bancoSnomed = BDSnomed(constantes.BD_SQL_SNOMED)
        with bancoSnomed:
            bancoSnomed.criarBancoDeDados() 
        snomedRF2toDB.importaRF2ParaSqliteSNOMED()

def criarBancoMeSH(): 
    """ Exclui e cria novo banco sqLite para armazenar a terminologia MeSH em formato relacional
    """    
    respCriar = input("Criar sqLite MESH? (S/N): ")
    if ((respCriar == 'S') or (respCriar == 's')):
        respExcluir = input("EXCLUIR arq sqLite MESH? (S/N): ")
        if ((respExcluir == 'S') or (respExcluir == 's')):
            if os.path.exists(constantes.BD_SQL_MESH):
                os.remove(constantes.BD_SQL_MESH)
        bancoMESH = BDMeSH(constantes.BD_SQL_MESH)
        with bancoMESH:
            if ((respExcluir == 'S') or (respExcluir == 's')):
                bancoMESH.criarBancoDeDados() 
            MeSHbancoFromXml.main()

if __name__ == "__main__":
    criarBancoEstatistica()
    criarBancoSNOMED()
    criarBancoMeSH()
