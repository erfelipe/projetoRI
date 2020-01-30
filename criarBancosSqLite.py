from elasticBancoEstrutura import BDelastic 
from MeSHbancoEstrutura import BDMeSH
import MeSHbancoFromXml 
import constantes
import os

if __name__ == "__main__":
    resp = input("Criar sqLite ELASTIC? (S/N): ")
    if ((resp == 'S') or (resp == 's')):
        if os.path.exists(constantes.BD_SQL_ELASTIC):
            os.remove(constantes.BD_SQL_ELASTIC)        
        bancoElastic = BDelastic(constantes.BD_SQL_ELASTIC)
        with bancoElastic:
            bancoElastic.criarBancoDeDados()

    resp = input("Criar sqLite MESH? (S/N): ")
    if ((resp == 'S') or (resp == 's')):
        if os.path.exists(constantes.BD_SQL_MESH):
            os.remove(constantes.BD_SQL_MESH)
        bancoMESH = BDMeSH(constantes.BD_SQL_MESH)
        with bancoMESH:
            bancoMESH.criarBancoDeDados() 
            MeSHbancoFromXml.main()
            

