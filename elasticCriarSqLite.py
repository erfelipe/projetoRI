from elasticBancoEstrutura import BDelastic 
import constantes


if __name__ == "__main__":
    bancoElastic = BDelastic(constantes.BD_SQL_ELASTIC)
    with bancoElastic:
        bancoElastic.criarBancoDeDados()

