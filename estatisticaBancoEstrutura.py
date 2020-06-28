import sqlite3
class BDestatistica:

    def __init__(self, nameDB):
        self.nameDB = nameDB
        self.conn = None
        self.cursor = None

    def __enter__(self):
        self.conn = sqlite3.connect(self.nameDB)
        self.cursor = self.conn.cursor()

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.conn.commit()
        self.conn.close() 

    def criarBancoDeDados(self):  
        """ Executa comandos para criacao da estrutura do banco de dados
        """
        self.cursor.execute(""" CREATE TABLE if not exists estatistica(
                                idEstatistica integer primary key autoincrement,
                                termo text not null,
                                totalUnigramas integer not null,
                                totalArtigosMesh integer not null,
                                totalTermosPesquisadosMesh integer not null,
                                totalTermosPesquisadosComRevocacaoMesh integer not null,
                                totalArtigosUnicosMesh integer not null, 
                                totalArtigosRepetidosMesh integer not null,
                                totalArtigosSnomed integer not null,
                                totalTermosPesquisadosSnomed integer not null,
                                totalTermosPesquisadosComRevocacaoSnomed integer not null,
                                totalArtigosUnicosSnomed integer not null,
                                totalArtigosRepetidosSnomed integer not null,
                                totalArtigosComuns integer not null,
                                totalTermosComuns integer not null
                                );
                            """) 

        self.cursor.execute(""" CREATE TABLE if not exists termosAssociados(
                                idAssociado integer primary key autoincrement, 
                                idEstatistica integer not null, 
                                terminologia char(1) not null, 
                                tipoTermo char(1) not null, 
                                termo text not null,
                                totalUnigramas integer not null
                                ); 
                            """)
        
    def insereEstatistica(self, termo, totalUnigramas, totalArtigosMesh, totalTermosPesquisadosMesh, totalTermosPesquisadosComRevocacaoMesh, totalArtigosUnicosMesh, totalArtigosRepetidosMesh, totalArtigosSnomed, totalTermosPesquisadosSnomed, totalTermosPesquisadosComRevocacaoSnomed, totalArtigosUnicosSnomed, totalArtigosRepetidosSnomed, totalArtigosComuns, totalTermosComuns):
        """ Dados da estatistica de uma determinada pesquisa pelo termo principal 

        Args:
            termo (str): Termo que encabeca a pesquisa expandida 
            totalUnigramas (int): Numero de palavras que compoem este termo
            totalArtigosMesh (int): Numero de artigos recuperados para terminologia MeSH
            totalTermosPesquisadosMesh (int): Numero total de termos para consulta expandida
            totalTermosPesquisadosComRevocacaoMesh (int): Numero de termos com revocacao
            totalArtigosUnicosMesh (int): Numero de artigos que nao se repetem para os diversos termos pesquisados
            totalArtigosRepetidosMesh (int): Numero de artigos que se repetem para os diversos termos pesquisados
            totalArtigosSnomed (int): Numero de artigos recuperados para a terminologia SNOMED CT 
            totalTermosPesquisadosSnomed (int): Numero total de termos para consulta expandida
            totalTermosPesquisadosComRevocacaoSnomed (int): Numero de termos com revocacao
            totalArtigosUnicosSnomed (int): Numero de artigos que nao se repetem para os diversos termos pesquisados
            totalArtigosRepetidosSnomed (int): Numero de artigos que se repetem para os diversos termos pesquisados
            totalArtigosComuns (int): Numero de artigos que aparecem em revocacao para ambas terminologias
            totalTermosComuns (int): Numero de termos comuns em ambas terminologias no processo de pesquisa 

        Returns:
            int: Codigo da chave primaria criada na insercao
        """
        self.cursor.execute(""" INSERT INTO estatistica (termo, totalUnigramas, totalArtigosMesh, totalTermosPesquisadosMesh, totalTermosPesquisadosComRevocacaoMesh, totalArtigosUnicosMesh, totalArtigosRepetidosMesh, totalArtigosSnomed, totalTermosPesquisadosSnomed, totalTermosPesquisadosComRevocacaoSnomed, totalArtigosUnicosSnomed, totalArtigosRepetidosSnomed, totalArtigosComuns, totalTermosComuns) 
                                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?) """, (termo, totalUnigramas, totalArtigosMesh, totalTermosPesquisadosMesh, totalTermosPesquisadosComRevocacaoMesh, totalArtigosUnicosMesh, totalArtigosRepetidosMesh, totalArtigosSnomed, totalTermosPesquisadosSnomed, totalTermosPesquisadosComRevocacaoSnomed, totalArtigosUnicosSnomed, totalArtigosRepetidosSnomed, totalArtigosComuns, totalTermosComuns, ))
        return self.cursor.lastrowid

    def insereTermosAssociados(self, idEstatistica, terminologia, tipoTermo, termo, totalUnigramas):
        """ Para cada estatistica, varios termos estao associados

        Args:
            idEstatistica (integer): Chave estrangeira para a tabela estatistica
            terminologia (str): "M" = MeSH, "S" = SNOMED
            tipoTermo (str): "P" = Pesquisado, "R" = Obteve revocacao
            termo (str): Termo usado nesta pesquisa 
            totalUnigramas (int): Numero de palavras que compoem este termo
        """
        self.cursor.execute(""" INSERT INTO termosAssociados (idEstatistica, terminologia, tipoTermo, termo, totalUnigramas) 
                                VALUES (?, ?, ?, ?, ?) """, (idEstatistica, terminologia, tipoTermo, termo, totalUnigramas,))

