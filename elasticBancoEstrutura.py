import sqlite3
class BDelastic:

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
            Args: 
                self: referencia a propria classe

                TABELA termoProcurado
                idTproc - Identificador de registro da tabela (chave primaria)
                instrumento - S = SNOMED e M = MeSH
                tipoTermo - O = original e T = tratado
                termoProc - Termo procurado
                quantTermoProcurado - Quantidade (revocação) deste termo encontrado no acervo
                idDescritor - Codigo identificador do termo 
                descritorPrincipal - Termo considerado como principal na estrutura do instrumento
                quantDescritorPrincipal - Quantidade deste termo encontrado no acervo 

                TABELA termoAssociado
                idTassoc - Identificador de registro da tabela (chave primaria)
                idTproc - Identificador de registro da tabela termoProcurado (chave estrangeira)
                termo - Termo associado ao termo principal 
                quantTermo - Quantidade (revocação) deste termo
                tipoTermo - C = conceitual e H = hierarquico

            Returns: 
                none: sem retorno
        """ 
        self.cursor.execute(""" CREATE TABLE if not exists termoProcurado(
                                idTproc integer primary key autoincrement,
                                instrumento char(1),
                                tipoTermo char(1) not null,
                                termoProc text not null,
                                quantTermoProcurado integer not null,
                                idDescritor text not null, 
                                descritorPrincipal text not null, 
                                quantDescritorPrincipal integer not null
                                );
                            """) 

        self.cursor.execute(""" CREATE TABLE if not exists termoAssociado(
                                idTassoc integer primary key autoincrement, 
                                idTproc integer not null, 
                                termo text not null, 
                                quantTermo integer not null, 
                                tipoTermo char(1) not null
                                ); 
                            """)
        
    def insereTermoProcurado(self, instrumento, tipoTermo, termoProc, quantTermoProcurado, idDescritor, descritorPrincipal, quantDescritorPrincipal ):
        """ Insere informacoes na tabela termoProcurado
        
        Arguments:
            instrumento {str} -- S = SNOMED e M = MeSH
            tipoTermo {str} -- O = original e T = tratado
            termoProc {str} -- Termo procurado (da lista previamente selecionada por determinado criterio)
            quantTermoProcurado {integer} -- Numero de documentos encontrados (revocacao)
            idDescritor {str} -- Codigo identificador do termo 
            descritorPrincipal {str} -- Termo principal, quando encontrado 
            quantDescritorPrincipal {integer} -- Numero de documentos encontrados (revocacao) 
        
        Returns:
            integer -- chave primaria 
        """        
        self.cursor.execute(""" INSERT INTO termoProcurado (instrumento, tipoTermo, termoProc, quantTermoProcurado, idDescritor, descritorPrincipal, quantDescritorPrincipal) 
                                VALUES (?, ?, ?, ?, ?, ?, ?) """, (instrumento, tipoTermo, termoProc, quantTermoProcurado, idDescritor, descritorPrincipal, quantDescritorPrincipal, ))
        return self.cursor.lastrowid

    def insereTermoAssociado(self, idTproc, termo, quantTermo, tipoTermo):
        """ Insere informacoes na tabela termoAssociado 
        
        Arguments:
            idTproc {integer} -- Identificador de registro da tabela termoProcurado (chave estrangeira)
            termo {str} -- Termo associado ao termo principal 
            quantTermo {integer} -- Quantidade (revocação) deste termo
            tipoTermo {str} -- C = conceitual e H = hierarquico
        """        
        self.cursor.execute(""" INSERT INTO termoAssociado (idTproc, termo, quantTermo, tipoTermo) 
                                VALUES (?, ?, ?, ?) """, (idTproc, termo, quantTermo, tipoTermo, ))

