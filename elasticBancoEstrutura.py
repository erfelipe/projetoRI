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
        self.cursor.execute(""" CREATE TABLE if not exists termoProcurado(
                                id integer primary key autoincrement,
                                instrumento char(1),
                                termoProc text not null,
                                quantTermoProcurado integer not null,
                                idDescritor text not null, 
                                descritorPrincipal text not null, 
                                quantDescritorPrincipal integer not null
                                );
                            """) 

        self.cursor.execute(""" CREATE TABLE if not exists termoAssociado(
                                idAssociado integer primary key autoincrement, 
                                id integer not null, 
                                termo text not null, 
                                quantTermo integer not null, 
                                tipoTermo char(1) not null
                                ); 
                            """)
        
    def insereTermoProcurado(self, instrumento, termoProc, quantTermoProcurado, idDescritor, descritorPrincipal, quantDescritorPrincipal ):
        self.cursor.execute(""" INSERT INTO termoProcurado (instrumento, termoProc, quantTermoProcurado, idDescritor, descritorPrincipal, quantDescritorPrincipal) 
                                VALUES (?, ?, ?, ?, ?, ?) """, (instrumento, termoProc, quantTermoProcurado, idDescritor, descritorPrincipal, quantDescritorPrincipal, ))
        return self.cursor.lastrowid

    def insereTermoAssociado(self, id, termo, quantTermo, tipoTermo):
        self.cursor.execute(""" INSERT INTO termoAssociado (id, termo, quantTermo, tipoTermo) 
                                VALUES (?, ?, ?, ?) """, (id, termo, quantTermo, tipoTermo, ))






