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
                                termoProc text not null,
                                quantTermoProcurado integer not null,
                                idDescritor text not null, 
                                descritorPrincipal text not null, 
                                quantDescritorPrincipal integer not null
                                );
                            """) 
        self.cursor.execute(""" CREATE TABLE if not exists termosAssociados(

                                );

        """)
        
    def insereTermoProcurado(self, termoProc, quantTermoProcurado, idDescritor, descritorPrincipal, quantDescritorPrincipal ):
        self.cursor.execute(""" INSERT INTO termoProcurado (termoProc, quantTermoProcurado, idDescritor, descritorPrincipal, quantDescritorPrincipal) 
                                VALUES (?, ?, ?, ?, ?) """, (termoProc, quantTermoProcurado, idDescritor, descritorPrincipal, quantDescritorPrincipal, ))
        return self.cursor.lastrowid

