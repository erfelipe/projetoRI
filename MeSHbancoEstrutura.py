import sqlite3
class BD:

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
        self.cursor.execute("""  CREATE TABLE if not exists descritores 
                            (iddesc   text PRIMARY KEY NOT NULL, 
                             namedesc text NOT NULL ); 
                    """) 
        
        self.cursor.execute("""  CREATE TABLE if not exists termos 
                            (iddesc   text NOT NULL,
                             idterm   text NOT NULL,
                             nameterm text NOT NULL );
                    """) 

        self.cursor.execute(""" CREATE TABLE if not exists hierarquia
                            (iddesc    text NOT NULL,
                             idhierarq text NOT NULL,
                             PRIMARY KEY (iddesc, idhierarq));
                    """)

    def inserirNoBancoDeDados(self, desc):
        iddesc = desc.iddesc
        descricao = desc.namedesc
        self.cursor.execute(" SELECT count(iddesc) from descritores where (iddesc = ?)", (iddesc, ) )
        result = self.cursor.fetchone()
        if (result[0] < 1):
            self.cursor.execute(" INSERT INTO descritores (iddesc, namedesc) VALUES (?, ?)", (iddesc, descricao.lower(),  ))
        
        termos = desc.terms
        for tr in termos:
            for idtermo, termo in tr.items():
                self.cursor.execute(" SELECT COUNT(idterm) from termos where (iddesc = ? and nameterm = ?)", (iddesc, termo.lower(), ) )
                result = self.cursor.fetchone()
                if (result[0] < 1):
                    self.cursor.execute(" INSERT INTO termos (iddesc, idterm, nameterm) VALUES (?, ?, ?)", (iddesc, idtermo, termo.lower() ) )

        for hq in desc.hierarq:
            self.cursor.execute(" SELECT COUNT(iddesc) from hierarquia where (iddesc = ? and idhierarq = ?)", (iddesc, hq, ) )
            result = self.cursor.fetchone()
            if (result[0] < 1):
                self.cursor.execute(" INSERT INTO hierarquia (iddesc, idhierarq) VALUES (?, ?)", (iddesc, hq) )
    
    # dado uma string, descobrir descritor. pode-se usar termos de entrada. 
    # retorna o iddesc e seu respectivo idhierarq + os termos de entrada 
    def selecionarDescritor_e_Termo(self, desc): 
        dataset = self.cursor.execute("""   select descritores.iddesc as idesc 
                                            , hierarquia.idhierarq as idhierarq 
                                            , descritores.namedesc
                                            from descritores 
                                            left join termos on descritores.iddesc = termos.iddesc
                                            left join hierarquia on descritores.iddesc = hierarquia.iddesc
                                            where (descritores.namedesc like ?) OR (termos.nameterm like ?) """, (desc, desc, )
                                     ).fetchone()
        return dataset

    def selecionarHierarquia(self, idhierarq):
        dataset = self.cursor.execute("""   select iddesc
                                            from hierarquia 
                                            where (idhierarq like ?) """, (idhierarq+'%', )
                                    ).fetchall()
        return dataset

    def selectionarTermosDeEntrada(self, iddesc):
        dataset = self.cursor.execute("""   select nameterm from termos 
                                            where iddesc = ? 
                                            order by nameterm """, (iddesc, )
                                    ).fetchall()
        return dataset

    def nomeDoDescritor(self, iddesc):
        dataset = self.cursor.execute("""   select namedesc
                                            from descritores
                                            where (iddesc = ?) """, (iddesc, ) 
                                    ).fetchone()
        return dataset[0]

    def selecionarTodosDescritores(self):
        dataset = self.cursor.execute("""   select namedesc from descritores                                          
                                            order by namedesc """,
                                    ).fetchall()
        return dataset

    def selecionarTodosTermos(self):
        dataset = self.cursor.execute("""   select nameterm from termos                                          
                                            order by nameterm """,
                                    ).fetchall()
        return dataset