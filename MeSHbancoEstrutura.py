import sqlite3

class BDMeSH:

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
        """ Cria a estrutura do banco de dados da terminologia MeSH
        """        
        self.cursor.execute("""  CREATE TABLE if not exists descritores 
                            ( 
                             iddesc   text PRIMARY KEY NOT NULL, 
                             namedesc text NOT NULL ); 
                    """) 
        
        self.cursor.execute("""  CREATE TABLE if not exists termos 
                            (
                             iddesc   text NOT NULL,
                             idterm   text NOT NULL,
                             nameterm text NOT NULL );
                    """) 

        self.cursor.execute(""" CREATE TABLE if not exists hierarquia
                            (iddesc    text NOT NULL,
                             idhierarq text NOT NULL,
                             PRIMARY KEY (iddesc, idhierarq));
                    """)

        self.cursor.execute("CREATE INDEX idx_descritores_namedesc ON descritores (namedesc);")
        self.cursor.execute("CREATE INDEX idx_termos_nameterm ON termos (nameterm);")

    def inserirNoBancoDeDados(self, desc):
        """ Insere os dados no modelo relacional mapeado para o MeSH
        
        Arguments:
            desc {class} -- Classe que contém tanto o descritor principal como os termos hierarquicos
        """        
        iddesc = desc.iddesc
        descricao = desc.namedesc
        self.cursor.execute(" INSERT INTO descritores (iddesc, namedesc) VALUES (?, ?)", (iddesc, descricao.lower(),))
        
        termos = desc.terms 
        for tr in termos:
            for idtermo, termo in tr.items():
                self.cursor.execute(" INSERT INTO termos (iddesc, idterm, nameterm) VALUES (?, ?, ?)", (iddesc, idtermo, termo.lower(),) )

        for hq in desc.hierarq:
            self.cursor.execute(" INSERT INTO hierarquia (iddesc, idhierarq) VALUES (?, ?)", (iddesc, hq,) )
        
    # *********************************************
    #  Para cada DESCRITOR pode haver varios TERMOS
    # *********************************************

    # dado uma string, descobrir descritor. pode-se usar termos de entrada. 
    # retorna o iddesc e o termo de entrada 
    def selecionarIdDescritorPeloNomeDescritor(self, desc): 
        """[summary]
        
        Arguments:
            desc {[type]} -- [description]
        
        Returns:
            [type] -- [description]
        """        
        dataset = self.cursor.execute("""   select descritores.iddesc as idesc 
                                            , descritores.namedesc
                                            from descritores 
                                            left join termos on descritores.iddesc = termos.iddesc
                                            where (descritores.namedesc like ?) OR (termos.nameterm like ?) 
                                            group by idesc, namedesc """, (desc, desc, )
                                     ).fetchone()
        return dataset

    # um IdDescritor pode possuir varios IdsHierarquicos 
    def selecionarIdsHierarquiaPorIdDescritor(self, idDescritor):
        """[summary]
        
        Arguments:
            idDescritor {[type]} -- [description]
        
        Returns:
            [type] -- [description]
        """        
        dataset = self.cursor.execute("""   select idhierarq 
                                            from hierarquia h
                                            where (h.iddesc like ?) """, (idDescritor, )
                                    ).fetchall()
        return dataset 

    # dado um IdHierarquico, quais termos estao associados a ele
    def selecionarTermosPorIdHierarquico(self, idhierarq):
        """[summary]
        
        Arguments:
            idhierarq {[type]} -- [description]
        
        Returns:
            [type] -- [description]
        """        
        dataset = self.cursor.execute("""   select d.namedesc, t.nameterm, idhierarq 
                                            from hierarquia h
                                            join descritores d on d.iddesc = h.iddesc
                                            join termos t on t.iddesc = d.iddesc
                                            where (idhierarq like ?) """, (idhierarq+'%', )
                                    ).fetchall()
        listagem = set()
        for linha in dataset:
            listagem.add(linha[0])
            listagem.add(linha[1])
        return listagem

    # qual IdDescritor o IdHierarquia esta relacionado
    def selecionarIdDescritorPorIdHierarquia(self, idhierarq):
        """[summary]
        
        Arguments:
            idhierarq {[type]} -- [description]
        
        Returns:
            [type] -- [description]
        """        
        dataset = self.cursor.execute("""   select iddesc
                                            from hierarquia 
                                            where (idhierarq like ?) """, (idhierarq+'%', )
                                    ).fetchall()
        return dataset

    def selectionarTermosDeEntrada(self, iddesc):
        """[summary]
        
        Arguments:
            iddesc {[type]} -- [description]
        
        Returns:
            [type] -- [description]
        """        
        dataset = self.cursor.execute("""   select nameterm from termos 
                                            where iddesc = ? 
                                            order by nameterm """, (iddesc, )
                                    ).fetchall()
        return dataset

    def nomeDoDescritor(self, iddesc):
        """[summary]
        
        Arguments:
            iddesc {[type]} -- [description]
        
        Returns:
            [type] -- [description]
        """        
        dataset = self.cursor.execute("""   select namedesc
                                            from descritores
                                            where (iddesc = ?) """, (iddesc, ) 
                                    ).fetchone()
        return dataset[0]

    def selecionarTodosDescritores(self):
        """ Retorna todos os descritores cadastrados no MeSH
        
        Returns:
            dataset -- conjunto de termos descritores em ordem alfabetica
        """        
        dataset = self.cursor.execute("""   select namedesc from descritores                                          
                                            order by namedesc """,
                                    ).fetchall()
        return dataset

    def selecionarTodosTermos(self):
        """ Retorna todos os termos de entrada (entry terms) cadastrados no MeSH
        
        Returns:
            str -- conjunto de termos de entrada em ordem alfabetica
        """        
        dataset = self.cursor.execute("""   select nameterm from termos                                          
                                            order by nameterm """,
                                    ).fetchall()
        return dataset