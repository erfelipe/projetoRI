import sqlite3 
import preProcessamentoTextual

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
                             iddesc   text NOT NULL, 
                             namedesc text NOT NULL,
                             namedescTratado text NOT NULL,
                             lang text NOT NULL ); 
                    """) 
        
        self.cursor.execute("""  CREATE TABLE if not exists termos 
                            (
                             iddesc   text NOT NULL,
                             idterm   text NOT NULL,
                             nameterm text NOT NULL,
                             nametermTratado text NOT NULL,
                             lang text NOT NULL );
                    """) 

        self.cursor.execute(""" CREATE TABLE if not exists hierarquia
                            (iddesc    text NOT NULL,
                             idhierarq text NOT NULL,
                             lang  text NOT NULL,
                             PRIMARY KEY (iddesc, idhierarq, lang));
                    """)

        self.cursor.execute("CREATE INDEX idx_descritores_namedesc ON descritores (namedesc);")
        self.cursor.execute("CREATE INDEX idx_descritores_namedescTratado ON descritores (namedescTratado);")
        self.cursor.execute("CREATE INDEX idx_termos_nameterm ON termos (nameterm);")
        self.cursor.execute("CREATE INDEX idx_termos_nametermTratado ON termos (nametermTratado);")

    def inserirNoBancoDeDados(self, desc):
        """ Insere os dados no modelo relacional mapeado para o MeSH
        
        Arguments:
            desc {class} -- Classe que contém tanto o descritor principal como os termos hierarquicos
        """        
        lang = desc.language
        iddesc = desc.iddesc
        descricao = desc.namedesc
        descricaoTratada = preProcessamentoTextual.trataDescricao(descricao)
        self.cursor.execute(" INSERT INTO descritores (iddesc, namedesc, namedescTratado, lang) VALUES (?, ?, ?, ?)", (iddesc, descricao.lower(), descricaoTratada.lower(), lang, ))
        
        termos = desc.terms 
        for tr in termos:
            for idtermo, termo in tr.items():
                termoTratado = preProcessamentoTextual.trataDescricao(termo)
                self.cursor.execute(" INSERT INTO termos (iddesc, idterm, nameterm, nametermTratado, lang) VALUES (?, ?, ?, ?, ?)", (iddesc, idtermo, termo.lower(), termoTratado.lower(), lang, ))

        for hq in desc.hierarq:
            self.cursor.execute(" INSERT INTO hierarquia (iddesc, idhierarq, lang) VALUES (?, ?, ?)", (iddesc, hq, lang,) )
        
    # *********************************************
    #  Para cada DESCRITOR pode haver varios TERMOS
    # *********************************************

    # dado uma string, descobrir descritor. pode-se usar termos de entrada. 
    # retorna o iddesc e o termo de entrada 
    def selecionarIdDescritorPeloNomeDescritor(self, desc, tipoTermo, idioma): 
        """ Dado um determinado descritor "heart attack", retorna o ID e seu nome de descricao
        
        Arguments:
            desc {str} -- Descritor em string
            tipoTermo {str} -- O = original e T = tratado
            idioma {str} -- en = ingles e spa = espanhol 
        
        Returns:
            [dataset] -- Array com ID e nome do descritor, de um indice (0) apenas
        """        
        if (tipoTermo == 'O'): #original
            dataset = self.cursor.execute("""   select descritores.iddesc as idesc 
                                            , descritores.namedesc 
                                            from descritores 
                                            left join termos on descritores.iddesc = termos.iddesc 
                                            where (descritores.lang like ?) and (termos.lang like ?) and (descritores.namedesc like ?) OR (termos.nameterm like ?) 
                                            group by idesc, namedesc """, (idioma, idioma, desc, desc, )
                                     ).fetchone()
        else: #tratado
            dataset = self.cursor.execute("""   select descritores.iddesc as idesc 
                                            , descritores.namedescTratado 
                                            from descritores 
                                            left join termos on descritores.iddesc = termos.iddesc 
                                            where (descritores.lang like ?) and (termos.lang like ?) and (descritores.namedescTratado like ?) OR (termos.nametermTratado like ?) 
                                            group by idesc, namedesc """, (idioma, idioma, desc, desc, )
                                     ).fetchone()
        return dataset

    # um IdDescritor pode possuir varios IdsHierarquicos 
    def selecionarIdsHierarquiaPorIdDescritor(self, idDescritor, idioma):
        """ Dado um determinado ID, selecionar seus IDs hierarquicos
        
        Arguments:
            idDescritor {str} -- Identificador do descritor 0099988
            idioma {str} -- eng = ingles, es = espanhol, etc...
        
        Returns:
            [dataset] -- Array com varios IDs
        """        
        dataset = self.cursor.execute("""   select idhierarq 
                                            from hierarquia h
                                            where (h.lang like ?) and (h.iddesc like ?) """, (idioma, idDescritor, )
                                    ).fetchall()
        return dataset 

    # dado um IdHierarquico, quais termos estao associados a ele
    def selecionarTermosPorIdHierarquico(self, idhierarq, tipoTermo, idioma):
        """[summary]
        
        Arguments:
            idhierarq {str} -- codigo para identificar a hierarquia do termo
            idioma {str} -- eng = ingles, es = espanhol, etc...
            tipoTermo {str} -- O = original e T = tratado

        Returns:
            set -- array com descricao, nome do termo e id hierarquico
        """     
        if (tipoTermo == 'O'): # original
            dataset = self.cursor.execute("""   select d.namedesc, t.nameterm, h.idhierarq 
                                                from hierarquia h
                                                join descritores d on d.iddesc = h.iddesc
                                                join termos t on t.iddesc = d.iddesc
                                                where (h.lang like ?) and (idhierarq like ?) """, (idioma, idhierarq+'%', )
                                        ).fetchall()
        else:
            dataset = self.cursor.execute("""   select d.namedescTratado, t.nametermTratado, h.idhierarq 
                                                from hierarquia h
                                                join descritores d on d.iddesc = h.iddesc
                                                join termos t on t.iddesc = d.iddesc
                                                where (h.lang like ?) and (idhierarq like ?) """, (idioma, idhierarq+'%', )
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

    def selectionarTermosDeEntrada(self, iddesc, tipoTermo, idioma):
        """[summary]
        
        Arguments:
            iddesc {[type]} -- [description]
            tipoTermo {str} -- O = original e T = tratado
            idioma {str} -- eng = ingles, es = espanhol, etc...      

        Returns: 
            [type] -- [description] 
        """  
        if (tipoTermo == 'O'): #original
            dataset = self.cursor.execute("""   select nameterm from termos 
                                                where  (termos.lang like ?) and (iddesc = ?) 
                                                order by nameterm """, (idioma, iddesc, )
                                        ).fetchall()
        else: #tratado
            dataset = self.cursor.execute("""   select nametermTratado from termos 
                                                where  (termos.lang like ?) and (iddesc = ?) 
                                                order by nameterm """, (idioma, iddesc, )
                                        ).fetchall()
        return dataset

    def selecionarNomeDoDescritor(self, iddesc):
        """ Dado um codigo de descritor, retorna sua descricao textual
        
        Arguments:
            iddesc {str} -- Codigo do descritor, exemplo D000001
        
        Returns:
            str -- A descricao por extenso do descritor
        """        
        dataset = self.cursor.execute("""   select namedesc
                                            from descritores
                                            where (iddesc = ?) """, (iddesc, ) 
                                    ).fetchone()
        return dataset[0]

    def selecionarNomeDoDescritorTratado(self, iddesc):
        """ Dado um codigo de descritor, retorna sua descricao textual
        
        Arguments:
            iddesc {str} -- Codigo do descritor, exemplo D000001
        
        Returns:
            str -- A descricao por extenso do descritor tratado, sem caracteres especiais
        """        
        dataset = self.cursor.execute("""   select namedescTratado
                                            from descritores
                                            where (iddesc = ?) """, (iddesc, ) 
                                    ).fetchone()
        return dataset[0]

    def selecionarNomesDeAmbosDescritores(self, iddesc):
        """ Dado um codigo de descritor, retorna suas descricoes textuais
        
        Arguments:
            iddesc {str} -- Codigo do descritor, exemplo D000001
        
        Returns:
            str -- A descricao por extenso dos descritores: original e tratado, sem caracteres especiais
        """        
        dataset = self.cursor.execute("""   select namedesc, namedescTratado
                                            from descritores
                                            where (iddesc = ?) """, (iddesc, ) 
                                    ).fetchone()
        return dataset

    def selecionarNomesDescritoresPorIdHierarquia(self, idhierarq):
        """ Retorna os descritores originais e tratados a partir de uma hierarquia
        
        Arguments:
            idhierarq {str} -- Codigo da hierarquia, exemplo  C26017

        Returns:
            dataset -- Conjunto de todas as descricoes a partir de um codigo hierarquico
        """        
        dataset = self.cursor.execute( """ select d.namedesc, d.namedescTratado 
                                            from descritores d 
                                            join hierarquia h on h.iddesc = d.iddesc 
                                            where (h.idhierarq like ?) """, (idhierarq+'%',)
                                            ).fetchall()
        return dataset

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
    
    def selecionarNomesTermosPorIdHierarquia(self, idhierarq):
        """ Retorna as descricoes dos termos de entrada (entry terms) cadastrados no MeSH, com e sem tratamento
        
        Returns:
            str -- conjunto de termos orignais e tratados
        """        
        dataset = self.cursor.execute("""   select t.nameterm, t.nametermTratado 
                                            from termos t 
                                            join hierarquia h on h.iddesc = t.iddesc 
                                            where (h.idhierarq like ?) """, (idhierarq+'%', )
                                    ).fetchall()
        return dataset
