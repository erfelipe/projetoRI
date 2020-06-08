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

    def selecionarIdDescritorPeloNomeDescritor(self, desc, tipoTermo, idioma): 
        """ Dado um determinado descritor "heart attack", retorna o ID e seu nome de descricao
        
        Arguments:
            desc {str} -- Descritor em string
            tipoTermo {str} -- O = original e T = tratado
            idioma {str} -- en = ingles e spa = espanhol 
        
        Returns:
            list -- Array com ID e nome do descritor
        """        
        if (tipoTermo == 'O'): #original
            dataset = self.cursor.execute("""   select descritores.iddesc as idesc 
                                            , descritores.namedesc 
                                            from descritores 
                                            left join termos on descritores.iddesc = termos.iddesc 
                                            where (descritores.lang like ?) and (termos.lang like ?) and ((descritores.namedesc like ?) OR (termos.nameterm like ?)) 
                                            group by idesc, namedesc """, (idioma, idioma, desc, desc, )
                                     ).fetchone()
        else: #tratado
            dataset = self.cursor.execute("""   select descritores.iddesc as idesc 
                                            , descritores.namedescTratado 
                                            from descritores 
                                            left join termos on descritores.iddesc = termos.iddesc 
                                            where (descritores.lang like ?) and (termos.lang like ?) and ((descritores.namedescTratado like ?) OR (termos.nametermTratado like ?)) 
                                            group by idesc, namedesc """, (idioma, idioma, desc, desc, )
                                     ).fetchone()
        return dataset

    def selecionarDescritoresPorIdHierarquico(self, idhierarq, tipoTermo, idioma):
        """ Retorna uma lista somente com os descritores a partir de um determinado id hierarquico 
            Ou seja, considera-se neste contexto de termos hierarquicos, as duas classes de palavras da terminologia

        Arguments:
            idhierarq {str} -- codigo para identificar a hierarquia do termo 
            idioma {str} -- eng = ingles, es = espanhol, etc... 
            tipoTermo {str} -- O = original e T = tratado 

        Returns:
            list -- array (set) com descritores de forma unica, sem repeticao 
        """     
        if (tipoTermo == 'O'): # original
            dataset = self.cursor.execute("""   select d.namedesc 
                                                from hierarquia h
                                                join descritores d on d.iddesc = h.iddesc
                                                where (d.lang like ?) and (idhierarq like ?) 
                                                group by d.namedesc """, (idioma, idhierarq+'%', )
                                        ).fetchall()
        else:
            dataset = self.cursor.execute("""   select d.namedescTratado 
                                                from hierarquia h
                                                join descritores d on d.iddesc = h.iddesc
                                                where (d.lang like ?) and (idhierarq like ?) 
                                                group by d.namedescTratado """, (idioma, idhierarq+'%', )
                                        ).fetchall()
        listagem = set() 
        for linha in dataset: 
            listagem.add(linha[0]) 
        return listagem 

    def selecionarIdDescritoresPorIdHierarquico(self, idhierarq, idioma):
        """ Retorna os ids dos descritores associados ao id hierarquico e em seu respectivo idioma
        
        Arguments:
            idhierarq {str} -- identificador hierarquico 
            idioma {str} -- eng = ingles, es = espanhol, etc... 

        Returns:
            str -- Retorna os ids descritores associados ao id hierarquico 
        """ 
        dataset = self.cursor.execute("""   select d.iddesc
                                            from hierarquia h
                                            join descritores d on d.iddesc = h.iddesc
                                            where (h.idhierarq like ?) and (d.lang like ?) 
                                            group by d.iddesc """, (idhierarq+'%', idioma, )
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
        """ Dado um codigo de descritor, retorna suas descricoes textuais, original e tratado
        
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

    def selecionarDescritoresHierarquicos(self, termoProcurado, tipoTermo, idioma):
        """ Retorna uma lista com os descritores hierarquicos a partir de um termo procurado da terminologia MeSH

        Arguments:
            termoProcurado {str} -- Termo da terminologia MeSH 
            tipoTermo {str} -- 'O' = original
            idioma {str} -- 'eng' = ingles
        
        Returns:
            list -- Array com termos hierarquicos, unicos (set) 
        """        
        resultado = self.selecionarIdDescritorPeloNomeDescritor(termoProcurado, tipoTermo, idioma) 
        if resultado is not None:
            idDescritor = str(resultado[0]) 
            descritorPrincipal = str(resultado[1]) 
        else:
            idDescritor = ""
            descritorPrincipal = ""

        resultado = self.selecionarIdsHierarquiaPorIdDescritor(idDescritor, idioma)
        descHierarquicos = set() 
        for id in resultado:
            DescritoresPorIdHierarquico = self.selecionarDescritoresPorIdHierarquico(id[0], tipoTermo, idioma)
            for desc in DescritoresPorIdHierarquico:
                descHierarquicos.add(desc)
        if (descritorPrincipal in descHierarquicos):
            descHierarquicos.remove(descritorPrincipal)
        return descHierarquicos

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

    def selecionarTermosDeEntradaPeloIdDescritor(self, iddesc, tipoTermo, idioma):
        """ Dado um identificador de descritor, retorna os termos de entrada (entry terms) 
        
        Arguments:
            iddesc {str} -- Identificador do descritor
            tipoTermo {str} -- O = original e T = tratado
            idioma {str} -- eng = ingles, es = espanhol, etc...      

        Returns: 
            list -- Nomes dos termos associados a determinado descritor
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

    def selecionarTermosDeEntradaDeUmDescritor(self, termoProcurado, tipoTermo, idioma):
        """ A partir do termo procurado, indentifica os termos de entrada relacionados ao mesmo
        
        Arguments:
            termoProcurado {str} -- Termo previamente selecionado da terminologia MeSH
            tipoTermo {str} -- 'O' = original
            idioma {str} -- 'eng' = ingles
        
        Returns:
            set -- Array com elementos unicos (sem repeticao) 
        """        
        resultado = self.selecionarIdDescritorPeloNomeDescritor(termoProcurado, tipoTermo, idioma) 
        idDescritor = str(resultado[0]) 
        descritorPrincipal = str(resultado[1]) 
        
        resultado = self.selecionarTermosDeEntradaPeloIdDescritor(idDescritor, tipoTermo, idioma) 
        termosEntrada = set()
        for ent in resultado:
            if (ent[0] != descritorPrincipal and ent[0] != termoProcurado):
                termosEntrada.add(ent[0])
        return termosEntrada

    def selecionarTermosDeEntradaHierarquicos(self, termoProcurado, tipoTermo, idioma):
        """ A partir do termo procurado, indentifica os termos de entrada relacionados em seus sub descritores

        Arguments:
            termoProcurado {str} -- Termo previamente selecionado da terminologia MeSH
            tipoTermo {str} -- 'O' = original
            idioma {str} -- 'eng' = ingles

        Returns:
            set -- Array com elementos unicos (sem repeticao) 
        """        
        resultado = self.selecionarIdDescritorPeloNomeDescritor(termoProcurado, tipoTermo, idioma) 
        if resultado is not None:
            idDescritorPrincipal = str(resultado[0]) 
        else:
            idDescritorPrincipal = ""
        idsHierarquicos = self.selecionarIdsHierarquiaPorIdDescritor(idDescritorPrincipal, idioma)

        termosEntradaHierarquicos = set()
        for idHierarquico in idsHierarquicos:
            IdDescritores = self.selecionarIdDescritoresPorIdHierarquico(idHierarquico[0], idioma)
            for idDesc in IdDescritores:
                termosEntrada = self.selecionarTermosDeEntradaPeloIdDescritor(idDesc[0], tipoTermo, idioma) 
                for termo in termosEntrada:
                    termosEntradaHierarquicos.add(termo[0])
        
        return termosEntradaHierarquicos

    def selecionarIdsHierarquiaPorIdDescritor(self, idDescritor, idioma):
        """ Dado um determinado ID, selecionar seus IDs hierarquicos
            obs: um IdDescritor pode possuir varios IdsHierarquicos 
        
        Arguments:
            idDescritor {str} -- Identificador do descritor 0099988
            idioma {str} -- eng = ingles, es = espanhol, etc...
        
        Returns:
            dataset -- Array com varios IDs
        """        
        dataset = self.cursor.execute("""   select idhierarq 
                                            from hierarquia h
                                            where (h.lang like ?) and (h.iddesc like ?) """, (idioma, idDescritor, )
                                    ).fetchall()
        return dataset 

    def substituiTermosDaFrasePLN(self, frase, termo, lista):
        """ Retorna novas frases a partir do termo encontrado na terminologia, substituindo o termo na parte da frase
            pelos outros termos hierarquicos ou de entrada, passados pela lista 
        
        Arguments:
            frase {str} -- Frase original em linguagem natural
            lista {list} -- Termos hierarquicos ou de entrada, relacionados ao termo encontrado na terminologia
        
        Returns:
            list -- Array com as frases modificadas com termos hierarquicos ou de entrada no lugar do termo original
        """        
        novasFrases = []
        for item in lista:
            novaFrase = frase.replace(termo, item)
            novasFrases.append(novaFrase)
        return novasFrases

    def possuiCorrespondenciaNoMeSH(self, idioma, termo, tipoTermo):
        """ Recebe um termo e pesquisa na terminologia se ha correspondencia identica
        
        Arguments:
            idioma {str} -- 'eng' = ingles
            termo {str} -- Termo a ser pesquisado na terminologia MeSH
            tipoTermo {str} -- 'O' = Original
        
        Returns:
            int -- Quantidade de termos identicos encontrados. Se > 0, foi encontrado. 
        """        
        if (tipoTermo == 'O'): #original
            dataset = self.cursor.execute(""" select count(d.iddesc) 
                                                from descritores d 
                                                left join termos t on d.iddesc = t.iddesc 
                                                where (d.lang = ?) and ((d.namedesc like ?) or (t.nameterm like ?)) """, (idioma, termo, termo, )
                                                ).fetchone()
        else:
            dataset = self.cursor.execute(""" select count(d.iddesc) 
                                                from descritores d 
                                                left join termos t on d.iddesc = t.iddesc 
                                                where (d.lang = ?) and ((d.namedescTratado like ?) or (t.nametermTratado like ?)) """, (idioma, termo, termo, )
                                                ).fetchone()            
        return dataset[0]

    def identificaSeEstaPresenteNoMeSH(self, tipoTermo, idioma, palavras, inicioPesquisa, fimPesquisa, tEncontrado):
        """ Recebe uma frase, quebra em tokens e deste array resultante, faz pesquisas incrementais para encontrar
            qual termo esta presente na terminologia MeSH
        
        Arguments:
            tipoTermo {str} -- 'O' = Original
            idioma {str} -- 'en' = ingles
            palavras {list} -- Array com os termos da frase ja separados, em minuscula, sem pontuacao
            inicioPesquisa {int} -- Posicao do array onde comeca a pesquia
            fimPesquisa {int} -- Posicao do array para marcar o termino da pesquisa
            tEncontrado {str} -- Termo encontrado na terminologia MeSH
        
        Returns:
            str -- Termo encontrado na terminologia MeSH
        """        
        if (fimPesquisa >= len(palavras)):
            return tEncontrado
        else:
            if (int(fimPesquisa) > int(inicioPesquisa)):
                termo = "" 
                ini = inicioPesquisa
                while (ini <= fimPesquisa):
                    termo = str(termo) + " " + str(palavras[ini])
                    ini += 1
                termo = termo.strip()
            else:
                termo = palavras[inicioPesquisa] 
            if (self.possuiCorrespondenciaNoMeSH(idioma, termo, tipoTermo) > 0): 
                tEncontrado = termo 
            fimPesquisa += 1 
            return self.identificaSeEstaPresenteNoMeSH(tipoTermo, idioma, palavras, inicioPesquisa, fimPesquisa, tEncontrado) 

    def identificarTermosPelaPLN(self, frase, idioma):
        """ Recebe uma frase de entrada em Linguagem Natural e identifica quais os termos da frase estao presentes 
            na terminologia MeSH
        
        Arguments:
            frase {str} -- Frase em Linguagem Natural
            idioma {str} -- 'O' = original
        """
        fraseOriginal = frase
        fraseTratada = preProcessamentoTextual.cleanEspecialsChars(fraseOriginal).strip()
        palavras = fraseTratada.lower().split(" ")
        inicioPesquisa = 0
        termosPresentesNoMeSH = []
        for _ in palavras:
            tPresenteNoMeSH = self.identificaSeEstaPresenteNoMeSH('O', idioma, palavras, int(inicioPesquisa), int(inicioPesquisa), "")
            if tPresenteNoMeSH:
                print(tPresenteNoMeSH)
                print('----')
                termosPresentesNoMeSH.append(tPresenteNoMeSH)
                termosEntrada = self.selecionarTermosDeEntradaDeUmDescritor(tPresenteNoMeSH, 'O', idioma) 
                for t in termosEntrada:
                    print(t)
                print('----')

                novasFrases = self.substituiTermosDaFrasePLN(fraseOriginal, tPresenteNoMeSH, termosEntrada)
                for nf in novasFrases:
                    print(nf)

                # termosHierarquicos = self.listaTermosHierarquicos(tPresenteNoMeSH, 'O', idioma)
                # for t in (termosHierarquicos):
                #     print(t)
            inicioPesquisa += 1
