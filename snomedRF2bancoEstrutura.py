import sqlite3
import re
import logging
import constantes
import preProcessamentoTextual

logging.basicConfig(filename=constantes.LOG_FILE, filemode='w', format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

class BDSnomed:
    
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
        """ Cria a estrutura do banco de dados 
        """ 
        self.cursor.execute("""  CREATE TABLE if not exists concept 
                            (id   text NOT NULL ); 
                    """) 
        
        self.cursor.execute("""  CREATE TABLE if not exists description 
                            (id   text NOT NULL,
                             conceptId text NOT NULL, 
                             languageCode text NOT NULL,
                             termOriginal text NOT NULL,
                             termTratado text NOT NULL );
                    """) 

        self.cursor.execute(""" CREATE TABLE if not exists refset
                            (id text NOT NULL, 
                            owlExpression text NOT NULL );
                    """)

        self.cursor.execute(""" CREATE TABLE if not exists statedrelationship
                            (id text NOT NULL, 
                            sourceId text NOT NULL, 
                            destinationId text NOT NULL );
                    """)

        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_concept_id ON concept (id);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_description_id ON description (conceptId);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_description_lang ON description (languageCode);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_description_termOriginal ON description (termOriginal);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_description_termTratado ON description (termTratado);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_refset_owl ON refset (owlExpression);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_statedrelationship_srcId ON statedrelationship (sourceId);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_statedrelationship_destId ON statedrelationship (destinationId);")

    def inserirConcept(self, tupla):
        """ Insere registros na tabela concept 
            Por motivos de performance farei insercao direta, sem validacao
            Args: 
                param1 (array): registro lido do arquivo txt

            Returns:
                void
        """ 
        id = tupla[0]
        #effectiveTime = preProcessamentoTextual.dateToTimeString(tupla[1])
        active = tupla[2]
        #moduleId = tupla[3]
        #definitionStatusId = tupla[4]
        if (active == '1'):
            try:
                self.cursor.execute(" INSERT INTO concept (id) VALUES (?)", (id, ))
            except Exception as identifier:
                print('* Erro na inserção concept - do ID' + id + identifier)

    def inserirDescription(self, tupla):
        """ Insere registros na tabela description 

            Por motivos de performance farei insercao direta, sem validacao
            Atenção: a linha: 1225447015	20040131	0	900000000000207008	275814008	en	900000000000013009	Morning after pill	900000000000020002
            contém uma aspa dupla que atrapalha toda a importacao. Eh necessario apagar antes de prosseguir com o algoritmo 
            
            Args: 
            param1 (array): registro lido do arquivo txt 

            Returns:
            void
        """         
        id = tupla[0]
        #effectiveTime = preProcessamentoTextual.dateToTimeString(tupla[1])
        active = tupla[2]
        #moduleId = tupla[3]
        conceptId = tupla[4]
        languageCode = tupla[5]
        #typeId = tupla[6]
        termOriginal = tupla[7]
        termTratado = preProcessamentoTextual.trataDescricao(tupla[7])
        #caseSignificanceId = tupla[8]
        if (active == '1'):
            try:
                self.cursor.execute(" INSERT INTO description (id, conceptId, languageCode, termOriginal, termTratado) VALUES (?, ?, ?, ?, ?)", (id, conceptId, languageCode, termOriginal.lower(), termTratado.lower(), ))
            except Exception as identifier:
                print('* Erro na inserção description - do ID' + id + identifier)
        
    def inserirRelationShip(self, tupla):
        id = tupla[0]
        effectiveTime = preProcessamentoTextual.dateToTimeString(tupla[1])
        active = tupla[2]
        moduleId = tupla[3]
        sourceId = tupla[4]
        destinationId = tupla[5]
        relationshipGroup = tupla[6]
        typeId = tupla[7]
        characteristicTypeId = tupla[8]
        modifierId = tupla[9]
        """ Por motivos de performance farei insercao direta, sem validacao """
        if (active == '1'):
            try:
                self.cursor.execute(" INSERT INTO relationship (id, effectiveTime, active, moduleId, sourceId, destinationId, relationshipGroup, typeId, characteristicTypeId, modifierId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, sourceId, destinationId, relationshipGroup, typeId, characteristicTypeId, modifierId, ))
            except Exception as identifier:
                print('* Erro na inserção relationship - do ID' + id + identifier)

    def inserirSrefSet(self, tupla):
        """ Insere registros na tabela refSet

            Por motivos de performance farei insercao direta, sem validacao

        Args:
            tupla (array): registro lido do arquivo txt 
        """        
        id = tupla[0]
        #effectiveTime = preProcessamentoTextual.dateToTimeString(tupla[1])
        active = tupla[2]
        #moduleId = tupla[3]
        #refsetId = tupla[4]
        #referencedComponentId = tupla[5]
        owlExpression = tupla[6]
        if (active == '1'):
            try:
                self.cursor.execute(" INSERT INTO refset (id, owlExpression) VALUES (?, ?)", (id, owlExpression, ))
            except Exception as identifier:
                print('* Erro na inserção refset - do ID' + id + identifier)

    def inserirStatedRelationShip(self, tupla):
        """ Insere registros na tabela statedrelationship

            Por motivos de performance farei insercao direta, sem validacao

        Args:
            tupla (array): registro lido do arquivo txt 
        """        
        id = tupla[0]
        #effectiveTime = preProcessamentoTextual.dateToTimeString(tupla[1])
        #active = tupla[2]
        #moduleId = tupla[3]
        sourceId = tupla[4]
        destinationId = tupla[5]
        #relationshipGroup = tupla[6]
        #typeId = tupla[7]
        #characteristicTypeId = tupla[8]
        #modifierId = tupla[9]
        try:
            self.cursor.execute(" INSERT INTO statedrelationship (id, sourceId, destinationId) VALUES (?, ?, ?)", (id, sourceId, destinationId, ))
        except Exception as identifier:
            print('* Erro na inserção statedrelationship - do ID' + id + identifier)

    def inserirTextDefinition(self, tupla):
        id = tupla[0]
        effectiveTime = preProcessamentoTextual.dateToTimeString(tupla[1])
        active = tupla[2]
        moduleId = tupla[3]
        conceptId = tupla[4]
        languageCode = tupla[5]
        typeId = tupla[6]
        term = tupla[7]
        caseSignificanceId = tupla[8]
        """ Por motivos de performance farei insercao direta, sem validacao """
        if (active == '1'):
            try:
                self.cursor.execute(" INSERT INTO textdefinition (id, effectiveTime, active, moduleId, conceptId, languageCode, typeId, term, caseSignificanceId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, conceptId, languageCode, typeId, term, caseSignificanceId, ))
            except Exception as identifier:
                print('* Erro na inserção do ID' + id + identifier)

    def inserirLinha(self, linha, tabela):
        if tabela == 'concept':
            self.inserirConcept(linha)
        elif tabela == 'description':
            self.inserirDescription(linha)
        # elif tabela == 'relationship':
        #     self.inserirRelationShip(linha)
        elif tabela == 'srefset':
            self.inserirSrefSet(linha)
        elif tabela == 'statedrelationship':
            self.inserirStatedRelationShip(linha)
        # elif tabela == 'textdefinition':
        #     self.inserirTextDefinition(linha)

    def listaTermosProximosConceitualmente(self, termoProcurado, tipoTermo, idioma):
        pass

    def hierarquiaDeIDsPorIdConcept(self, IdConcept, resp=None):
        """ Dado um ConceptId: 22298006, pesquisa seus IDs ligados hierarquicamente

        Args: 
            param1 (str): identificador do conceito 
            param2 (array): Ids hierarquicos

        Returns: 
            list: retorna array de IDs: ['266288001', '266288333', ...]

        a primeira query recupera os axiomas que possuem este conceptId
        para cada axioma, eh analisado se eh uma hierarquia do termo
        exemplo: 
        EquivalentClasses(:238594009 ObjectIntersectionOf(:247446008 :418363000 :64572001 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:116676008 :409777003) ObjectSomeValuesFrom(:363698007 :39937001)))))
        SubClassOf(:247446008 ObjectIntersectionOf(:404684003 ObjectSomeValuesFrom(:609096000 ObjectSomeValuesFrom(:363698007 :39937001))))
        EquivalentClasses(:201077008 ObjectIntersectionOf(:247446008 :418363000 :89105000 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:116676008 :409777003) ObjectSomeValuesFrom(:363698007 :39937001)))))
        SubClassOf(:371068009 :22298006)
        EquivalentClasses(:30277009 ObjectIntersectionOf(:29889000 :371068009 :57054005 ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:116676008 :125671007) ObjectSomeValuesFrom(:363698007 :21814001))) ObjectSomeValuesFrom(:609096000 ObjectIntersectionOf(ObjectSomeValuesFrom(:116676008 :55470003) ObjectSomeValuesFrom(:363698007 :74281007)))))
        """
        dataSetAxioma = self.selecionarAxiomasPorConceptID(IdConcept)

        if (len(dataSetAxioma) > 0):
            if (resp is None):
                resp = []
            axAbout = ''
            codigos = []
            espaco = int(-1)
            objInterSize = len('ObjectIntersectionOf(') 
            for ax in dataSetAxioma: 
                codigos.clear()
                pParentesis = ax[0].find('(')

                introAxioma = ax[0][0:pParentesis]
                if introAxioma == 'EquivalentClasses':
                    espaco = ax[0].find(' ', len('EquivalentClasses(:'))
                    axAbout = ax[0][len('EquivalentClasses(:') : espaco]
                else:
                    if introAxioma == 'SubClassOf':
                        espaco = ax[0].find(' ', len('SubClassOf(:'))
                        axAbout = ax[0][len('SubClassOf(:') : espaco]

                ind = ax[0].find('ObjectIntersectionOf(') 
                ehNumero = True 
                if ind > -1: 
                    ind = ind + objInterSize 
                    while (ehNumero): 
                        espaco = ax[0].find(' ', ind) 
                        if espaco > -1: 
                            cod = ax[0][ind+1 : espaco]
                            ehNumero = cod.isdigit()
                            if (ehNumero): 
                                codigos.append(ax[0][ind+1:espaco]) 
                            ind = int(espaco) + 1 
                        else:
                            ehNumero = False
                else: #situacao SubClassOf(:371068009 :22298006)
                    if (introAxioma == 'SubClassOf'):
                        espaco = ax[0].find(' ')
                        ultParentese = ax[0].find(')')
                        if (espaco > -1) and (ultParentese > -1):
                            cod = ax[0][espaco+2 : ultParentese]
                            ehNumero = cod.isdigit()
                            if (ehNumero):
                                codigos.append(cod)
                isChild = False
                for cod in codigos:
                    if (cod == IdConcept):
                        isChild = True
                if isChild:
                    if axAbout:
                        resp.append(axAbout)
                        temMaisFilhos = self.hierarquiaDeIDsPorIdConcept(axAbout, resp) 
                        if (len(temMaisFilhos) > 0):
                            if (type(temMaisFilhos) is str):
                                resp.append(temMaisFilhos) 
                            elif (type(temMaisFilhos is list)):
                                resp + temMaisFilhos
            return resp
        else:
            return ''

    def selecionarAxiomasPorConceptID(self, identificador):
        """ Ao receber um conceptID identifica quais axiomas estao relacionados a este conceito

        Arguments:
            identificador {str} -- codigo ID do conceito

        Returns:
            list -- retorna um array de Axiomas 
        """ 
        if identificador:
            dataset = self.cursor.execute(""" select r.owlExpression
                                            from refset r
                                            where r.owlExpression like ? 
            """, ('%'+identificador+'%', )).fetchall()
        else: 
            dataset = ''
        return dataset

    def selecionarConceptIdsPorTermo(self, termo):
        """ Dado um termo por extenso: "heart attack", pesquisa seus IDs relacionados

        Args: 
            param1 (str): nome do conceito 

        Returns: 
            list: retorna array de IDs: ['266288001', '266288001', ...]
        """ 
        dataset = self.cursor.execute( """ select d.conceptId
                                           from description d
                                           where termOriginal like ?
                                           group by d.conceptId """, (termo, )
                                    ).fetchall()
        datasetSimples = []
        for d in dataset:
            datasetSimples.append(d[0])
        logging.info(f"O termo {termo} possui os seguintes conceptsIDs: {str(datasetSimples)}")
        return datasetSimples

    def selecionarIdPrincipal(self, IDs):
        """ Recebe um conjunto de ConceptsIDs e verifica qual deles deve ser considerado o principal
            na representação de uma determinada terminologia 

            Args: 
                param1 (array): Concept IDs

            Returns: 
                str: Um concept ID principal
        """
        if (IDs is not None) and (len(IDs) > 0): 
            sql = "select s.destinationId from statedrelationship s where s.destinationId in ({seq}) group by s.destinationId ".format(seq = ','.join(['?']*len(IDs))) 
            dataset = self.cursor.execute(sql, IDs).fetchone() 
            if (dataset is None):
                return ""
            else:
                return dataset[0]
        else: 
            return "" 

    def selecionarTermoPrincipal(self, IdPrincipal, idioma, tipoTermo):
        """ Dado um identificador, seleciona o descritor principal dentre os varios que respondem ao mesmo codigo
        
        Arguments:
            IdPrincipal {str} -- Exemplo: 22298006
            idioma {str} -- Exemplo: en
            tipoTermo {str} -- O = original e T = tratado
        
        Returns:
            str -- A descricao do termo 
        """        
        if (IdPrincipal): 
            if (tipoTermo == 'O'): #termo original
                dataSet = self.cursor.execute(""" select d.termOriginal 
                                                from description d 
                                                where (d.typeId = '900000000000003001') 
                                                    and (d.conceptId = ?) 
                                                    and (d.languageCode like ?) """, (IdPrincipal, idioma,)).fetchone()
            else: #termo tratado
                dataSet = self.cursor.execute(""" select d.termTratado 
                                                    from description d 
                                                    where (d.typeId = '900000000000003001') 
                                                        and (d.conceptId = ?) 
                                                        and (d.languageCode like ?) """, (IdPrincipal, idioma,)).fetchone()
            return dataSet[0]

    def selecionarDescricoesPorIDsConcept(self, IDs, tipoTermo, idioma):
        """ Recebe uma lista (array) de conceptIDs ou apenas um ID (str) e procura todas as descricoes associadas ao conjunto

            Args: 
                IDs {array ou str} -- Concept ID(s)
                tipoTermo {str} -- O = original e T = tratado
                idioma {str} -- en = ingles e es = espanhol
            
            Returns:
                array: Vários termos encapsulados em um array de string
        """
        if (IDs is not None) and (len(IDs) > 0) and (isinstance(IDs, list)): 
            while (len(IDs) > 999): 
                itemRemovido = IDs.pop(-1) 
                print("Item " + str(len(IDs)) + " para query descricao removido: " + itemRemovido) 
            if (tipoTermo == 'O'): #termo original
                sql = "select d.termOriginal from description d where (d.languageCode = \'{lang}\') and (d.conceptId in ({seq})) group by d.termOriginal ".format(lang = idioma, seq = ','.join(['?']*len(IDs))) 
            else: 
                sql = "select d.termTratado from description d where (d.languageCode = \'{lang}\') and (d.conceptId in ({seq})) group by d.termTratado ".format(lang = idioma, seq = ','.join(['?']*len(IDs))) 
            dataset = self.cursor.execute(sql, IDs).fetchall() 
            resposta = []
            for d in dataset:
                resposta.append(d[0]) 
            return resposta 
        else:
            if (IDs is not None) and (isinstance(IDs, str)): 
                if (tipoTermo == 'O'): #termo original
                    dataset = self.cursor.execute(""" 
                        select d.termOriginal 
                        from description d 
                        where d.languageCode = ? and d.conceptId = (?) 
                        group by d.termOriginal 
                        """, (idioma, IDs,)).fetchall() 
                else: 
                    dataset = self.cursor.execute(""" 
                        select d.termTratado 
                        from description d 
                        where d.languageCode = ? and d.conceptId = (?) 
                        group by d.termTratado 
                        """, (idioma, IDs,)).fetchall()
                resposta = []
                for d in dataset:
                    resposta.append(d[0])
                return resposta
            else:    
                return ""

    def extrairConceitoRelacionadoDoAxioma(self, axioma, objectproperyID, posini=0, listFinding = []):
        pos = axioma.find(objectproperyID, posini)
        if (pos == -1):
            return listFinding
        else:
            posiniconcept = axioma.find(':', pos)
            posfimconcept = axioma.find(')', pos)
            if (posiniconcept > 0 and posfimconcept > 0):
                concept = axioma[posiniconcept+1:posfimconcept]
                if concept.isnumeric():
                    listFinding.append(concept)
                    return self.extrairConceitoRelacionadoDoAxioma(axioma, objectproperyID, posfimconcept)
            else:
                return listFinding

    def selecionarIdPrincipalDoTermo(self, termo):
        """ A partir de um termo, seleciona-se o identificador principal

        Arguments:
            termo {str} -- Termo procurado
            tipoTermo {str} -- 'O' = original
            idioma {str} -- 'en' = ingles 

        Returns:
            str -- Codigo identificador 
        """   
        iDsRelacionados = self.selecionarConceptIdsPorTermo(termo)
        logging.info("SNOMED - termo: %s - idsRelacionados: %s" , str(termo), str(iDsRelacionados))
        return self.selecionarIdPrincipal(iDsRelacionados)

    def selecionarTermosProximosConceitualmente(self, iDPrincipal, tipoTermo, idioma):
        """ Dado um identificador, retorna um conjunto de termos de proximidade conceitual segundo SNOMED CT

        Arguments:
            iDPrincipal {str} -- Codigo do identificador
            tipoTermo {str} -- 'O' = original
            idioma {str} -- 'en' = ingles 

        Returns:
            list -- Um array com os nomes dos termos proximos conceitualmente
        """        
        termosProximosConceitualmente = self.selecionarDescricoesPorIDsConcept(iDPrincipal, tipoTermo, idioma) 
        logging.debug("SNOMED - idPrincipal: %s - termos proximos conceitualmente: %s", str(iDPrincipal), str(termosProximosConceitualmente))
        return termosProximosConceitualmente

    def selecionarTermosHierarquicos(self, iDPrincipal, tipoTermo, idioma):
        """ Dado um identificador, retorna um conjunto de termos na hierarquia da terminologia SNOMED CT 

        Arguments:
            iDPrincipal {str} -- Codigo do identificador
            tipoTermo {str} -- 'O' = original
            idioma {str} -- 'en' = ingles 

        Returns:
            list -- Um array com os nomes dos termos hierarquicos 
        """  
        iDsHierarquicos = []
        iDsHierarquicos.clear()
        termosHierarquicos = []
        termosHierarquicos.clear()

        iDsHierarquicos = self.hierarquiaDeIDsPorIdConcept(iDPrincipal)
        termosHierarquicos = self.selecionarDescricoesPorIDsConcept(iDsHierarquicos, tipoTermo, idioma)

        return termosHierarquicos

