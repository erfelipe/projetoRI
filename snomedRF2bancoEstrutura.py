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

    def hierarquiaDeIDsPorIdConcept(self, IdConcept, resp = []):
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

        if (len(dataSetAxioma) <= 0) or (len(resp) > constantes.LIMITE_HIERARQUICO):
            return resp
        axAbout = ''
        codigos = []
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
                        ind = espaco + 1 
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
                temMaisFilhos = self.hierarquiaDeIDsPorIdConcept(axAbout)
                if (len(temMaisFilhos) > 0) and (type(temMaisFilhos) is str):
                    resp.append(temMaisFilhos)
        return resp

    #dado um ID, encontrar o(s) axioma(s) associado(s)
    def selecionarAxiomasPorConceptID(self, identificador):
        """ Ao receber um conceptID identifica quais axiomas estao relacionados a este conceito

            Args:
                param1 (str): codigo ID do conceito

            Returns:
                list: retorna um array de Axiomas 
        """
        dataset = self.cursor.execute(""" select r.owlExpression
                                        from refset r
                                        where r.owlExpression like ? 
        """, ('%'+identificador+'%', )).fetchall()
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
                                           and active = 1
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

    def selecionarDescricoesPorIDsConcept(self, IDs):
        """ Recebe uma lista (array) de conceptIDs e procura todas as descricoes associadas ao conjunto

            Args: 
                param1 (array ou str): Concept ID(s)

            Returns:
                array: Vários termos encapsulados em um array de string
        """
        if (IDs is not None) and (len(IDs) > 0) and (isinstance(IDs, list)): 
            while (len(IDs) > 999): 
                itemRemovido = IDs.pop(-1) 
                print("Item " + str(len(IDs)) + " para query descricao removido: " + itemRemovido) 
            sql = "select d.term from description d where d.conceptId in ({seq}) group by d.term ".format(seq = ','.join(['?']*len(IDs))) 
            dataset = self.cursor.execute(sql, IDs).fetchall()
            resposta = []
            for d in dataset:
                resposta.append(d[0]) 
            return resposta 
        else:
            if (IDs is not None) and (isinstance(IDs, str)):
                dataset = self.cursor.execute(""" 
                    select d.term 
                    from description d 
                    where d.conceptId = (?) 
                    group by d.term 
                    """, (IDs,)).fetchall() 
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

    def criarBancoDeDados(self): 
        """ Cria a estrutura do banco de dados
        """ 
        self.cursor.execute("""  CREATE TABLE if not exists concept 
                            (id   text NOT NULL, 
                             effectiveTime text NOT NULL,
                             active integer NOT NULL,
                             moduleId text NOT NULL,
                             definitionStatusId text NOT NULL ); 
                    """) 
        
        self.cursor.execute("""  CREATE TABLE if not exists description 
                            (id   text NOT NULL,
                             effectiveTime   text NOT NULL,
                             active integer NOT NULL,
                             moduleID text NOT NULL,
                             conceptId text NOT NULL, 
                             languageCode text NOT NULL,
                             typeId text NOT NULL,
                             termOriginal text NOT NULL,
                             termTratado text NOT NULL,
                             caseSignificanceId text NOT NULL);
                    """) 

        self.cursor.execute(""" CREATE TABLE if not exists relationship 
                            (id text NOT NULL,
                            effectiveTime text NOT NULL,
                            active integer NOT NULL, 
                            moduleId text NOT NULL,
                            sourceId text NOT NULL, 
                            destinationId text NOT NULL, 
                            relationshipGroup text NOT NULL, 
                            typeId text NOT NULL, 
                            characteristicTypeId text NOT NULL, 
                            modifierId text NOT NULL );
                    """)

        self.cursor.execute(""" CREATE TABLE if not exists refset
                            (id text NOT NULL, 
                            effectiveTime text NOT NULL, 
                            active integer NOT NULL, 
                            moduleId text NOT NULL, 
                            refsetId text NOT NULL,
                            referencedComponentId text NOT NULL,
                            owlExpression text NOT NULL );
                    """)

        self.cursor.execute(""" CREATE TABLE if not exists statedrelationship
                            (id text NOT NULL, 
                            effectiveTime text NOT NULL, 
                            active integer NOT NULL, 
                            moduleId text NOT NULL, 
                            sourceId text NOT NULL, 
                            destinationId text NOT NULL,
                            relationshipGroup text NOT NULL, 
                            typeId text NOT NULL, 
                            characteristicTypeId text NOT NULL, 
                            modifierId text NOT NULL );
                    """)

        self.cursor.execute(""" CREATE TABLE if not exists textdefinition
                            (id text NOT NULL, 
                            effectiveTime text NOT NULL, 
                            active integer NOT NULL, 
                            moduleId text NOT NULL, 
                            conceptId text NOT NULL,
                            languageCode text NOT NULL, 
                            typeId text NOT NULL,
                            term text NOT NULL,
                            caseSignificanceId text NOT NULL );
                    """)

        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_description_term ON description (termTratado);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_description_termOriginal ON description (termOriginal);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_relationship_destId ON relationship (destinationId);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_relationship_srcId ON relationship (sourceId);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_statedrelationship_destId ON statedrelationship (destinationId);")

    def inserirConcept(self, tupla):
        """ Insere registros na tabela concept 

            Args: 
                param1 (array): registro lido do arquivo txt

            Returns:
                void
        """ 
        id = tupla[0]
        effectiveTime = preProcessamentoTextual.dateToTimeString(tupla[1])
        active = tupla[2]
        moduleId = tupla[3]
        definitionStatusId = tupla[4]
        """ Por motivos de performance farei insercao direta, sem validacao """
        if (active == '1'):
            try:
                self.cursor.execute(" INSERT INTO concept (id, effectiveTime, active, moduleId, definitionStatusId) VALUES (?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, definitionStatusId, ))
            except Exception as identifier:
                print('* Erro na inserção do ID' + id + identifier)

    def inserirDescription(self, tupla):
        """ Insere registros na tabela description 

            Args: 
            param1 (array): registro lido do arquivo txt 

            Returns:
            void
        """         
        id = tupla[0]
        effectiveTime = preProcessamentoTextual.dateToTimeString(tupla[1])
        active = tupla[2]
        moduleId = tupla[3]
        conceptId = tupla[4]
        languageCode = tupla[5]
        typeId = tupla[6]
        termOriginal = tupla[7]
        termTratado = preProcessamentoTextual.trataDescricao(tupla[7])
        caseSignificanceId = tupla[8]
        """ Por motivos de performance farei insercao direta, sem validacao """
        if (active == '1'):
            try:
                self.cursor.execute(" INSERT INTO description (id, effectiveTime, active, moduleId, conceptId, languageCode, typeId, termOriginal, termTratado, caseSignificanceId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, conceptId, languageCode, typeId, termOriginal.lower(), termTratado.lower(), caseSignificanceId, ))
            except Exception as identifier:
                print('* Erro na inserção do ID' + id + identifier)
        
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
                print('* Erro na inserção do ID' + id + identifier)

    def inserirSrefSet(self, tupla):
        id = tupla[0]
        effectiveTime = preProcessamentoTextual.dateToTimeString(tupla[1])
        active = tupla[2]
        moduleId = tupla[3]
        refsetId = tupla[4]
        referencedComponentId = tupla[5]
        owlExpression = tupla[6]
        """ Por motivos de performance farei insercao direta, sem validacao """
        if (active == '1'):
            try:
                self.cursor.execute(" INSERT INTO refset (id, effectiveTime, active, moduleId, refsetId, referencedComponentId, owlExpression) VALUES (?, ?, ?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, refsetId, referencedComponentId, owlExpression, ))
            except Exception as identifier:
                print('* Erro na inserção do ID' + id + identifier)

    def inserirStatedRelationShip(self, tupla):
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
        try:
            self.cursor.execute(" INSERT INTO statedrelationship (id, effectiveTime, active, moduleId, sourceId, destinationId, relationshipGroup, typeId, characteristicTypeId, modifierId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, sourceId, destinationId, relationshipGroup, typeId, characteristicTypeId, modifierId, ))
        except Exception as identifier:
            print('* Erro na inserção do ID' + id + identifier)

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
        elif tabela == 'relationship':
            self.inserirRelationShip(linha)
        elif tabela == 'srefset':
            self.inserirSrefSet(linha)
        elif tabela == 'statedrelationship':
            self.inserirStatedRelationShip(linha)
        elif tabela == 'textdefinition':
            self.inserirTextDefinition(linha)

