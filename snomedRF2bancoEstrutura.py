import sqlite3
import re
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

    def dateToTimeString(self, dt):
        resp = dt[:4] + '-' + dt[4:6] + '-' + dt[6:]
        return resp

    def suprimirSubstringComLimitadores(self, text, ini, fim):
        if not text:
            return 'V A Z I O'
        if (ini == fim):
            posini = text.find(ini)
            posfim = text.rfind(fim, posini+1)
        else:
            posini = text.find(ini)
            posfim = text.rfind(fim)
        if (posini < 0 or posfim < 0):
            return text.strip()
        elif ((posini >= 0 and posfim >= 0) and (posfim+1 > posini)):
            text = text[:posini] + text[posfim+1:]
            return self.suprimirSubstringComLimitadores(text, ini, fim)
        else: 
            return text

    def suprimirHifenInicioeFim(self, text):
        if not (text.startswith('-') or text.endswith('-')):
            return text.strip()
        elif text.startswith('-'):
            text = text[1:]
            return self.suprimirHifenInicioeFim(text)
        elif text.endswith('-'):
            text = text[0:len(text)-1]
            return self.suprimirHifenInicioeFim(text)

    def trataDescricao(self, text):
        #selecionar apenas o primeiro termo da lista com separador virgula 
        resp = ''
        listastr = text.split(',')
        i = 0
        for it in listastr:
            if not (it.isnumeric()):
                resp = listastr[i].strip()
                break
            i+=1

        #suprimir palavras entre parenteses, entre ^^, entre ><, entre [] 
        resp = self.suprimirSubstringComLimitadores(resp, '(', ')')
        resp = self.suprimirSubstringComLimitadores(resp, '[', ']')
        resp = self.suprimirSubstringComLimitadores(resp, '>', '<')
        resp = self.suprimirSubstringComLimitadores(resp, '^', '^')

        #suprimir caracteres numericos 
        resp = ''.join(i for i in resp if not i.isdigit())

        #suprimir termos particulares (-RETIRED-  ; NOS; O/E) 
        #replace('mm', '') nao pode pq afetou palavras que possuem mm
        resp = resp.replace('-RETIRED-', '').replace('NOS', '').replace('O/E', '').replace('&/or', '')

        #tratamento de caracteres especiais (^, <, >, :, ',', ';', &, '/', '%') [exceto hifen]
        #suprimir a palavra que contem esses caracteres? 
        resp = resp.replace('#', '').replace('%', '').replace('\'-', '').replace('/', '').replace('\'', '').replace('\"', '').replace('^', '').replace('<', '').replace('>', '').replace(':', '').replace(',', '').replace(';', '').replace('&', '').replace('(', '').replace(')', '').replace('*', '').replace('.', '').replace('-', '').replace('?', '').replace('+', '').replace('|', '')

        #retirar dois ou mais espacos em sequencia e dois ou mais hifens
        resp = re.sub("[ ]{2,}", " ", resp)
        resp = re.sub("[-]{2,}", " ", resp)

        #retirar espacos antes e apos (trimmer)
        resp = resp.strip()

        #termo nao pode comecar ou terminar com hifen ou caracter especial e nao pode ter dois ou mais hifens juntos 
        resp = self.suprimirHifenInicioeFim(resp)

        #apos todas as regras, validar se ha string vazia como resultado! 
        if not resp:
            return text
        else:
            return resp

# *******************************************************************************************************

    def hierarquiaDeIDsPorIdConcept(self, IdConcept, resp = []):
        """ Dado um ConceptId: 22298006, pesquisa seus IDs lidados a ele hierarquicamente

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

        if len(dataSetAxioma) <= 0:
            return resp

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
                resp.append(axAbout) 
                temMaisFilhos = self.hierarquiaDeIDsPorIdConcept(axAbout)
                if (len(temMaisFilhos) > 0) and (type(temMaisFilhos) == str):
                    resp.append(temMaisFilhos)
        return resp

    #dado um ID, encontrar o(s) axioma(s) associado(s)
    def selecionarAxiomasPorConceptID(self, identificador):
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
            return dataset[0]
        else: 
            return "" 

    def selecionarDescricoesPorIDsConcept(self, IDs):
        if (IDs is not None) and (len(IDs) > 0):
            sql = "select d.term from description d where d.conceptId in ({seq}) and d.active = 1 group by d.term ".format(seq = ','.join(['?']*len(IDs))) 
            dataset = self.cursor.execute(sql, IDs).fetchall()
            return dataset 
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

# *******************************************************************************************************

    def criarBancoDeDados(self): 
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
                             term text NOT NULL,
                             caseSignificanceId text NOT NULL,
                             correspondenciaMeSH text NOT NULL,
                             correspondenciaMeSHoriginal text NOT NULL );
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

        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_description_term ON description (term);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_description_termOriginal ON description (termOriginal);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_relationship_destId ON relationship (destinationId);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_relationship_srcId ON relationship (sourceId);")
        self.cursor.execute("CREATE INDEX IF NOT EXISTS idx_statedrelationship_destId ON statedrelationship (destinationId);")

# *******************************************************************************************************

    def inserirConcept(self, tupla):
        id = tupla[0]
        effectiveTime = self.dateToTimeString(tupla[1])
        active = tupla[2]
        moduleId = tupla[3]
        definitionStatusId = tupla[4]
        """ Por motivos de performance farei insercao direta, sem validacao """
        if (active == '1'):
            try:
                print('concept', tupla)
                self.cursor.execute(" INSERT INTO concept (id, effectiveTime, active, moduleId, definitionStatusId) VALUES (?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, definitionStatusId, ))
            except Exception as identifier:
                print('* Erro na inserção do ID' + id + identifier)

    def inserirDescription(self, tupla):
        id = tupla[0]
        effectiveTime = self.dateToTimeString(tupla[1])
        active = tupla[2]
        moduleId = tupla[3]
        conceptId = tupla[4]
        languageCode = tupla[5]
        typeId = tupla[6]
        termOriginal = tupla[7]
        term = self.trataDescricao(tupla[7])
        caseSignificanceId = tupla[8]
        correspondenciaMeSH = 'N' #default nao
        correspondenciaMeSHoriginal = 'N' #default nao
        """ Por motivos de performance farei insercao direta, sem validacao """
        if (active == '1'):
            try:
                print('description' , tupla)
                self.cursor.execute(" INSERT INTO description (id, effectiveTime, active, moduleId, conceptId, languageCode, typeId, termOriginal, term, caseSignificanceId, correspondenciaMeSH, correspondenciaMeSHoriginal) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, conceptId, languageCode, typeId, termOriginal, term, caseSignificanceId, correspondenciaMeSH, correspondenciaMeSHoriginal, ))
            except Exception as identifier:
                print('* Erro na inserção do ID' + id + identifier)
        
    def inserirRelationShip(self, tupla):
        id = tupla[0]
        effectiveTime = self.dateToTimeString(tupla[1])
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
                print('relationship', tupla) 
                self.cursor.execute(" INSERT INTO relationship (id, effectiveTime, active, moduleId, sourceId, destinationId, relationshipGroup, typeId, characteristicTypeId, modifierId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, sourceId, destinationId, relationshipGroup, typeId, characteristicTypeId, modifierId, ))
            except Exception as identifier:
                print('* Erro na inserção do ID' + id + identifier)

    def inserirSrefSet(self, tupla):
        id = tupla[0]
        effectiveTime = self.dateToTimeString(tupla[1])
        active = tupla[2]
        moduleId = tupla[3]
        refsetId = tupla[4]
        referencedComponentId = tupla[5]
        owlExpression = tupla[6]
        """ Por motivos de performance farei insercao direta, sem validacao """
        if (active == '1'):
            try:
                print('refset', tupla)
                self.cursor.execute(" INSERT INTO refset (id, effectiveTime, active, moduleId, refsetId, referencedComponentId, owlExpression) VALUES (?, ?, ?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, refsetId, referencedComponentId, owlExpression, ))
            except Exception as identifier:
                print('* Erro na inserção do ID' + id + identifier)

    def inserirStatedRelationShip(self, tupla):
        id = tupla[0]
        effectiveTime = self.dateToTimeString(tupla[1])
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
            print('statedrelationship', tupla)
            self.cursor.execute(" INSERT INTO statedrelationship (id, effectiveTime, active, moduleId, sourceId, destinationId, relationshipGroup, typeId, characteristicTypeId, modifierId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, sourceId, destinationId, relationshipGroup, typeId, characteristicTypeId, modifierId, ))
        except Exception as identifier:
            print('* Erro na inserção do ID' + id + identifier)

    def inserirTextDefinition(self, tupla):
        id = tupla[0]
        effectiveTime = self.dateToTimeString(tupla[1])
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
                print('textdefinition', tupla)
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

