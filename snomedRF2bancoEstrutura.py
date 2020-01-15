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

        #suprimir termos particulares (-RETIRED- ; mm ; NOS; O/E)
        resp = resp.replace('-RETIRED-', '').replace('mm', '').replace('NOS', '').replace('O/E', '').replace('&/or', '')

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
    
    #dado um ID, encontrar o(s) axioma(s) associado(s)
    def selecionarAxiomasPorConceptID(self, identificador):
        dataset = self.cursor.execute(""" select r.owlExpression
                                        from refset r
                                        where r.owlExpression like ? 
        """, ('%'+identificador+'%', )).fetchall()
        return dataset

    #dado um termo por extenso: "heart attack"
    #retorna array complexo: [('Acute myocardial infarction', '266288001'), ('Attack - heart', '266288001'), ...]
    def selecionarListaDeTermosPorNome(self, nome):
        dataset = self.cursor.execute( """ select d.term, d.conceptId
                                        from description as d 
                                        where conceptId in (select d.conceptId
                                                            from description d
                                                            where term like (?)
                                                            and active = 1
                                                            group by d.conceptId)
                                        and d.active = 1 
                                        group by d.term """, (nome, )
                                    ).fetchall()
        return dataset 

    def selecionarListaDeTermosPorCodigo(self, lstCodigos):
        if (lstCodigos is not None) and (len(lstCodigos) > 0):
            codigos = ",".join(lstCodigos)
            dataset = self.cursor.execute( "select d.term from description d where d.conceptId in (?) and d.active = 1 group by d.term ", (codigos, )).fetchall()
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

# Ao apontar para um destinationId o sourceId representa o descendente
    def extrairTermosHierarquicosPorTermoOriginal(self, idTermo, resp = []):
        dataset = self.cursor.execute(""" select d.conceptId, d.termOriginal
                                        from description as d 
                                        where conceptId in (select r.sourceId 
                                                            from relationship r 
                                                            where r.destinationId = ?)	
                                        and d.active = 1  
                                        group by d.conceptId, d.termOriginal 
                                    """, (idTermo, ) ).fetchall()
        if (len(dataset) > 0):
            resp = resp + dataset
            for item in dataset:
                self.extrairTermosHierarquicosPorTermoOriginal(item[0])
        else: 
            return resp

    def extrairTermosHierarquicosPorTermoTratado(self, idTermo):
        dataset = self.cursor.execute(""" select d.term
                                        from description as d 
                                        where conceptId in (select r.sourceId 
                                                            from relationship r 
                                                            where r.destinationId = ?)	
                                        and d.active = 1  
                                        group by d.term 
                                    """, (idTermo, ) ).fetchall()
        return dataset

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

        self.cursor.execute("CREATE INDEX idx_description_term ON description (term);")
        self.cursor.execute("CREATE INDEX idx_description_termOriginal ON description (termOriginal);")
        self.cursor.execute("CREATE INDEX idx_relationship_destId ON relationship (destinationId);")
        self.cursor.execute("CREATE INDEX idx_relationship_srcId ON relationship (sourceId);")

# *******************************************************************************************************

    def inserirConcept(self, tupla):
        id = tupla[0]
        effectiveTime = self.dateToTimeString(tupla[1])
        active = tupla[2]
        moduleId = tupla[3]
        definitionStatusId = tupla[4]
        """ Por motivos de performance farei insercao direta, sem validacao """
        self.cursor.execute(" INSERT INTO concept (id, effectiveTime, active, moduleId, definitionStatusId) VALUES (?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, definitionStatusId, ))
        print('concept', tupla)

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
        self.cursor.execute(" INSERT INTO description (id, effectiveTime, active, moduleId, conceptId, languageCode, typeId, termOriginal, term, caseSignificanceId, correspondenciaMeSH, correspondenciaMeSHoriginal) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, conceptId, languageCode, typeId, termOriginal, term, caseSignificanceId, correspondenciaMeSH, correspondenciaMeSHoriginal, ))
        print('description' , tupla)

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
        self.cursor.execute(" INSERT INTO relationship (id, effectiveTime, active, moduleId, sourceId, destinationId, relationshipGroup, typeId, characteristicTypeId, modifierId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, sourceId, destinationId, relationshipGroup, typeId, characteristicTypeId, modifierId, ))
        print('relationship', tupla)        

    def inserirSrefSet(self, tupla):
        id = tupla[0]
        effectiveTime = self.dateToTimeString(tupla[1])
        active = tupla[2]
        moduleId = tupla[3]
        refsetId = tupla[4]
        referencedComponentId = tupla[5]
        owlExpression = tupla[6]
        """ Por motivos de performance farei insercao direta, sem validacao """
        self.cursor.execute(" INSERT INTO refset (id, effectiveTime, active, moduleId, refsetId, referencedComponentId, owlExpression) VALUES (?, ?, ?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, refsetId, referencedComponentId, owlExpression, ))
        print('refset', tupla)        

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
        self.cursor.execute(" INSERT INTO statedrelationship (id, effectiveTime, active, moduleId, sourceId, destinationId, relationshipGroup, typeId, characteristicTypeId, modifierId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, sourceId, destinationId, relationshipGroup, typeId, characteristicTypeId, modifierId, ))
        print('statedrelationship', tupla)        

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
        self.cursor.execute(" INSERT INTO textdefinition (id, effectiveTime, active, moduleId, conceptId, languageCode, typeId, term, caseSignificanceId) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", (id, effectiveTime, active, moduleId, conceptId, languageCode, typeId, term, caseSignificanceId, ))
        print('statedrelationship', tupla)        

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

