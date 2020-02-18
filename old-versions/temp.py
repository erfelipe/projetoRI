import re 

def suprimirSubstringPorLimitadores(text, ini, fim):
    if (ini == fim):
        posini = text.find(ini)
        posfim = text.rfind(fim, posini+1)
    else:
        posini = text.find(ini)
        posfim = text.rfind(fim)
    if (posini < 0 or posfim < 0):
        return text.strip()
    elif ((posini >= 0 and posfim >= 0) and (posfim > posini)):
        text = text[:posini] + text[posfim+1:]
        return suprimirSubstringPorLimitadores(text, ini, fim)

def suprimirHifenInicioeFim(text):
    if not (text.startswith('-') or text.endswith('-')):
        return text.strip()
    elif text.startswith('-'):
        text = text[1:]
        return suprimirHifenInicioeFim(text)
    elif text.endswith('-'):
        text = text[0:len(text)-1]
        return suprimirHifenInicioeFim(text)

def trataDescricao(text):
    #selecionar apenas o primeiro termo da lista com separador vírgula 
    resp = ''
    listastr = text.split(',')
    i = 0
    for it in listastr:
        if not (it.isnumeric()):
            resp = listastr[i].strip()
            break
        i+=1

    #suprimir palavras entre parênteses, entre ^^, entre ><, entre [] 
    resp = suprimirSubstringPorLimitadores(resp, '(', ')')
    resp = suprimirSubstringPorLimitadores(resp, '[', ']')
    resp = suprimirSubstringPorLimitadores(resp, '>', '<')
    resp = suprimirSubstringPorLimitadores(resp, '^', '^')

    #suprimir caracteres numéricos e 
    resp = ''.join(i for i in resp if not i.isdigit())

    #suprimir termos particulares (-RETIRED- ; mm ; NOS; O/E)
    resp = resp.replace('-RETIRED-', '').replace('mm', '').replace('NOS', '').replace('O/E', '').replace('&/or', '')

    #tratamento de caracteres especiais (^, <, >, :, ',', ';', &, '/', '%') [exceto hífen]
    #suprimir a palavra que contém esses caracteres? 
    resp = resp.replace('^', '').replace('<', '').replace('>', '').replace(':', '').replace(',', '').replace(';', '').replace('&', '').replace('/', '').replace('%', '')

    #retirar dois ou mais espaços em sequencia e dois ou mais hífens
    resp = re.sub("[ ]{2,}", " ", resp)
    resp = re.sub("[-]{2,}", " ", resp)

    #retirar espaços antes e após (trimmer)
    resp = resp.strip()

    #termo nao pode começar ou terminar com hífen ou caracter especial e não pode ter dois ou mais hífens juntos 
    resp = suprimirHifenInicioeFim(resp)

    #apos todas as regras, validar se há string vazia como resultado! 
    if not resp:
        return text
    else:
        return resp

if __name__ == "__main__":
    listagem = [
                'Oxoglutarate dehydrogenase (NADP+) (NAD(P)) (phosphorylating)',
                '^202^Thallium NOS-',
                'Salmonella II 9 nos,12,(46),27:g,t:e,n,x',
                'Blood group antibody LW>3<',
                'Salmonella III arizonae 18:r:z',
                '5,8,11,14-Eicosatetraenoic acid',
                '3-3''Dichlorobenzidine',
                '1,25-Dihydroxy cholecalciferol',
                '1-alpha, 25-Dihydroxy cholecalciferol',
                'Salmonella II 56:d:--',
                '20alpha-Hydroxysteroid dehydrogenase',
                '17beta,20alpha-Hydroxy-steroid dehydrogenase',
                '5-Oxopent-3-ene-1,2,5-tricarboxylate',
                '2,4,6-Trinitrophenol',
                '[D]Echocardiogram abnormal',
                'Medical attendant(s) &/or orderly',
                'Drainage: [colon] or [pericolonic tissue]',
                'Incision of colon NOS',
                'Basic oestrogen: 17-B-oestriol',
                'O/E - weight NOS',
                'O/E -weight 10-20% below ideal',
                'O/E - arterial wall normal',
                '[M]Carcinoma, metastatic, NOS (morphologic abnormality)',
                '[X]Pedestrian injured in collision with two- or three-wheeled motor vehicle, traffic accident (finding)',
                '[EDTA] Traumatic or surgical loss of kidney associated with renal failure (disorder)',
                'T11 - T-cell lineage 11',
                'Omega-5 gliadin IgE (immunoglobulin E) level',
                'pT3a category',
                'Glyceraldehyde-3-phosphate dehydrogenase (NAD(P)) (phosphorylating)'
                ]
    for l in listagem:
        textotratado = trataDescricao(l)
        print (l, ' || ', textotratado)

