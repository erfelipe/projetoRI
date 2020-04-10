from inflection import singularize
from cleantext import clean

def textoSingular(texto):
    resp = singularize(texto)
    return resp

def textoNormalizado(texto):
    resp = clean(texto,
            fix_unicode=True,               # fix various unicode errors
            to_ascii=True,                  # transliterate to closest ASCII representation
            lower=True,                     # lowercase text
            no_line_breaks=True,            # fully strip line breaks as opposed to only normalizing them
            no_urls=False,                  # replace all URLs with a special token
            no_emails=False,                # replace all email addresses with a special token
            no_phone_numbers=False,         # replace all phone numbers with a special token
            no_numbers=False,               # replace all numbers with a special token
            no_digits=False,                # replace all digits with a special token
            no_currency_symbols=False,      # replace all currency symbols with a special token
            no_punct=False,                 # fully remove punctuation
            replace_with_url="<URL>",
            replace_with_email="<EMAIL>",
            replace_with_phone_number="<PHONE>",
            replace_with_number="",
            replace_with_digit="",
            replace_with_currency_symbol="<CUR>",
            lang="en"                       # set to 'de' for German special handling
    )
    return resp
    
def validaHierarquiaDescritores(idhierarq, lista):
    resposta = []
    tamanho = len(idhierarq)
    for item in lista:
        id = item[1]
        if ( (len(id)) <= (tamanho + 3) ):
            resposta.append(item[0])
    return resposta

