from owlready2 import *
#from owlready2.pymedtermino2 import *
#from owlready2.pymedtermino2.umls import import_umls

def init():
    global SNOMEDCT_US
    default_world.set_backend(filename = "db-snomed.sqlite3")
    PYM = get_ontology("http://PYM/").load()
    SNOMEDCT_US = PYM["SNOMEDCT_US"]

def search_term_snomed(text, filename):
    global SNOMEDCT_US
    i = 0
    for concept in SNOMEDCT_US.descendant_concepts(): 
        i = i + 1
        if (text.find(concept.label.first()) != -1):
            print("* " + filename + " <- " + concept.label.first())
    print ("# Percorrido: " + str(i) + " iterações.")

        
