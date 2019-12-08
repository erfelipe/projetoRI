from owlready2 import *
from owlready2.pymedtermino2.umls import import_umls

default_world.set_backend(filename = "pym.sqlite3")
import_umls("/Users/eduardofelipe/Documents/Ontologias/Umls/umls-2019AA-full.zip", terminologies = ["SNOMEDCT_US"])
default_world.save()

