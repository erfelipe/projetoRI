from owlready2 import *
import elasticsearch 
import json

def main():
    itensTotais = 0
    itensMatch = 0
    es = elasticsearch.Elasticsearch()
    es.indices.open("articles")

    default_world.set_backend(filename = "db-snomed.sqlite3")
    PYM = get_ontology("http://PYM/").load()
    SNOMEDCT_US = PYM["SNOMEDCT_US"]
    compElastic = {}    
    for concept in SNOMEDCT_US.descendant_concepts(): 
        itensTotais += 1
        label = concept.label.first().lower()
        #result = es.search(index="articles", body={"track_total_hits": True, "query": {"match_phrase" : {"dcDescription" : label}}})
        result = es.search(index="articles", body={"track_total_hits": True, "query": {"multi_match" : {"query": label, "type": "match_phrase", "fields": [ "dcTitle", "dcDescription" ]}}})
        if (result['hits']['total']['value'] > 0):
            itensMatch += 1
            compElastic[label] = result['hits']['total']['value']
            print(label)
            print(result['hits']['total']['value'])
            print("---------")
    
    compElastic['itensTotais'] = itensTotais
    compElastic['itensMatch'] = itensMatch

    jsonlist = json.dumps(compElastic)
    f = open("comparacao-snomedElastic.json","w")
    f.write(jsonlist)
    f.close()

if __name__ == "__main__":
    main()