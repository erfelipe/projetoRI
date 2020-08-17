DRIVE = "/Users/eduardofelipe/workspace/dadosParaRI/"

#bancos de dados
BD_SQL_ESTATISTICA = DRIVE + "BancosSQL/db-estatistica.sqlite3"
BD_SQL_MESH = DRIVE + "BancosSQL/db-MeSH.sqlite3"
BD_SQL_SNOMED = DRIVE + "BancosSQL/db-snomed-RF2.sqlite3"

#terminologias medicas
TERMINOLOGIA_SNOMED_PATH = DRIVE + "SnomedCT_InternationalRF2_PRODUCTION_20200309T120000Z/Snapshot/Terminology/"
TERMINOLOGIA_MESH_XML = DRIVE + "MeSH2020_/desc2020.xml"

#deprecated 
TERMOS_COMUNS_JSON = DRIVE + "Utils/termosComuns.json"

#descritores/termos comuns em ambas terminologias
MESH_DESCRITORES_COMUNS_ORIGINAIS = DRIVE + "Utils/descritoresComunsOriginais.txt"
MESH_DESCRITORES_COMUNS_TRATADOS = DRIVE + "Utils/descritoresComunsTratados.txt" 

#arquivo de log para debug
LOG_FILE = DRIVE + "Utils/log.txt"
LOG_PREPROCESSAMENTOTEXTUAL = DRIVE + "Utils/logPreProcessamentoTextual.txt"

#limites para hierarquia terminologia dos instrumentos
LIMITE_HIERARQUICO = 900

#artigos
PDF_ARTIGOS = "/Volumes/SD-64-Interno/artigosPDFbmc" 
