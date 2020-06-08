DRIVE = "/Volumes/Backup/backupMacBook/"

#bancos de dados
BD_SQL_ESTATISTICA = DRIVE + "SD-64-Interno/BancosSQL/db-estatistica.sqlite3"
BD_SQL_MESH = DRIVE + "SD-64-Interno/BancosSQL/db-MeSH.sqlite3"
BD_SQL_SNOMED = DRIVE + "SD-64-Interno/BancosSQL/db-snomed-RF2.sqlite3"

#terminologias medicas
TERMINOLOGIA_SNOMED_PATH = DRIVE + "SD-64-Interno/SnomedCT_SpanishRelease-es_PRODUCTION_20191031T120000Z/RF2Release/Snapshot/Terminology/"
TERMINOLOGIA_MESH_XML = DRIVE + "SD-64-Interno/MeSH2020/desc2020.xml"

#deprecated 
TERMOS_COMUNS_JSON = DRIVE + "SD-64-Interno/Utils/termosComuns.json"

#descritores/termos comuns em ambas terminologias
MESH_TERMOS_COMUNS_ORIGINAIS = DRIVE + "SD-64-Interno/Utils/termosComunsOriginais.txt"
MESH_TERMOS_COMUNS_TRATADOS = DRIVE + "SD-64-Interno/Utils/termosComunsTratados.txt"
MESH_DESCRITORES_COMUNS_ORIGINAIS = DRIVE + "SD-64-Interno/Utils/descritoresComunsOriginais.txt"
MESH_DESCRITORES_COMUNS_TRATADOS = DRIVE + "SD-64-Interno/Utils/descritoresComunsTratados.txt"

#arquivo de log para debug
LOG_FILE = DRIVE + "SD-64-Interno/Utils/log.txt"
LOG_PREPROCESSAMENTOTEXTUAL = DRIVE + "SD-64-Interno/Utils/PreProcessamentoTextual.txt"

#limites para hierarquia terminologia dos instrumentos
LIMITE_HIERARQUICO = 900

#artigos
PDF_ARTIGOS = DRIVE + "SD-64-Interno/artigosPDFbmc" 
