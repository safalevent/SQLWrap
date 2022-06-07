from sqlite3 import PARSE_DECLTYPES, PARSE_COLNAMES

databaseName = None
dataPath = "data/"

detect_types = PARSE_DECLTYPES | PARSE_COLNAMES