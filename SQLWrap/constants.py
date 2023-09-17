from sqlite3 import PARSE_DECLTYPES, PARSE_COLNAMES

database_name = None
data_path = "data/"

detect_types = PARSE_DECLTYPES | PARSE_COLNAMES