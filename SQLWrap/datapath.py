from os import path, makedirs
from .constants import data_path

def get_datafile_path(fileName):
    """Creates file if needed and gives the path of the file in data folder."""
    if not path.exists(data_path):
        makedirs(data_path, exist_ok=True)
        
    filePath = data_path + fileName
    if (not path.exists(filePath)):
        file = open(filePath, "w")
        file.close()

    return path.abspath(filePath)