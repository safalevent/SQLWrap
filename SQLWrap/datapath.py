from os import path, makedirs
from .constants import dataPath

def get_datafile_path(fileName):
    """Creates file if needed and gives the path of the file in data folder."""
    if not path.exists(dataPath):
        makedirs(dataPath, exist_ok=True)
        
    filePath = dataPath + fileName
    if (not path.exists(filePath)):
        file = open(filePath, "w")
        file.close()

    return path.abspath(filePath)