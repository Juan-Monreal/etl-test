import glob
import pandas as pd
from datetime import datetime
import os

tmpfile = "sifinETL_temp.tmp"
logfle = "sifinETL_logfile.txt"
targetFolder = "clean/"
dataFolder = "data/"


def createDir(path):
    """
    Function to create the required directories
    If directory not exist
    Using os library
    """
    try:
        if not os.path.exists(path):
            os.makedirs(path)
        else:
            return True
    except OSError:
        print(f"ERROR: Creating directory with name {path}")


def getFiles(extension, path):
    """
    Get all the files with the same extension
    :param extension: extension file to be obtained
    :param path: directory/path to check
    :return: List with the path of files
    """
    return [os.path.join(path, file)
            for file in os.listdir(path)
            if file.endswith(extension)]


def log(message):
    """
    Utility function to record the events occurring during the execution  of the script
    It stores using timestamp + message
    :param message:
    """
    timestamp_format = '%H:%M:%S-%h-%d-%Y'
    # Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now()  # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("sifinETL_logfile.txt", "a") as f:
        f.write(timestamp + ', ' + message + '\n')


def extract_from_csv(file_path):
    """
    Utility function to extract the info inside a file in the format .csv
    :param file_path: File containing the info to be extracted
    :return: Pandas dataframe with the extracted data
    """
    dataframe = pd.read_csv(file_path)
    return dataframe


def extract(data):
    """

    :param data:
    """
    pass


def transform(data):
    pass


def load(targetFile, data_to_load):
    data_to_load.to_csv(targetFile)


log("ETL Job Started")
log("Extract phase Started")
# extracted_data = extract() 
log("Extract phase Ended")

log("Transform phase Started")

log("Transform phase Ended")

log("Load phase Started")
# load(targetfile,transformed_data)
log("Load phase Ended")
