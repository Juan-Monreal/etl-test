import glob 
import pandas as pd 
from datetime import datetime


tmpfile = "sifinETL_temp.tmp"
logfle = "sifinETL_logfile.txt"
targetFolder = "clean/"

def log(message):
    timestamp_format = '%H:%M:%S-%h-%d-%Y'
    #Hour-Minute-Second-MonthName-Day-Year
    now = datetime.now() # get current timestamp
    timestamp = now.strftime(timestamp_format)
    with open("sifinETL_logfile.txt","a") as f: f.write(timestamp + ', ' + message + '\n')

def extract_from_csv(file_path):
	dataframe = pd.read_csv(file_path)
	return dataframe


def extract(data):
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