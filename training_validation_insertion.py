from datetime import datetime
from Training_RawDataValidation.DataValidation import Raw_Data_Validation
from Training_DataTransformation.DataTransformation import dataTransform
from Training_DataTypeValidation_Insertion.DataTypeValidation import DBOperation
from Application_Logging.logger import App_Logger

class train_validation:
    def __init__(self,path):
        self.raw_data = Raw_Data_Validation(path)
        self.dataTransform = dataTransform()
        self.DBOperation = DBOperation()
        self.file_object= open("Training_Logs/Training_Main_Log.txt", 'a+')
        self.log_writer = App_Logger()


    def train_validation(self):
        try:
            self.log_writer.log(self.file_object, " Start of validation of files for training!!")
            # extracting values from prediction schema
            LengthOfFirstWordInFile, LengthOfSecondWordInFile, ColName, NumberOfColumns = self.raw_data.valuesFromSchema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex, LengthOfFirstWordInFile, LengthOfSecondWordInFile)
            # validating `column `length in the file
            self.raw_data.validateColumnLength(NumberOfColumns)
            # validating if any column has all values missing
            self.raw_data.validateMissingValuesinWholeColumn()
            self.log_writer.log(self.file_object, "Raw Data Validation Complete!!")

            self.log_writer.log(self.file_object,"Raw Data Transformation Started!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dataTransform.renameColumns()
            self.log_writer.log(self.file_object, "Raw Data Transformation Completed!!")





            self.log_writer.log(self.file_object, "Creating Training_Database and tables on the basis of given schema!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.DBOperation.createTableDB('training')
            self.log_writer.log(self.file_object, "Table creation Completed!!")
            self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            # insert csv files in the table
            self.DBOperation.insertIntoTableGoodData('training')
            self.log_writer.log(self.file_object, "Insertion in Table completed!!!")
            self.log_writer.log(self.file_object, "Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDataTrainingFolder()
            self.log_writer.log(self.file_object, "Good_Data folder deleted!!!")
            self.log_writer.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.file_object, "Validation Operation completed!!")
            self.log_writer.log(self.file_object, "Extracting csv file from table")
            # export data in table to csvfile
            self.DBOperation.selectingDatafromtableintocsv('training')
            self.log_writer.log(self.file_object, "Extraction of table data to csv file is finished. ")
            self.file_object.close()

        except Exception as e:
            raise e    