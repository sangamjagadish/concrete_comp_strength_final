from datetime import datetime
from Prediction_RawDataValidation.predictionDataValidation import Prediction_Data_Validation
from Prediction_DataTransformation.predictionDataTransformation import dataTransform
from Prediction_DataTypeValidation_Insertion.predictionDataTypeValidation import DBOperation
from Application_Logging.logger import App_Logger

class pred_validation:
    def __init__(self,path):
        self.raw_data = Prediction_Data_Validation(path)
        self.dataTransform = dataTransform()
        self.DBOperation = DBOperation()
        self.file_object= open("Prediction_Logs/Prediction_Main_Log.txt", 'a+')
        self.log_writer = App_Logger()


    def prediction_validation(self):
        try:
            self.log_writer.log(self.file_object, " Start of validation of files for prediction!!")
            # extracting values from prediction schema
            LengthOfFirstWordInFile, LengthOfSecondWordInFile, ColName, NumberOfColumns = self.raw_data.valuesFromSchema()
            # getting the regex defined to validate filename
            regex = self.raw_data.manualRegexCreation()
            # validating filename of prediction files
            self.raw_data.validationFileNameRaw(regex, LengthOfFirstWordInFile, LengthOfSecondWordInFile)
            # validating column length in the file
            self.raw_data.validateColumnLength(NumberOfColumns)
            self.log_writer.log(self.file_object, "Raw Data Validation Complete!!")
            self.raw_data.validateMissingValuesinWholeColumn()
            
            self.log_writer.log(self.file_object,
                                "Raw Data Transformation Started!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.dataTransform.renameColumns()
            self.log_writer.log(self.file_object, "Raw Data Transformation Completed!!")



            self.log_writer.log(self.file_object,
                                "Creating Prediction_Database and tables on the basis of given schema!!!")
            # create database with given name, if present open the connection! Create table with columns given in schema
            self.DBOperation.createTableDB('prediction')
            self.log_writer.log(self.file_object, "Table creation Completed!!")
            self.log_writer.log(self.file_object, "Insertion of Data into Table started!!!!")
            # insert csv files in the table
            self.DBOperation.insertIntoTableGoodData('prediction')
            self.log_writer.log(self.file_object, "Insertion in Table completed!!!")
            self.log_writer.log(self.file_object, "Deleting Good Data Folder!!!")
            # Delete the good data folder after loading files in table
            self.raw_data.deleteExistingGoodDatapredictionFolder()
            self.log_writer.log(self.file_object, "Good_Data folder deleted!!!")
            self.log_writer.log(self.file_object, "Moving bad files to Archive and deleting Bad_Data folder!!!")
            # Move the bad files to archive folder
            self.raw_data.moveBadFilesToArchiveBad()
            self.log_writer.log(self.file_object, "Bad files moved to archive!! Bad folder Deleted!!")
            self.log_writer.log(self.file_object, "Validation Operation completed!!")
            self.log_writer.log(self.file_object, "Extracting csv file from table")
            # export data in table to csvfile
            self.DBOperation.selectingDatafromtableintocsv('prediction')
            self.file_object.close()

        except Exception as e:
            raise e    