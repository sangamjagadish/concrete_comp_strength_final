from datetime import datetime
import pandas as pd
import os
from os import listdir
import re
import json
import shutil
import xlrd
import xlwt
from Application_Logging.logger import App_Logger



class Raw_Data_Validation:

    """
    This class shall be used for handling all the validation on Raw Training Data.
    """

    def __init__(self,path):
        self.Batch_Directory = path
        self.schema_path = "schema_training.json"
        self.logger = App_Logger()

    def valuesFromSchema(self):
        """
        Description: This method extracts all the relevant information from pre-defined "Schema" file.
        Output: LengthOfFirstWordInFile, LengthOfSecondWordInFile, column_names, NumberOfColumns
        On Failure: Raise ValueError, KeyError, Exception 
        """

        try:
            with open(self.schema_path,'r') as f:
                dic = json.load(f)
                f.close()
            pattern = dic['SampleFileName']
            LengthOfFirstWordInFile = dic['LengthOfFirstWordInFile']
            LengthOfSecondWordInFile = dic['LengthOfSecondWordInFile']
            ColName = dic['ColName']
            NumberOfColumns = dic['NumberOfColumns'] 


            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            message = "LengthOfFirstWordInFile:: %s" %LengthOfFirstWordInFile + "\t" + "LengthOfSecondWordInFile:: %s" %LengthOfSecondWordInFile + "\t" + "NumberofColumns:: %s" %NumberOfColumns + "\n"
            self.logger.log(file, message)

            file.close()

        except ValueError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, f"ValueError:Value not found inside {self.schema_path}") 
            file.close()
            raise ValueError

        except KeyError:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, "KeyError:Key value error incorrect key passed") 
            file.close()
            raise KeyError 

        except Exception as e:
            file = open("Training_Logs/valuesfromSchemaValidationLog.txt", 'a+')
            self.logger.log(file, str(e))
            file.close()
            raise e

        return LengthOfFirstWordInFile, LengthOfSecondWordInFile, ColName, NumberOfColumns



    def manualRegexCreation(self):

        """
        Description: This mehtod contains a manually defined regex based on file name given in schema.
                    This regex will be used to validate the file name of the training data.
        Output: Regex Pattern
        On Failure: None 

        """

        regex = "['Concrete']+['\_'']+['Data']+\.xls"
        return regex



    def createDirectoryforGoodBadRawData(self):
        
        """
        Description: This creates directory to store the Good Data and Bad Data after validating the training data.
        Output: None
        OnFailure: OSError

        """

        try:
            path = os.path.join("Training_Raw_Files_Validated/", "Good_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)
            path = os.path.join("Training_Raw_Files_Validated/", "Bad_Raw/")
            if not os.path.isdir(path):
                os.makedirs(path)

        except OSError as ex:
            file = open("Training_Logs/GeneralLog.txt",'a+')
            self.logger.log(file, "Error while creating Directory %s:" %ex)
            file.close()
            raise OSError



    def deleteExistingGoodDataTrainingFolder(self):

        """
        Description: This method deletes the directory made to store the Good Data
                    after loading the data in the table. Once the good files are loaded
                    in the DB, deleting the directory ensures space optimization.
        Output: None
        On Failure: OSError
        """

        try:
            path = "Training_Raw_Files_Validated/"
            if os.path.isdir(path + 'Good_Raw/'):
                shutil.rmtree(path + 'Good_Raw/')
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "GoodRaw Directory deleted successfully!!!")
                file.close()
        
        except OSError as s:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file,"Error while deleting directory : %s" %s)
            file.close()
            raise OSError



    def deleteExistingBadDataTrainingFolder(self):

        """
        Description: This method deletes the directory made to store the Bad Data
                    after loading the data in the table. Once the good files are loaded
                    in the DB, deleting the directory ensures space optimization.
        Output: None
        On Failure: OSError
        """


        try:
            path="Training_Raw_Files_Validated/"
            if os.path.isdir(path + 'Bad_Raw/'):
                shutil.rmtree(path + 'Bad_Raw/')
                file = open("Training_Logs/GeneralLog.txt,'a+")
                self.logger.log(file, "BadRaw Directory Deleted Successfully!!!")
                file.close()

        except OSError as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while Deleting BadRaw Directory : %s" %e)
            file.close()
            raise OSError        


    def moveBadFilesToArchiveBad(self):

        """
        Description: This function deletes the directory made to store the Bad Data
                    after moving the data in an archive folder. We archive the bad
                    files to send them back to the client for invalid data issue.
        Output: None
        On Failure: Exception

        """


        now =datetime.now()
        date = now.date()
        time = now.strftime("%H%M%S")
        try:
            source = 'Training_Raw_Files_Validated/Bad_Raw/'
            if os.path.isdir(source):
                path = "TrainingArchiveBadData"
                if not os.path.isdir(path):
                    os.makedirs(path)
                dest = "TrainingArchiveBadData/BadData_" + str(date) + "_" + str(time)
                if not os.path.isdir(dest):
                    os.makedirs(dest)
                files = os.listdir(source)
                for f in files:
                    if f not in os.listdir(dest):
                        shutil.move(source + f, dest)
                file = open("Training_Logs/GeneralLog.txt", 'a+')
                self.logger.log(file, "Bad Files moved to archive")
                path = 'Training_Raw_Files_Validated/'
                if os.path.isdir(path + 'Bad_Raw/'):
                    shutil.rmtree(path + 'Bad_Raw/')
                self.logger.log(file, "Bad Raw Data folder deleted successfully!!!")
                file.close()

        except Exception as e:
            file = open("Training_Logs/GeneralLog.txt", 'a+')
            self.logger.log(file, "Error while moving bad files to archive:: %s" % e)
            file.close()
            raise e



    def validationFileNameRaw(self, regex, LengthOfFirstWordInFile, LengthOfSecondWordInFile):

        """
        Description: This function validates the name of the training xls files as per the given name in schema!
                    Regex pattern is used to do the validation.If name format doesn't match, the file is moved 
                    to the Bad Raw data folder else in Good Raw data folder.
        Output: None
        On Failure: Exception

        """

        #Delete the directories for good and bad data incase the last run was unsuccessful and folders were not deleted
        self.deleteExistingBadDataTrainingFolder()
        self.deleteExistingGoodDataTrainingFolder()

        #Create new directories for good and bad data
        self.createDirectoryforGoodBadRawData()
        onlyfiles = [f for f in listdir(self.Batch_Directory)]
        try:
            f = open("Training_Logs/nameValdationLog.txt", 'a+')
            for filename in onlyfiles:
                if  (re.match(regex, filename)):
                    splitAtDot = re.split('.xls', filename)
                    splitAtDot = (re.split('_', splitAtDot[0]))
                    if len(splitAtDot[0]) == LengthOfFirstWordInFile:
                        if len(splitAtDot[1]) == LengthOfSecondWordInFile:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_Files_Validated/Good_Raw")
                            self.logger.log(f, "Valid File Name!! File is moved to GoodRaw folder :: %s" %filename)
                        else:
                            shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_Files_Validated/Bad_Raw")
                            self.logger.log(f, "Invalid File Name (second_word) !! File is moved to BadRaw folder :: %s" %filename)

                    else:
                        shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f, "Invalid File Name (first_word)!! File is moved to BadRaw folder :: %s" %filename)
                else:
                    shutil.copy("Training_Batch_Files/" + filename, "Training_Raw_Files_Validated/Bad_Raw")
                    self.logger.log(f, "Invalid File Name!! File is moved to BadRaw folder :: %s" %filename)  
            f.close()    

        except Exception as e:
            f = open("Training_Logs/nameValidationLog.txt", 'a+')
            self.logger.log(f, "Error occures while validating file name %s" %e)
            f.close()
            raise e                          



    def validateColumnLength(self,NumberOfColumns):

        """
        Description: This function validates the number of columns in files.
                    It should be same as given in schema file.
                    If not same, file is not suitable for processing and thus moved to Bad Raw Folder.
                    If the column number matches, file remains in Good Raw Folder for processing.
        Output: None
        On Failure: Exception
        """


        try:
            f = open("Training_Logs/columnValidationLog.txt", 'a+')
            self.logger.log(f, "Column Length Validation Started!!")
            for file in listdir('Training_Raw_Files_Validated/Good_Raw'):
                xls = pd.read_excel("Training_Raw_Files_Validated/Good_Raw/" + file)
                if xls.shape[1] == NumberOfColumns: 
                    pass
                else:
                    shutil.move("Training_Raw_files_validated/Good_Raw/" + file, "Training_Raw_files_validated/Bad_Raw")
                    self.logger.log(f, "Invalid Column length of file!! File moved to Bad Raw folder :: %s" %file)
            self.logger.log(f, "Column Length Validation Completed!!")
        except OSError:
            f = open("Training_Logs/columnValidationLog.txt",'a+')
            self.logger.log(f, "Error occured while moving the file:: %s" %OSError) 
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/columnValidationLog.txt",'a+')
            self.logger.log(f, "Error occured:: %s" %e) 
            f.close()
            raise e
        f.close()    



    def validateMissingValuesinWholeColumn(self):


        try:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f,"Missing Values Validation Started!!")

            for file in listdir("Training_Raw_Files_Validated/Good_Raw/"):
                xls = pd.read_excel("Training_Raw_Files_Validated/Good_Raw/" + file)
                for columns in xls:
                    if(len(xls[columns]) - xls[columns].count()) == len(xls[columns]):
                        shutil.move("Training_Raw_Files_Validated/Good_Raw" + file,
                                    "Training_Raw_Files_Validated/Bad_Raw")
                        self.logger.log(f, "Invalid Column Length for the file!! File moved to Bad Raw folder :: %s" %file) 
        except OSError:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured while moving the file :: %s" % OSError)
            f.close()
            raise OSError
        except Exception as e:
            f = open("Training_Logs/missingValuesInColumn.txt", 'a+')
            self.logger.log(f, "Error Occured:: %s" % e)
            f.close()
            raise e
            f.close()                           


