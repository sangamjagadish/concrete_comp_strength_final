from datetime import datetime
from os import listdir
import pandas as pd
import xlrd
import xlwt
from Application_Logging.logger import App_Logger


class dataTransform:

    """
        This class shall be used for transformaing the Good Raw Training Data before loading it in database

    """



    def __init__ (self):
        self.goodDataPath = "Training_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()


    def renameColumns(self):


        """
        Description: This function renamed the columns having longer name with spaces in between them
                     to shorter name and then replaces the original file with file having renamed columns.
        Output: None
        On Failue: Raise Exception
        """

        log_file = open("Training_Logs/renameColumns.txt", 'a+')
        
        try:
            onlyfiles = [f for f in listdir(self.goodDataPath)]
            for file in onlyfiles:
                data = pd.read_excel(self.goodDataPath+"/" + file)
                data=data.rename(columns= lambda x: x.split(" ")[0])

                data.to_excel(self.goodDataPath+"/" + file, index=False, header=True)
                self.logger.log(log_file, "%s: Columns of file have been renamed to shorter name successfully!!" % file)
     
        except Exception as e:
            self.logger.log(log_file, "Renaming of columns failed because:: %s" %e)
            log_file.close()
            raise e

        log_file.close()    



"""
    def replaceMissingWithNull(self):

        '
        As there are no missing values we have found in any of the columns during EDA
        so we don't need this function to be used in this project.
        '
         
    """