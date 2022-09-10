import pandas as pd
import numpy as np
from sklearn.preprocessing import StandardScaler, RobustScaler



class Preprocessor:

    "This class shall be used for cleaning and transforming the data before training."

    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object


    def removeDuplicates(self,data):

        """
        Description: This method removes the duplicate rows from a pandas dataframe.
        Output: A pandas DataFrame after removing the duplicate rows.
        On Failure: Raise Exception
        """

        self.logger_object.log(self.file_object, "Entered the removeDuplicates method of the Preprocessor class")
        self.data = data


        try:

            #We are doing all these steps because  drop_duplicates is not working as it should be. It is removing all the duplicate 
            #values including the unique ones.

            #Filter out all duplicate rows
            allduplicates = self.data.loc[data.duplicated()]
            #Let's filter out unique rows of duplicates rows
            unqval=allduplicates.drop_duplicates()
            #Let's get only the duplicate rows by filtering out the unique values
            duplicates =allduplicates.drop(unqval.index)
            #Now let's remove the duplicate rows from main dataframe
            self.useful_data = self.data.drop(duplicates.index)

            self.logger_object.log(self.file_object, "Duplicate Rows removal successful. Exited the removeDuplicates method of Preprocessor class")
            return self.useful_data
        except Exception as e:
            self.logger_object.log(self.file_object, "Exception occured in removeDuplicates method of Preprocessor class. Exception Message: %s" % e)
            self.logger_object.log(self.file_object, "Duplicate Rows removal unsuccessful. Exited the removeDuplicates method of Preprocessor class") 
            raise Exception()   




    def separate_label_feature(self, data, label_column_name):
        
        """
        Description: This method separates the features and a Label Coulmns.
        Output: Returns two separate Dataframes, one containing features and the other containing Labels .
        On Failure: Raise Exception

        """

        self.logger_object.log(self.file_object, 'Entered the separate_label_feature method of the Preprocessor class')

        try:
            self.X=data.drop(labels=label_column_name,axis=1) # drop the columns specified and separate the feature columns
            self.Y=data[label_column_name] # Filter the Label columns
            self.logger_object.log(self.file_object,
                                   'Label Separation Successful. Exited the separate_label_feature method of the Preprocessor class')
            return self.X,self.Y
        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in separate_label_feature method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'Label Separation Unsuccessful. Exited the separate_label_feature method of the Preprocessor class')
            raise Exception()



    def ScalingData(self,X,dist='normal'):

        """
        Description: This method brings the data of different scales on the same scale.
        Output: Scaled Data
        On Failure: Raise Exception
        """


        try:
            if dist == 'linear':

                #Let's use Standard Scaler as our we can see in pandas profiling plots that our data is almost normally distributed.

                self.logger_object.log(self.file_object, 'Entered the standardScalingData method of the Preprocessor class')
                scalar = StandardScaler()
                X_scaled = scalar.fit_transform(X)
                self.logger_object.log(self.file_object,'Data Scaling Successful. Exited the standardScalingData method of the Preprocessor class')

                return X_scaled 

            elif dist == 'notnormal':

                #Let's use Standard Scaler as our we can see in pandas profiling plots that our data is almost normally distributed.

                self.logger_object.log(self.file_object, 'Entered the standardScalingData method of the Preprocessor class')
                scalar = RobustScaler()
                X_scaled = scalar.fit_transform(X)
                self.logger_object.log(self.file_object,'Data Scaling Successful. Exited the standardScalingData method of the Preprocessor class')

                return X_scaled 


        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in standardScalingData method of the Preprocessor class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object, 'Data Scaling Unsuccessful. Exited the standardScalingData method of the Preprocessor class')
            raise Exception()



