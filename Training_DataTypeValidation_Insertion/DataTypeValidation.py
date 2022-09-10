from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandracsv import CassandraCsv
import pandas as pd
import shutil
import os
from os import listdir
import csv
from Application_Logging.logger import App_Logger


class DBOperation:
    """
    This class shall be used for handling all the Cassandra Operations.
    """

    def __init__(self):
        self.badFilePath = "Training_Raw_Files_Validated/Bad_Raw"
        self.goodFilePath = "Training_Raw_Files_Validated/Good_Raw"
        self.logger = App_Logger()
        self.keyspace = 'training'

    def dataBaseConnection(self):

        """
        Description: This function will make the connection to Cassandra Database.
        Output: Connection to DB
        On Failure: Raise ConnectionError
        """

        try:
            cloud_config = {'secure_connect_bundle': './secure-connect-concreteproject.zip'}
            auth_provider = PlainTextAuthProvider('tUCZRspebWHpHxxoFZTvFceF',
                                                  '-jKA_8DCZcRh6o-LGO,FIkP2DPRKUj6lkPL8DTmRmu5TMCaZn1hSorlCW.k9G_t8YzJjkmXH4-2g,GLqIXXU6wokDvJ5ZSjc1kLpTQTQd+wsgd.YA+ERpfU3YAZucF-b')
            cluster = Cluster(cloud=cloud_config, auth_provider=auth_provider)
            session = cluster.connect()

            file = open('Training_Logs/DataBaseConnectionLog.txt', 'a+')
            self.logger.log(file, "Database is connected")
            file.close()

        except ConnectionError:
            file = open('Training_Logs/DataBaseConnection.txt', 'a+')
            self.logger.log(file, "Error while connecting to Database:: %s" % ConnectionError)
            file.close()
            raise ConnectionError

        return session

    def createTableDB(self, keyspace):

        """
        Description: This method creates the table in the given DB which will be used to insert the Good data after raw data validation.
        Output: None
        On Failure: Raise Exception
        """

        session = self.dataBaseConnection()
        session.execute("USE %s;" % keyspace)
        goodFilePath = self.goodFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        log_file = open("Training_Logs/DbTableCreateLog.txt", 'a+')

        for file in onlyfiles:
            df = pd.read_excel(goodFilePath + '/' + file)
            cols = [i[1] for i in enumerate(df)]
            try:
                query = 'DROP TABLE IF EXISTS Good_Raw_Data;'
                session.execute(query)

                query = f"""CREATE TABLE IF NOT EXISTS Good_Raw_Data(id int  PRIMARY KEY, "{cols[0]}" float,"{cols[1]}" float,"{cols[2]}" float,"{cols[3]}" float,
                        "{cols[4]}" float,"{cols[5]}" float,"{cols[6]}" float,"{cols[7]}" float,"{cols[8]}" float);"""
                session.execute(query)

                log_file = open("Training_Logs/DBTableCreateLog.txt", 'a+')
                self.logger.log(log_file, "Table Good_Raw_Data created successfully")
                log_file.close()


            except Exception as e:
                log_file = open("Training_Logs/DBTableCreateLog.txt", 'a+')
                self.logger.log(log_file, "Error occured while creating Table Good_Raw_Data: %s" % e)
                log_file.close()
                log_file = open("Prediction_Logs/DatabaseConnectionLog.txt", 'a+')
                self.logger.log(log_file, "%s Database disconnected successfully!!" % keyspace)
                log_file.close()
                raise e
        session.shutdown()

    def insertIntoTableGoodData(self, keyspace):

        """
        Description: This method inserts the Good data files from the Good_Raw folder into the
                     table of Database.
        Output: None
        On Failure: Raise Exception
        """

        session = self.dataBaseConnection()
        goodFilePath = self.goodFilePath
        badFilePath = self.badFilePath
        onlyfiles = [f for f in listdir(goodFilePath)]
        session.execute("USE %s;" % keyspace)
        log_file = open("Training_Logs/DbInsertLog.txt", 'a+')

        for file in onlyfiles:

            try:
                df = pd.read_excel(goodFilePath + '/' + file)
                cols = [i[1] for i in enumerate(df)]
                list_ = df.values.tolist()
                values = [i for i in list_]
                for i in range(len(values)):
                    try:
                        query = f"""INSERT INTO Good_Raw_Data(id,"{cols[0]}","{cols[1]}","{cols[2]}","{cols[3]}","{cols[4]}","{cols[5]}","{cols[6]}","{cols[7]}","{cols[8]}") VALUES({i},{values[i][0]},{values[i][1]},{values[i][2]},{values[i][3]},{values[i][4]},{values[i][5]},{values[i][6]},{values[i][7]},{values[i][8]});"""
                        session.execute(query)
                        self.logger.log(log_file, "%s: Data inserted into table successfully!!" % file)
                    except Exception as e:
                        raise e

            except Exception as e:

                self.logger.log(log_file, "Error while inserting data into table: %s" % e)
                shutil.move(goodFilePath + '/' + file, badFilePath)
                self.logger.log(log_file, "File Move Successfully %s" % file)
                log_file.close()
        session.shutdown()
        log_file.close()

    def selectingDatafromtableintocsv(self, keyspace):

        """
        Description: This method exports the data from table of Database to csv file in a given location.
        Output: None
        On Failure: Raise Exception
        """

        self.fileFromDb = 'Training_FileFromDB/'
        self.fileName = 'InputFile.csv'
        log_file = open("Training_Logs/ExportToCsv.txt", 'a+')
        session = self.dataBaseConnection()
        session.execute("USE %s;" % keyspace)

        try:

            query = "SELECT * FROM Good_Raw_Data;"
            result = session.execute(query)

            # Make the CSV ouput directory
            if not os.path.isdir(self.fileFromDb):
                os.makedirs(self.fileFromDb)

            # Exporting to csv file from Database
            CassandraCsv.export(result, output_dir=self.fileFromDb, filename=self.fileName)

            # Sorting rows and columns and exporting to csv file
            sortfile = pd.read_csv(self.fileFromDb + self.fileName)
            sortfile = sortfile.sort_values(['Id']).drop('Id', axis=1)
            sortfile = sortfile[
                ['Cement', 'Blast', 'Fly', 'Water', 'Superplasticizer', 'Coarse', 'Fine', 'Age', 'Concrete']]
            sortfile.to_csv(self.fileFromDb + self.fileName, index=False)

            self.logger.log(log_file, "File exported successfully!!!")
            log_file.close()

        except Exception as e:
            self.logger.log(log_file, " File export failed. Error: %s" % e)
            log_file.close()

        session.shutdown()        


