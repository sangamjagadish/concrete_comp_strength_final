import pandas
from Data_Ingestion import data_loader_prediction
from Data_Preprocessing import preprocessing
from File_Operations import file_methods
from Application_Logging.logger import App_Logger
from Prediction_RawDataValidation.predictionDataValidation import Prediction_Data_Validation


class prediction:

    """
    This is entry point for prediction from the Machine Learning Model.    
    """

    def __init__(self,path):
        self.log_writer = App_Logger()
        self.file_object = open("Prediction_Logs/Prediction_Log.txt", 'a+')
        self.pred_data_val = Prediction_Data_Validation(path)



    def predictionFromModel(self):
        
        self.log_writer.log(self.file_object, 'Start of Prediction')
        try:
            #Delete existing prediction file
            self.pred_data_val.deletePredictionFile()
            #Getting Data from the source
            data_getter = data_loader_prediction.Data_Getter(self.file_object, self.log_writer)
            data = data_getter.get_data()

            "#PREPROCESSING STEPS:" 
            preprocessor = preprocessing.Preprocessor(self.file_object, self.log_writer)

            #Removing Duplicate rows
            data = preprocessor.removeDuplicates(data)

            #scale the prediction data 
            data_scaled = pandas.DataFrame(preprocessor.ScalingData(data, dist = 'notnormal'), columns=data.columns)
                

            #Loading the cluster model
            file_loader = file_methods.File_Operation(self.file_object, self.log_writer)
            kmeans = file_loader.load_model('KMeans')

            #Predicting the clusters
            clusters = kmeans.predict(data_scaled) 
            data_scaled['clusters'] = clusters
            clusters = data_scaled['clusters'].unique()
            result=[]

            for i in clusters:
                cluster_data = data_scaled[data_scaled['clusters']==i]
                cluster_data = cluster_data.drop(['clusters'],axis=1)
                model_name = file_loader.find_correct_model_file(i)
                model = file_loader.load_model(model_name)
                for val in (model.predict(cluster_data.values)):
                    result.append(val)
            result = pandas.DataFrame(result,columns=['Predictions'])
            path = "Prediction_Output_File/Predictions.csv" 
            result.to_csv("Prediction_Output_File/Predictions.csv", header = True)  
            self.log_writer.log(self.file_object, 'End of Prediction')
        except Exception as ex:
            self.log_writer.log(self.file_object, 'Error occured while running the prediction!! Error:: %s' % ex)
            raise ex
        return path, result.head().to_json(orient="records")