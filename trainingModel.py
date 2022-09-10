from sklearn.model_selection import train_test_split
from Data_Ingestion import data_loader
from Data_Preprocessing import preprocessing
from Data_Preprocessing import clustering
from File_Operations import file_methods
from BestModel_Finder import tuner
from Application_Logging.logger import App_Logger

class trainModel:

    """
    This is entry point for training the Machine Learning Model.    
    """

    def __init__(self):
        self.log_writer = App_Logger()
        self.file_object = open("Training_Logs/ModeltrainingLog.txt", 'a+')


    def trainingModel(self):

        self.log_writer.log(self.file_object, 'Start of Training')
        try:
            #Getting Data from the source
            data_getter = data_loader.Data_Getter(self.file_object, self.log_writer)
            data = data_getter.get_data()

            "#PREPROCESSING STEPS:" 
            preprocessor = preprocessing.Preprocessor(self.file_object, self.log_writer)

            #Removing Duplicate rows
            data = preprocessor.removeDuplicates(data)

            #Seperating Features and Label
            X,Y = preprocessor.separate_label_feature(data,label_column_name = 'Concrete')

            "#CLUSTERING STEPS:"
            kmeans=clustering.KMeansClustering(self.file_object,self.log_writer)
            number_of_clusters = kmeans.elbow_plot(X)
            X=kmeans.create_clusters(X,number_of_clusters)

            #create a column in the dataset consisting of coresponding cluster assignments
            X['Labels'] = Y

            #getting unique clusters from our dataset
            list_of_clusters = X['Cluster'].unique()

            "#Parsing all the clusters and looking for the best ML ALgorithm to fit on individual clusters."

            for i in list_of_clusters:
                cluster_data = X[X['Cluster']==i]  #Filter the data for one cluster

                #Preapare the feature and label columns
                cluster_features = cluster_data.drop(['Labels','Cluster'], axis = 1)
                cluster_label = cluster_data['Labels']

                #Splitting the data into train and test set for each cluster one by one
                x_train,x_test,y_train,y_test = train_test_split(cluster_features, cluster_label, test_size = 1/3, random_state=22)

                x_train_scaled = preprocessor.ScalingData(x_train, dist='notnormal')
                x_test_scaled = preprocessor.ScalingData(x_test, dist='notnormal')

                #getting the best model
                model_finder =  tuner.Model_Finder(self.file_object,self.log_writer)

                best_model_name, best_model = model_finder.get_best_model(x_train_scaled,y_train,x_test_scaled,y_test)

                #saving the best best model to the models directory
                file_op = file_methods.File_Operation(self.file_object,self.log_writer)
                save_model = file_op.save_model(best_model, best_model_name+ str(i))

        except Exception:        

            self.log_writer.log(self.file_object, "Successful End of Training")
            self.file_object.close()    
            raise Exception








