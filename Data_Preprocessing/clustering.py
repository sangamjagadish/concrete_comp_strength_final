import matplotlib.pyplot as plt
from sklearn.cluster import KMeans
from kneed import KneeLocator
from File_Operations import file_methods

class KMeansClustering:



    def __init__(self, file_object, logger_object):
        self.file_object = file_object
        self.logger_object = logger_object

    def elbow_plot(self, data):

        """
            Description: This method saves the plot to decide the optimum number of clusters to the file.
            Output: A picture saved to the directory
            On Failure: Raise Exception
            
        """

        self.logger_object.log(self.file_object, "Entered the elbow plot method of the KMeansClustering class")
        wcss=[]
        try:
            for i in range (1,11):
                kmeans = KMeans(n_clusters=i, init='k-means++', random_state=12) #Initializing the KMeansa Object
                kmeans.fit(data) #Fitting will create clusters of data
                wcss.append(kmeans.inertia_)
            plt.plot(range(1,11),wcss) #Creating the Graph between WCSS and the number of clusters
            plt.title("The Elbow Method")
            plt.xlabel("Numbers of Clusters")
            plt.ylabel("WCSS")
            plt.savefig('preprocessing_data/K-Means_Elbow.PNG') #saving th eelbow plot locally

            #Finding the value of the optimum number of clusters programmatically
            self.kn = KneeLocator(range(1,11), wcss, curve='convex', direction='decreasing')

            self.logger_object.log(self.file_object, 'The optimum numbers of cluster is:  '+ str(self.kn.knee)+ '.')
            self.logger_object.log(self.file_object, 'Exited the elbow_plot method of the KMeansClustering class')
            return self.kn.knee

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in elbow_plot method of the KMeansClustering class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Finding the number of clusters failed. Exited the elbow_plot method of the KMeansClustering class')
            raise Exception()

    def create_clusters(self,data, number_of_clusters):

        """
            Description: This method shall be used to divide the data into clusters before training.
            Output: Clustered Model
            On Failure: Raise Exception.
        """
        

        self.logger_object.log(self.file_object, 'Entered the create_clusters method of the KMeansClustering class')
        self.data =data

        try:
            self.kmeans = KMeans(n_clusters = number_of_clusters, init='k-means++', random_state=12)
            self.y_means = self.kmeans.fit_predict(data) #Dividing data into given number of cluster

            #Saving KMeans model to the local directory
            self.file_op = file_methods.File_Operation(self.file_object, self.logger_object)
            self.save_model = self.file_op.save_model(self.kmeans, 'KMeans') 

            self.data['Cluster'] = self.y_means #Create a new column in dataset for storing the cluster information.
            self.logger_object.log(self.file_object, 'Succesfully created '+str(self.kn.knee)+ 'clusters. Exited the create_clusters method of the KMeansClustering class') 

            return self.data

        except Exception as e:
            self.logger_object.log(self.file_object,'Exception occured in create_clusters method of the KMeansClustering class. Exception message:  ' + str(e))
            self.logger_object.log(self.file_object,'Fitting the data to clusters failed. Exited the create_clusters method of the KMeansClustering class')
            raise Exception()









