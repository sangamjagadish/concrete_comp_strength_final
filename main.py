

from wsgiref import simple_server
from flask import Flask, request, render_template, send_file
from flask import Response
from flask_cors import CORS, cross_origin
import flask_monitoringdashboard as dashboard
import os
import json
from training_validation_insertion import train_validation
from trainingModel import trainModel
from prediction_validation_insertion import pred_validation
from predictionfromModel import prediction

os.putenv('LANG', 'en_US.UTF-8')
os.putenv('LC_ALL', 'en_US.UTF-8')

app = Flask(__name__)
dashboard.bind(app)
CORS(app)


@app.route('/', methods=['GET'])
@cross_origin()
def home():
    return render_template('index.html')

@app.route('/predict', methods=['GET','POST'])
@cross_origin()
def predictRouteClient():
    try:
        #if request.json is not None:
        #   path = request.json['filepath']
        folder_path = "Prediction_Batch_Files"
        if folder_path is not None:
            path = folder_path
             
            
            pred_val = pred_validation(path) #Object initialization

            pred_val.prediction_validation()  #calling the prediction validation function

            pred = prediction(path) #object initialization

            #predicting for datast present in database
            path, json_predictions = pred.predictionFromModel()
            return Response("Prediction File created at !!!"  +str(path) + 'and few of the predictions are ' +str(json.loads(json_predictions)))

        elif request.form is not None:
            path = request.form['filepath'] 

            pred_val = pred_validation(path) #Object initialization
            pred_val.prediction_validation()  #calling the prediction validation function

            pred = prediction(path) #object initialization

            #prediciting for dataset present in databse
            path, json_predictions = pred.predictionFromModel()
            return Response('Prediction File created at !!!'  +str(path) + 'and few of the predictions are' + str(json.loads(json_predictions)))   

        else:
            print('Nothing Matched')
    except ValueError:
        return Response("Error Occured! %s" %ValueError) 
    except KeyError:
        return Response("Error Occured! %s" %KeyError)
    except Exception as e:
        return Response("Error Occured! %s" %e)




@app.route('/train', methods=['GET','POST'])
def trainRouteClient():

    try:
        #if request.json['folderPath'] is not None:
        #   path = request.json['folderPath']
        folder_path = "Training_Batch_Files"
        if folder_path is not None:
            path=folder_path

            train_val = train_validation(path) #Object initialization
            train_val.train_validation() #calling the training validation function

            trainModelObj = trainModel() #Object initialization
            trainModelObj.trainingModel() #training the model for the files present in the table

    except ValueError:
        return Response("Error Occured! %s" %ValueError)
    except KeyError:
        return Response("Error Occured! %s" %KeyError)
    except Exception as e:
        return Response("Error Occured! %s" %e)

    return Response("Training Successfull!!")




@app.route('/download', methods = ['GET','POST'])
def download():
    try:
        folder_path = "Prediction_Output_File"
        file = "Predictions.csv" 
        if folder_path is not None:
            return send_file(folder_path + '/'+ file, as_attachment=True)

        else:
            print("No prediction file found. Start training the model to generate one.")  

    except Exception as e:
        return Response('Error Occured! %s' %e)          




port = int(os.getenv("PORT", 1234))
if __name__ == '__main__':
    host = '0.0.0.0'
    httpd = simple_server.make_server(host, port, app)
    print("Serving on %s %d" % ( host,port))
    httpd.serve_forever()






   







