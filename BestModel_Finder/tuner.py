from sklearn.ensemble import RandomForestRegressor
from sklearn.ensemble import BaggingRegressor
from sklearn.tree import DecisionTreeRegressor
from xgboost import XGBRegressor
from sklearn.svm import SVR 
from sklearn.metrics import  r2_score, mean_squared_error
import optuna


class Model_Finder:

    """
    This class shall be used to find the model with best accuracy and R2 score
    """

    def __init__(self,file_object,logger_object):
        self.file_object = file_object
        self.logger_object = logger_object
        """
        self.rfReg = RandomForestRegressor()
        self.dtReg = DecisionTreeRegressor()
        self.bagReg = BaggingRegressor()
        self.xgbReg = XGBRegressor()
        self.svReg = SVR()
        """

    def get_best_params_for_random_forest(self,train_x,train_y,test_x,test_y):

        """
        
        """

        self.logger_object.log(self.file_object, 'Entered the get_best_params_for_random_forest method of the Model_Finder class')

        def objective(trial):

            #Initializaing with different combination of parameters
            params={'criterion':trial.suggest_categorical('criterion',['mse','mae']),
                    'max_depth':trial.suggest_categorical('max_depth',[None,1,3,5,7,9]),
                    'min_samples_split':trial.suggest_int('min_samples_split',2,10),
                    'n_estimators':trial.suggest_categorical('n_estimators',[100,120,150]),
                    'max_features':trial.suggest_categorical('max_features',["auto", "sqrt", "log2"])
                    }

            #Creating an object of Optuna

            param=params
            rfreg = RandomForestRegressor(**params)
            rfreg.fit(train_x,train_y)
            pred_rf = rfreg.predict(test_x)
            rmse = mean_squared_error(test_y,pred_rf)
            return rmse   



        #calling the optuna study
        find_param = optuna.create_study(direction='minimize')
        find_param.optimize(objective, n_trials = 10, show_progress_bar=True)
            
        # extracting the best parameters
        criterion = find_param.best_trial.params['criterion']
        max_depth = find_param.best_trial.params['max_depth']
        min_samples_split = find_param.best_trial.params['min_samples_split']
        n_estimators = find_param.best_trial.params['n_estimators']
        max_features = find_param.best_trial.params['max_features']
        

        # creating a new model with the best parameters
        rfReg = RandomForestRegressor(criterion= criterion,max_depth =max_depth, n_estimators=n_estimators, max_features=max_features,min_samples_split=min_samples_split)
        # training the mew models
        rfReg.fit(train_x, train_y)
        return rfReg
                



    def get_best_params_for_bagging_decision_tree(self,train_x,train_y,test_x,test_y):
        
    

        def objective(trial):

            #Initializaing with different combination of parameters
            params={'criterion':trial.suggest_categorical('criterion',['mse','mae']),
                'max_depth':trial.suggest_categorical('max_depth',[1,2,3,4,5,6,7,None]),
                'min_samples_leaf':trial.suggest_int('min_samples_leaf',1,9),
                'max_features':trial.suggest_categorical('max_features',["auto", "sqrt", "log2"])
                    }

            #Creating an object of Optuna

            param=params
            bagdtreg = BaggingRegressor(DecisionTreeRegressor(**param),n_estimators=100)
            bagdtreg.fit(train_x,train_y)
            pred_dt = bagdtreg.predict(test_x)
            rmse = mean_squared_error(test_y,pred_dt)
            return rmse  



        #calling the optuna study
        find_param = optuna.create_study(direction='minimize')
        find_param.optimize(objective, n_trials = 10, show_progress_bar=True)
            
        # extracting the best parameters
        criterion = find_param.best_trial.params['criterion']
        max_depth = find_param.best_trial.params['max_depth']
        min_samples_leaf = find_param.best_trial.params['min_samples_leaf']
        max_features = find_param.best_trial.params['max_features']
        

        # creating a new model with the best parameters
        bagdtReg = BaggingRegressor(DecisionTreeRegressor(criterion = criterion ,max_depth = max_depth, max_features=max_features,min_samples_leaf=min_samples_leaf),n_estimators=100)
        # training the mew models
        bagdtReg.fit(train_x, train_y)
        return bagdtReg
                                



    def get_best_params_for_xgboost(self,train_x,train_y,test_x,test_y):
        
    

        def objective(trial):

            #Initializaing with different combination of parameters
            params={
            'reg_lambda':trial.suggest_loguniform('reg_lambda', 1e-4,10.0),
            'reg_alpha' : trial.suggest_loguniform('reg_alpha', 1e-4,10.0),
            'colsample_bytree':trial.suggest_categorical('colsample_bytree',[.1,.2,.3,.4,.5,.6,.7,.8,.9,1]),
            'subsample':trial.suggest_categorical('subsample',[.1,.2,.3,.4,.5,.6,.7,.8,.9,1]),
            'learning_rate': trial.suggest_loguniform('learning_rate',0.01,0.5),
            'n_estimator' : 30000,
            'max_depth' : trial.suggest_categorical('max_depth', [3,4,5,6,7,8,9,10,11,12]),
            'random_state' :trial.suggest_categorical('random_state', [0]),
            'min_child_weight': trial.suggest_int('min_child_weight',1,200),
            }

            #Creating an object of Optuna

            param=params
            xgb_reg_model = XGBRegressor(**param)
            xgb_reg_model.fit(train_x,train_y,eval_set = [(test_x,test_y)], verbose = True)
            pred_xgb = xgb_reg_model.predict(test_x)
            rmse = mean_squared_error(test_y,pred_xgb)
            return rmse  



        #calling the optuna study
        find_param = optuna.create_study(direction='minimize')
        find_param.optimize(objective, n_trials = 10, show_progress_bar=True)
            
        # extracting the best parameters
        reg_lambda = find_param.best_trial.params['reg_lambda']
        reg_alpha = find_param.best_trial.params['reg_alpha']
        colsample_bytree = find_param.best_trial.params['colsample_bytree']
        learning_rate = find_param.best_trial.params['learning_rate']
        max_depth = find_param.best_trial.params['max_depth']
        random_state = find_param.best_trial.params['random_state']
        min_child_weight = find_param.best_trial.params['min_child_weight']

        

        # creating a new model with the best parameters
        xgb_Reg = XGBRegressor(reg_lambda = reg_lambda, reg_alpha = reg_alpha, colsample_bytree = colsample_bytree,learning_rate = learning_rate, max_depth = max_depth, random_state = random_state, min_child_weight = min_child_weight)
        # training the mew models
        xgb_Reg.fit(train_x, train_y)
        return xgb_Reg





    def get_best_params_for_svr(self,train_x,train_y,test_x,test_y):
        
    

        def objective(trial):

            #Initializaing with different combination of parameters
            params={'C':trial.suggest_categorical('C',[0.1, 1,2,5, 10]),
                    'degree':trial.suggest_categorical('degree',[0, 1, 2, 3, 4, 5, 6]),
                    'kernel':trial.suggest_categorical('kernel',['linear', 'rbf', 'poly']),
                    }

            #Creating an object of Optuna

            param=params
            svm_reg = SVR(**param)
            svm_reg.fit(train_x,train_y)
            pred_svr = svm_reg.predict(test_x)
            rmse = mean_squared_error(test_y,pred_svr)
            return rmse  



        #calling the optuna study
        find_param = optuna.create_study(direction='minimize')
        find_param.optimize(objective, n_trials = 10, show_progress_bar=True)
            
        # extracting the best parameters
        C = find_param.best_trial.params['C']
        degree = find_param.best_trial.params['degree']
        kernel = find_param.best_trial.params['kernel']

        

        # creating a new model with the best parameters
        svm_Reg = SVR(kernel = kernel, C = C, degree = degree)
        # training the mew models
        svm_Reg.fit(train_x, train_y)
        return svm_Reg



    def get_best_model(self,train_x,train_y,test_x,test_y):


        """
        
        """

        self.logger_object.log(self.file_object, 'Entered the get_best_model method of the Model_Finder class' )    

        
        try:

            #create best model for Reandom Forest Regressor
            randomForestReg = self.get_best_params_for_random_forest(train_x,train_y,test_x,test_y)
            prediction_randomForestReg = randomForestReg.predict(test_x)
            prediction_randomForestReg_error = r2_score(test_y, prediction_randomForestReg)



            #create best model for Bagging Regressor : Decision Tree 
            baggingDecisionTreeReg = self.get_best_params_for_bagging_decision_tree(train_x,train_y,test_x,test_y)
            prediction_baggingDecisionTreeReg = baggingDecisionTreeReg.predict(test_x)
            prediction_baggingDecisionTreeReg_error = r2_score(test_y, prediction_baggingDecisionTreeReg)


            #create best model for XG Boost Regressor
            XGBoostReg = self.get_best_params_for_xgboost(train_x,train_y,test_x,test_y)
            prediction_XGBoostReg = XGBoostReg.predict(test_x)
            prediction_XGBoostReg_error = r2_score(test_y, prediction_XGBoostReg)


            #create best model for SVM Regressor
            SVRReg = self.get_best_params_for_svr(train_x,train_y,test_x,test_y)
            prediction_SVRReg = SVRReg.predict(test_x)
            prediction_SVRReg_error = r2_score(test_y, prediction_SVRReg)


            #comparing the models
            bestmodel = max(prediction_randomForestReg_error,prediction_baggingDecisionTreeReg_error,prediction_XGBoostReg_error,prediction_SVRReg_error)

            if bestmodel == prediction_randomForestReg_error:
                return 'RandomForestRegressor', randomForestReg

            elif bestmodel == prediction_baggingDecisionTreeReg_error:
                return 'BaggingDecisionTreeRegressor', baggingDecisionTreeReg

            elif bestmodel == prediction_XGBoostReg_error:
                return 'XGBoostRegressor', XGBoostReg

            else:
                return 'SVMRegressor', SVRReg

        except Exception as e:
            self.logger_object.log(self.file_object,
                                   'Exception occured in get_best_model method of the Model_Finder class. Exception message:  ' + str(
                                       e))
            self.logger_object.log(self.file_object,
                                   'Model Selection Failed. Exited the get_best_model method of the Model_Finder class')
            raise Exception()        














        






