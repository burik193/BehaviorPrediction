import pandas as pd
import dask
from dask.distributed import Client, progress
import dask.dataframe as dd
import numpy as np
from sklearn.metrics import accuracy_score, confusion_matrix, ConfusionMatrixDisplay, roc_curve, auc, RocCurveDisplay, f1_score, recall_score, precision_score
from sklearn import preprocessing
import logging
from functools import cache
import gensim
from gensim.parsing.preprocessing import remove_stopwords, preprocess_string
from geopy import distance
from joblib import dump, load


def derive_topics(text: str) -> set:
    words = gensim.utils.simple_preprocess(text)
    words_prep = [preprocess_string(remove_stopwords(word)) for word in words]
    return set([w[0] for w in words_prep if w])


@cache
def make_distance(c1: tuple, c2: tuple) -> float:
    d = round(float(distance.distance(c1, c2).km), 2)
    return d


@cache
def calc_score(v1: set, v2: set) -> int:
    return sum([1 for v in v2 if v in str(v1)])


def fall_out_score(y_test, y_predict):
    confusion = confusion_matrix(y_test, y_predict)
    tn = confusion[0][0]
    fp = confusion[0][1]
    return fp/(fp+tn)


class BehaviorModelling:
    """Idea: take HCP and Webinar participation set. Bring them together using clustering"""
    def __init__(self, hcp_data: pd.DataFrame, ):
        pass



class BehaviorPrediction:
    """Idea: take HCP set, that's __ready for modelling__, 
    append the webinar information and derive the prediction."""

    def __init__(self, hcp_data: dd.DataFrame or pd.DataFrame, webinar_data: dict, merge_key: str, model_path: str, by_franchise=True, feature_names: list = None):

        if not isinstance(hcp_data, pd.DataFrame) and not isinstance(hcp_data, dd.DataFrame):
            raise Exception(f"Parameter 'hcp_data' should be of type {pd.DataFrame} or {dd.DataFrame}, not {type(hcp_data)}")
        
        if not isinstance(webinar_data, dict):
            raise Exception(f"Parameter 'webinar_data' should be of type {dict}")

        if not isinstance(model_path, str):
            raise Exception(f"Parameter 'model_path' should be of type {str}")
        
        if not model_path.endswith('joblib'):
            raise Exception(f"Parameter 'model_path' should have extention 'joblib', got: {model_path.split('.')[-1]}")
        
        client = Client(n_workers=1)

        self.hcp_data = hcp_data
        self.webinar_data = webinar_data
        self.model = load(model_path)
        if feature_names is None:
            try:
                self.feature_names = self.model.feature_names
            except AttributeError as AE:
                raise AE("The given model has no attribute 'feature_names'. "
                        "Please specify the 'feature_names' by giving it as one of the init parameters, "
                        "or put them inside of your model as 'feature_names'.")
        else:
            self.feature_names = feature_names

        self.merge_key = merge_key
        self.by_franchise = by_franchise

    def _resolve_feature(self, feature, df) -> pd.Series:
        if feature == 'distance':
            result = df.apply(lambda row: make_distance((row['account_latitude'], row['account_longitude']), (row['webinar_latitude'], row['webinar_longitude'])), axis=1, meta=('distance', 'float64'))
        elif feature == 'segment_intersection':
            result = df[self.webinar_data['_segment_key'][0]]
        elif feature == 'main_interests_score':
            df['main_interests'] = df['main_interests'].apply(lambda x: frozenset(map(lambda x: x.strip().strip("\'"), x.strip('{}[]').split(','))) if isinstance(x, str) else frozenset(x), meta=('main_interests', 'object'))
            df['topics'] = df['topics'].apply(lambda x: frozenset(map(str.strip, x.strip('{}[]').split(','))) if isinstance(x, str) else frozenset(x), meta=('topics', 'object'))
            try:
                result = df.apply(lambda row: calc_score(row['main_interests'], row['topics']), axis=1, meta=('main_interests_score', 'int64'))
            except KeyError as k:
                raise Exception("Merged DataFrame is missing requered features to calculate 'main_interests_score'.")
        elif feature == 'similar_interests_score':
            df['similar_interests'] = df['similar_interests'].apply(lambda x: frozenset(map(lambda x: x.strip().strip("\'"), x.strip('{}[]').split(','))) if isinstance(x, str) else x, meta=('similar_interests', 'object'))
            try:
                result = df.apply(lambda row: calc_score(row['similar_interests'], row['topics']), axis=1, meta=('similar_interests_score', 'int64'))
            except KeyError as k:
                raise Exception("Merged DataFrame is missing requered features to calculate 'similar_interests_score'.")
        else:
            raise Exception(f'Feature {f} cannot be resolved...')
        return result
    
    def _compile_dataset(self) -> pd.DataFrame:
        # create webinar_df

        self.webinar_data = {k: [v] for k, v in self.webinar_data.items() if not isinstance(v, list)}

        # check on dimension missmatch
        check_dims = [k for k, v in self.webinar_data.items() if len(v) > 1]
        if check_dims != []:
            raise Exception(f"Following webinar attributes have length > 1: {check_dims}")
        del check_dims

        # make a DataFrame out of webinar_data
        webinar_df = pd.DataFrame.from_dict(self.webinar_data)
        webinar_df['topics'] = webinar_df['topics'].apply(lambda x: derive_topics(x + self.webinar_data['webinar_name_original'][0]))
        
        if self.by_franchise:
            self.hcp_data = self.hcp_data[self.hcp_data[self.webinar_data['_segment_key'][0]] == 1]

        self.hcp_data.drop_duplicates(subset=[self.merge_key], inplace=True)
        merge_on = self.hcp_data[self.merge_key].compute().to_list()

        webinar_df = pd.concat([webinar_df]*len(merge_on), ignore_index=True)
        webinar_df.insert(0, self.merge_key, merge_on)
        del merge_on

        # merge hcp_df and webinar_df
        delayed_operations = []
        print('Merging HCPs and Webinars together...')
        result = self.hcp_data.merge(webinar_df, on=self.merge_key, how='inner')
        # del webinar_df

        # Derive needed features
        features = ['distance', 'segment_intersection', 'main_interests_score', 'similar_interests_score']
        # 1. Distance
        
        for f in features:
            if f in self.feature_names:
                print(f'Calculating {f}...')
                delayed_operations.append(dask.delayed(result.assign(f=self._resolve_feature(f, result))))
        
        print('Compile the dataset for the model...')
        result = dask.compute(*delayed_operations)
        
        if not (set(self.feature_names).intersection(set(result.columns)) == set(self.feature_names)): 
            raise Exception("Some feature names could't be found in the merged DataFrame")
        result = result[self.feature_names]

        return result

    def get_prediction(self, scale=True) -> np.array:

        # compile DataFrame for prediction
        X = self._compile_dataset()
        X.fillna(0, inplace=True)
        
        # scale if needed
        if scale:
            scaler = preprocessing.MinMaxScaler()
            scaler.fit(X)
            X = scaler.transform(X)
        
        # get prediction my the merge key
        prediction = self.model.predict(X)
        prediction = self.hcp_data[self.hcp_data[[True if _ == 1 else False for _ in prediction]]][self.merge_key]

        return prediction

if __name__ == '__main__':
    hcp_df = dd.read_csv('hcp_segment_37webs.csv')
    webinar_oman = {'webinar_name_original': 'Management of Hypothyroidism in Primary Health Care Workshop - Hybrid event', 
                    'webinar_latitude': 21.3866249, 
                    'webinar_longitude': 51.6560391, 
                    'day_of_year': 341, 
                    'day_of_week': 4, 
                    'webinar_duration': 240, 
                    'topics': 'To discuss in general the primary Hypothyroidism.To discuss the classification & diagnosis of hypothyroidism and its management in primary health care. To discuss the Management of thyroid disease in pregnancy.', 
                    '_segment_key': 'CM&E'}
    model_path = 'XGB_RF_37Webs_distance_fallout.joblib'

    BP = BehaviorPrediction(hcp_data=hcp_df, webinar_data=webinar_oman, merge_key='_account_key', model_path=model_path)
    prediction = BP.get_prediction()
    print(f'Predicted: {prediction.shape[0]} out of {hcp_df.shape[0]} HCPs')
