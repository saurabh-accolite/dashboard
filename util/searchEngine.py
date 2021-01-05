import re
import pandas as pd
import numpy as np
from numpy import random
import gensim
import nltk
from sklearn.model_selection import train_test_split
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem.snowball import SnowballStemmer
from datetime import datetime
from sklearn.linear_model import SGDClassifier

class SearchEngine:
    def __init__(self,dataPath):
        self.df = pd.read_csv(dataPath)
        self.tfidf = None
        self.clf_final = None
    
    def clean_text(self,text):
        """
            text: a string
        """
        REPLACE_BY_SPACE_RE = re.compile('[/(){}\[\]\|@,;]')
        BAD_SYMBOLS_RE = re.compile('[^0-9a-z #+_]')
        STOPWORDS = set(stopwords.words('english'))
            
        text = text.lower() # lowercase text
        text = REPLACE_BY_SPACE_RE.sub(' ', text) # replace REPLACE_BY_SPACE_RE symbols by space in text
        text = BAD_SYMBOLS_RE.sub('', text) # delete symbols which are in BAD_SYMBOLS_RE from text
        text = ' '.join(word for word in text.split() if word not in STOPWORDS) # delete stopwors from text
        return text

    def preProcessData(self,df):
        df = df.dropna()
        df['tags'] = df['tags'].apply(lambda x: x.split(','))
        df = df.explode('tags')
        df = df.sample(frac=1)
        df['tags'] = df['tags'].apply(self.clean_text)
        labels = list(df.chartId.unique())
        self.labels = {value:index for index,value in enumerate(labels)}
        self.labels_map = {value:key for key,value in self.labels.items()}
        df.chartId = df['chartId'].apply(lambda x: self.labels[x])
        return df

    def trainModel(self):
        df_new = self.preProcessData(self.df)
        tfidf = TfidfVectorizer()
        data = tfidf.fit_transform(df_new.tags)
        clf_final = SGDClassifier(alpha = 1e-4, loss='log',class_weight="balanced", n_jobs=-1)
        clf_final.fit(data, df_new.chartId)
        self.clf_final = clf_final
        self.tfidf = tfidf

    def process_query(self,query):
        preprocessed_sentence = []
        preprocessed_sentence.append(self.clean_text(query).strip())
        return preprocessed_sentence

    def getResults(self,query):
        if self.clf_final is None:
            self.trainModel()
        query = self.process_query(query)
        query = self.tfidf.transform(query)
        ans = self.clf_final.predict(query)
        prob = self.clf_final.predict_proba(query)
        idx = np.argsort(prob.flatten())
        results = [self.labels_map[x] for x in idx][::-1]
        
        return results