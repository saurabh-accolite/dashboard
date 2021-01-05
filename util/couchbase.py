from couchbase.cluster import Cluster, ClusterOptions
from couchbase.cluster import PasswordAuthenticator
from couchbase.cluster import QueryOptions
import datetime
from datetime import timedelta, date
import pandas as pd
from pandas import json_normalize
import time

from util.utilDecorator import timeit

class CouchbasePython:

    def __init__(self,cb_url,cb_username,cb_password):
        self.cb_url = cb_url
        self.cb_username = cb_username
        self.cb_password = cb_password
    
        self.start = '2020-12-25'
        self.end = '3000-05-04T00:00Z'
        
        self.cluster  = Cluster(self.cb_url,ClusterOptions(PasswordAuthenticator(self.cb_username,self.cb_password)),lockmode=2)
        if self.cluster:
            print(">>INFO: Couchbase Connection Successful")
        else:
            print(">>INFO: Couchbase Connection Unsuccessful!!!!")

    def getFirstOpenCount(self,bucket,start,end=None):
        if start is None:
            start = self.start
        if end is None:
            end = self.end

        cb = self.cluster.bucket(bucket)

        query = f'''
        select channel, countryCode, date, distinctCount from `{bucket}` where type_='FirstOpen' and date > {start} limit 200
        '''

        event_query_result = self.cluster.query(query, timeout= datetime.timedelta(seconds=500))
        eventTmp = []
        for row in event_query_result:
            eventTmp.append(row)
        df_event = json_normalize(eventTmp)
        return df_event

    def getRegCount(self,bucket,start,end=None):
        if start is None:
            start = self.start
        if end is None:
            end = self.end

        cb = self.cluster.bucket(bucket)

        query = f'''
                select date, events, type_ from `{bucket}` where type_ in ['Registration_event', 'Registration_event_VN', 'Registration_event_TH', 'Registration_event_MY', 'Registration_event_ID'] and date > {start} limit 50
        '''

        event_query_result = self.cluster.query(query, timeout= datetime.timedelta(seconds=500))
        eventTmp = []
        for row in event_query_result:
            eventTmp.append(row)
        df_event = json_normalize(eventTmp)
        for i, row in df_event.iterrows():
            reg = df_event['events'][i][2]['platform.UserRegistration.createCustomer.TermsAcceptedState']
            df_event.at[i,'Registration'] = reg
    
        df_event['countryCode'] = df_event.type_.str.strip().str[-2:]
        df_event['countryCode'] = df_event['countryCode'].apply(lambda x: 'PH' if x == 'nt' else x)
        return df_event

    def getBabylonCount(self,bucket,start,end=None):
        if start is None:
            start = self.start
        if end is None:
            end = self.end

        cb = self.cluster.bucket(bucket)

        query = f'''
                select date, events, type_ from `{bucket}` where type_ in ['babylon_event', 'babylon_event_VN', 'babylon_event_TH', 'babylon_event_MY', 'babylon_event_ID'] and date > {start} limit 50
        '''

        event_query_result = self.cluster.query(query, timeout= datetime.timedelta(seconds=500))
        eventTmp = []
        for row in event_query_result:
            eventTmp.append(row)
        df_event = json_normalize(eventTmp)
        
        for i, row in df_event.iterrows():
            hac = df_event['events'][i][2]['pulse.babylon.healthAssessment.chat.fullAssessment.start']
            df_event.at[i,'ha'] = hac
            scc = df_event['events'][i][3]['pulse.babylon.symptomChecker.chat.start']
            df_event.at[i,'sc'] = scc

        df_event['countryCode'] = df_event.type_.str.strip().str[-2:]
        df_event['countryCode'] = df_event['countryCode'].apply(lambda x: 'PH' if x == 'nt' else x)
        return df_event 

    # def getTagWiseHourlyData_cb(self,bucket,start,end=None):
    #     if start is None:
    #         start = self.start
    #     if end is None:
    #         end = self.end

    #     cb = self.cluster.bucket(bucket)

    #     query = f'''
    #     SELECT split(`auditDetail`.`createTime`, "T")[0] as date, SPLIT(split(`auditDetail`.`createTime`, "T")[1], ":")[0] as hour, tags[2] as channel, count( distinct owner) as regnCount
    #     FROM `{bucket}`
    #     WHERE type_ = "Event"
    #     and  `auditDetail`.`createTime` > \'{start}\'
    #     AND name = "platform.UserRegistration.createCustomer.TermsAcceptedState"
    #     GROUP BY tags[2],split(`auditDetail`.`createTime`, "T")[0], SPLIT(split(`auditDetail`.`createTime`, "T")[1], ":")[0]
    #     ORDER BY split(`auditDetail`.`createTime`, "T")[0], SPLIT(split(`auditDetail`.`createTime`, "T")[1], ":")[0],tags[2];
    #     '''

    #     event_query_result = self.cluster.query(query, timeout= datetime.timedelta(seconds=500))
    #     eventTmp = []
    #     for row in event_query_result:
    #         eventTmp.append(row)
    #     df_event = json_normalize(eventTmp)
    #     return df_event
