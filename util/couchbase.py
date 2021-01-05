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

    # def getFirstOpenCount(self,bucket,start,end=None):
    #     if start is None:
    #         start = self.start
    #     if end is None:
    #         end = self.end

    #     cb = self.cluster.bucket(bucket)

    #     query = f'''
    #     select channel, countryCode, date, distinctCount from `{bucket}` where type_='FirstOpen' and date > {start} limit 50
    #     '''

    #     event_query_result = self.cluster.query(query, timeout= datetime.timedelta(seconds=500))
    #     eventTmp = []
    #     for row in event_query_result:
    #         eventTmp.append(row)
    #     df_event = json_normalize(eventTmp)
    #     print(df_event.head())
    #     return df_event

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
        print(df_event.columns)
        print(df_event['events'][0][2]['platform.UserRegistration.createCustomer.TermsAcceptedState'])
        for i, row in df_event.iterrows():
            try:
                reg = df_event['events'][i][2]['platform.UserRegistration.createCustomer.TermsAcceptedState']
                df_event.at[i,'Registration'] = reg
                fo = df_event['events'][i][0]['pulse.firstOpen']
                df_event.at[i,'FirstOpen'] = fo
            except:
                print('Error in the order of the events')
        print(df_event.head())
        df_event['countryCode'] = df_event.type_.str.strip().str[-2:]
        df_event['countryCode'] = df_event['countryCode'].apply(lambda x: 'PH' if x == 'nt' else x)
        #print(df_event.countryCode)
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
        print(df_event.columns)
        print(df_event['events'][0])
        for i, row in df_event.iterrows():
            hac = df_event['events'][i][2]['pulse.babylon.healthAssessment.chat.fullAssessment.start']
            df_event.at[i,'ha'] = hac
            scc = df_event['events'][i][3]['pulse.babylon.symptomChecker.chat.start']
            df_event.at[i,'sc'] = scc

        print(df_event.head())
        df_event['countryCode'] = df_event.type_.str.strip().str[-2:]
        df_event['countryCode'] = df_event['countryCode'].apply(lambda x: 'PH' if x == 'nt' else x)
        #print(df_event.countryCode)
        return df_event





    def getTagWiseHourlyData_cb(self,bucket,start,end=None):
        if start is None:
            start = self.start
        if end is None:
            end = self.end

        cb = self.cluster.bucket(bucket)

        query = f'''
        SELECT split(`auditDetail`.`createTime`, "T")[0] as date, SPLIT(split(`auditDetail`.`createTime`, "T")[1], ":")[0] as hour, tags[2] as channel, count( distinct owner) as regnCount
        FROM `{bucket}`
        WHERE type_ = "Event"
        and  `auditDetail`.`createTime` > \'{start}\'
        AND name = "platform.UserRegistration.createCustomer.TermsAcceptedState"
        GROUP BY tags[2],split(`auditDetail`.`createTime`, "T")[0], SPLIT(split(`auditDetail`.`createTime`, "T")[1], ":")[0]
        ORDER BY split(`auditDetail`.`createTime`, "T")[0], SPLIT(split(`auditDetail`.`createTime`, "T")[1], ":")[0],tags[2];
        '''

        event_query_result = self.cluster.query(query, timeout= datetime.timedelta(seconds=500))
        eventTmp = []
        for row in event_query_result:
            eventTmp.append(row)
        df_event = json_normalize(eventTmp)
        return df_event
    

    @timeit('Getting PulseGold Users Data . . . ')
    def getPulseGoldUserData(self,bucket):
        cb = self.cluster.bucket(bucket)

        query = f'''
        select cs.customer.id as customerId,
        cs.subscriptionNo,
        cs.startDate,
        cs.endDate,
        po.product.code as ProductCode,
        po.product.fullName as ProductName,
        cs.cancelDate, cs.status,
        cust.contactDetails.email.`value` as custEmail
        FROM `{bucket}` cs
        UNNEST productOptions po
        JOIN {bucket} cust ON KEYS cs.customer.id
        WHERE cs.type_ = "CustomerSubscription"
        AND cust.type_ = "customer"
        AND cs.status not in [ "ABANDONED", "INITIATED", "INACTIVE"]
        AND po.product.code = "S00131"
        AND cs.auditDetail.createTime > "2020-11-30T16"
        and (cust.contactDetails.email.`value`not like "%yopmail%" and lower(cust.contactDetails.email.`value`) not like "%test%" )
        ORDER BY cs.startDate, po.product.code, cs.customer.id;
        '''

        event_query_result = self.cluster.query(query, timeout= datetime.timedelta(seconds=500))
        eventTmp = []
        for row in event_query_result:
            eventTmp.append(row)
        df_event = json_normalize(eventTmp)
        return df_event

    @timeit('Getting PulseGold Events Data . . . ')
    def getPulseGoldEventsData(self,bucket,ownerList):
        cb = self.cluster.bucket(bucket)

        query = f'''
        SELECT 
            owner,
            CASE
                WHEN name = "PulseUsage" THEN "SCREEN-" ||attributes.screen

                WHEN name = "PulseUsage" THEN "SCREEN-" ||attributes.screen

                WHEN ( NOT(name = "PulseUsage") AND name LIKE "pulse.communities.myCommunities%Post"

                                AND attributes.communityName IS NOT MISSING) THEN name || "/" || "communityName:" || attributes.communityName

                                                            || "/" || "postId:" ||attributes.postId || "/" || "postTitle:" ||attributes.postTitle

                WHEN ( NOT(name = "PulseUsage") AND name LIKE "pulse.communities.myCommunities%Post"

                    AND attributes.communityName IS MISSING) THEN name || "/" || "communityName:WorkoutVideos" || "/" || "postId:" ||attributes.postId                                                    || ":" || attributes.postId                                          

                ELSE name                         

            END as name,
            auditDetail.createTime as createTime

        FROM `{bucket}`

        WHERE type_ = "Event"

        AND auditDetail.createTime > "2020-12-01"

        and owner in {ownerList}

        UNION ALL

        SELECT

            ca.customer.clientId as owner,

            CASE

                WHEN ca.completedHabitCount > 0 THEN "pulse.wellnessGoals.habitCompleted" || "/" || "actionPlanId:" || ca.actionPlan.id || "/" || "habitId:" || hb.habit.id || "/" || "activeHabitCount:" || ca.activeHabitCount

                WHEN hb.completedMilestoneCount > 0 THEN "pulse.wellnessGoals.milestonesCompleted" || "/" || "actionPlanId:" ||ca.actionPlan.id || "/" || "habitId:" || hb.habit.id

                ELSE "pulse.wellnessGoals.wellnessGoalJoined" || "/" || "actionPlanId:" ||ca.actionPlan.id

            END AS name,

            ca.joinedDate as createTime

        FROM {bucket} ca

            UNNEST habits hb

        WHERE ca.type_ = "CustomerActionPlan"

        and ca.actionPlan.id in ["ActionPlan::1", "ActionPlan::2", "ActionPlan::4"]

        AND ca.joinedDate > "2020-12-01"

        AND ca.customer.clientId in {ownerList};'''


        event_query_result = self.cluster.query(query, timeout= datetime.timedelta(seconds=500))
        eventTmp = []
        for row in event_query_result:
            eventTmp.append(row)
        df_event = json_normalize(eventTmp)
        return df_event

    def getOwner_advIdMapping(self, bucket, ownerList):
        cb = self.cluster.bucket(bucket)

        query = f'''
        select owner , userAgent.advId as advId 
        from `{bucket}` 
        where type_ = 'Event' 
        and owner in {ownerList};
        '''

        event_query_result = self.cluster.query(query, timeout=datetime.timedelta(seconds=500))
        eventTmp = []
        for row in event_query_result:
            eventTmp.append(row)
        df_event = json_normalize(eventTmp).drop_duplicates()
        return df_event

    @timeit('Getting PulseGold Customer Data . . . ')
    def getCustomerData(self, bucket, ownerList):
        cb = self.cluster.bucket(bucket)

        query = f'''
        select id , auditDetail.createTime as createTime, sex, dob
        from `{bucket}` 
        where type_ = 'customer' 
        and id in {ownerList};
        '''

        event_query_result = self.cluster.query(query, timeout=datetime.timedelta(seconds=500))
        eventTmp = []
        for row in event_query_result:
            eventTmp.append(row)
        df_event = json_normalize(eventTmp)
        return df_event



