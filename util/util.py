# Standard library imports
import re
import json
import glob
import os
import sys
from datetime import timezone,timedelta
from datetime import date
import datetime as dt
import pytz
import time
from pathlib import Path
# Third party imports
import dash_table
import pandas as pd
import numpy as np
from pandas import json_normalize
import plotly
import plotly.io as pio
pio.templates.default = "seaborn"

from plotly.io import write_image
from scipy import stats
import feather
# from plotly.offline import *
from plotly import graph_objs as go
# import plotly.express as px
import plotly.figure_factory as ff
import dash_bootstrap_components as dbc
import dash_html_components as html
# import dash
import dash_core_components as dcc
import urllib.parse
from util.druid import DruidPython
from util.couchbase import CouchbasePython
import util.appLogger as appLogger
from util.exceptionHandler import Exception

utilLogger = appLogger.get_logger(__name__)
print = utilLogger.debug

# Singleton class
class Util:
    __instance = None

    @staticmethod
    def getInstance(app=None,config=None):
        """ Static access method. """
        if Util.__instance == None:
            Util(app,config)
        return Util.__instance

    def __init__(self,app,config):
        """ Virtually private constructor. """
        if Util.__instance != None:
            raise Exception("This class is a singleton!")
        else:
            utilLogger.debug('\n ------------------------------------------ Starting App -------------------------------------------\n')
            self.app = app
            self.config = config
            # self.countryCode = self.config.countryCode
            self.cbPython = CouchbasePython(self.config.cb_url,self.config.cb_username,self.config.cb_password)
            df = self.cbPython.getFirstOpenCount(self.config.cb_buckets, None)
            if self.config.dataSource == 'Druid':
                # self.druidPython = DruidPython(self.config.druid_url,self.config.druid_host,self.config.druid_port,self.config.druid_username,self.config.druid_password,self.config.druid_events,self.config.druid_customer,self.config.druid_dpasPolicy,self.config.druid_dpasBen,self.config.druid_dpasCust)
                #couchbase
                self.cbPython = CouchbasePython(self.config.cb_url,self.config.cb_username,self.config.cb_password)

                
                self.currentDay = None
                self.nextCurrentDay = None

                self.dataPath = './data/'+ self.config.countryCode+'/'
                
                self.mapName =  self.getEventMapName()
                self.reverseMapName = {value:key for key, value in self.mapName.items()}
                self.eventStartField = 'eventDate'
                self.eventNameField = 'eventName'
                
                ##druid Queries
                self.eventsCountsTmp = self.getEventsCounts()
                # self.dauCount = self.getDAUCount()
                # self.mauCount = self.getMAUCount()
                # self.registrationLoginOcrDistinctOwner = self.getRegistrationLoginOcrDistinctOwner()
                # self.attributesIDTitleDistinctOwner = self.getAttributesIDTitleDistinctOwner()
                # self.events = self.getEvents()
                # self.referralGroupSize = self.getReferralGroupSize()
                # self.uniqueJoinedBy = self.getUniqueJoinedBy()
                # self.failureCauseCount = self.getFailureCauseCount()
                

                ##DPAS 
                self.dpasPolicyIdCount = None
                self.dpasBeneficiaryRelationTypeCount = None
                self.dpasMerged = None
                self.benCount = None
                

                if(self.config.dpasSwitch):
                    self.dpasPolicyIdCount = self.getDpasPolicyIdCount()
                    self.dpasBeneficiaryRelationTypeCount = self.getDpasBeneficiaryRelationTypeCount()
                    self.dpasMerged = self.getDpasMerged()
                    self.benCount = self.getBenCount()
                


                # self.eventStartField = 'auditDetail.createTime'
                # self.eventNameField = 'name'
                
                # ## create directory to save plots
                # Path('./plots').mkdir(parents=True, exist_ok=True)

                # self.stateInfo = {}
                # self.hardReload = self.config.hardReload
                # if not self.hardReload:
                #     print('IN....')
                    
                #     if not os.path.isdir(self.config.pathToSaveDataObject):
                #         print('>>ERROR: Please enter valid path to save dataObjects OR set hardReload to True .......')
                #         sys.exit()
                #     if os.path.isfile(self.config.pathToSaveDataObject+'stateInfo.json'):
                #         print('getting StateFiles...')
                #         with open(self.config.pathToSaveDataObject+'stateInfo.json') as json_file:
                #             self.stateInfo = json.load(json_file)
                #     else:
                #         print('>>INFO: No State file Found. Data reading will be from Static Files.')        



            
                self.customNameMap={'login_Success':'Login Success',
                'login_Failure':'Login Failure','social_login_success':'Social Login Success','Social_Login_Failure':'Social Login Failure',
                'Registration_Success':'Registration Success','Registration_Failure':'Registration Failure','Social_Registration_Success':'Social Registration Success'}

                # self.customerData = self.readStaticData(Path(self.dataPath + '/Customer'),'Customer')
                # self.staticData = self.getMergedData()

                #DPAS data
                # self.dpasCustomer = self.readStaticData(Path(self.dataPath + '/DPAS_Customer'),'DPAS_Customer' )
                # self.dpasPolicy = self.readStaticData(Path(self.dataPath + '/DPAS_Policy'),'DPAS_Policy')
                # self.wallet = self.readStaticData(Path(self.dataPath + '/Wallet'),'Wallet')
                # self.beneficiary = self.readStaticData(Path(self.dataPath + '/Beneficiary'),'Beneficiary')
                

                # self.defaultDate = pd.to_datetime(sorted(self.staticData[self.eventStartField])[-1],utc=True)
                
                self.defaultDate = pd.to_datetime(self.nextCurrentDay,utc=True) + pd.DateOffset(hours=8)
                self.week_ago = (self.defaultDate - pd.DateOffset(days=7)).tz_convert("UTC")
                # self.dateRanges  = self.staticData['auditDetail.createTime'].astype(str).str[:10].values
                self.dateRanges = self.eventsCountsTmp['eventDate'].astype(str).unique()
                print("dateRange - " + str(self.dateRanges))

                ###CSV's
                self.AppDownloadsCount = self.getAppDownloadsCount(self.dataPath+'App_Downloads.csv')
                self.eventsCounts = (self.eventsCountsTmp.append([self.AppDownloadsCount],ignore_index=True)) if(self.AppDownloadsCount is not None) else  self.eventsCountsTmp
                self.AppDownloadsIOSCount = self.getAppDownloadsIOSCount(self.dataPath+'App_Downloads_IOS.csv')

                
                self.AppInstallCount = self.getAppInstallCount(self.dataPath+'FIRST_OPEN.csv')
                self.AppInstallIOSCount = self.getAppInstallIOSCount(self.dataPath+'FIRST_OPEN_IOS.csv')
                self.AppRemove = self.getAppRemove(self.dataPath+'App_Remove.csv')
                self.failureCodeNameMap = self.getfailureCodeNameMap(self.dataPath+'failureCodeNameMap.csv')
                
                self.firebaseLoginCount = None
                self.firebaseRegistrationCount = None
                self.firebaseTrafficSource = None
                self.firebaseOsAppVersion = None
                self.firebaseBrandModel = None

                if(self.config.firebaseSwitch):
                    self.firebaseLoginCount = self.readCSVFiles(self.dataPath+'FIREBASE_LOGIN.csv','firebaseLoginReg')
                    self.firebaseRegistrationCount = self.readCSVFiles(self.dataPath+'FIREBASE_REGISTRATION.csv','firebaseLoginReg')
                    self.firebaseTrafficSource = self.readCSVFiles(self.dataPath+'FIREBASE_TRAFFIC_SOURCE.csv','trafficSource')
                    self.firebaseOsAppVersion = self.readCSVFiles(self.dataPath+'FIREBASE_OS_APP_VERSION.csv','firebaseOsApp')
                    self.firebaseBrandModel = self.readCSVFiles(self.dataPath+'FIREBASE_BRAND.csv','firebaseBrand')
                
                self.contentMap = self.readCSVFiles(self.dataPath+'Content_Map.csv','contentMap')
            
                # self.eventsCounts = self.getTotalEventsCounts(self.staticData,self.dataPath+'REGISTERED_USERS.csv')
                self.figures = {}
                # self.userScores = self.getUserScoresData(self.staticData)

                # self.policyOwnerEvents = self.eventsForPolicyOwners()
                
                self.startDate,self.endDate = self.get_start_end_dates(self.eventsCounts.copy(),'eventDate')
                print("start and endDate - " + str([self.startDate,self.endDate]))
                # self.endDate = self.get_start_end_dates(self.eventsCounts,'eventDate')[-1]
                # ,self.startDate.tzinfo,self.endDate.tzinfo)

                self.today = pd.to_datetime(dt.datetime.now(pytz.timezone('Asia/Singapore'))).date()
                # pd.to_datetime(dt.date.today(), utc=True).date()
                self.customStart = (pd.to_datetime(self.today,utc=True) - pd.DateOffset(days=config.defaultDuration-1)).tz_convert("UTC")
                self.customStart = pd.to_datetime(self.customStart,utc=True).date()
                print("Custom Range ->" + str([self.customStart,self.today]))
                
                self.eventsCountsHourly = self.getHourlyEventsCounts()

            self.lastupdated = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            self.chartFontSize = 12
            # if bool(self.stateInfo):
            #     print('>>INFO: saving StateFile...')
            #     with open(self.config.pathToSaveDataObject+'stateInfo.json', 'w') as outfile:
            #         json.dump(self.stateInfo, outfile)
            #     print('>>INFO: saved StateFile...')    

            ## must be last statement
            Util.__instance = self


    ## ------------------------------------ data Reading functions ---------------------------------------------

    def getAppDownloadsCount(self,filePath):
        if self.config.countryCode in self.config.featureFilter['appDownload']:
            dft = pd.read_csv(filePath,thousands=',').T
            dft.columns = dft.loc['Event'].values
            dft = dft.iloc[1:,]
            dft = dft.reset_index().rename(columns = {'index':'date'})
            dft = dft.dropna()
            dft['date'] = pd.to_datetime(dft['date'],dayfirst = True,utc = True)
            
            dft['App Download (Unique and Returning devices)*'] = dft['App Download (Unique and Returning devices)*'].apply(lambda x: x if pd.isnull(x) else int(x) )
            dft['App Download (Unique devices)*'] = dft['App Download (Unique devices)*'].apply(lambda x: x if  pd.isnull(x) else int(x) )
            dft['eventDate'] = dft['date'].astype(str).str[:10]

            data = []
            for index,row in dft.iterrows():
                data.append([ row.eventDate,'App Download','Unique and Returning devices',row['App Download (Unique and Returning devices)*']])
                data.append([ row.eventDate,'App Download','Unique devices',row['App Download (Unique devices)*']])

            dft = pd.DataFrame(data,columns=['eventDate','eventName','category','eventCount'])
            dft['tags1'] = dft['category']
            dft['displayName'] = dft['eventName']
            dft['timestamp'] = dft['eventDate'].apply(lambda x:x+'T00:00:00.000Z')
            dft = dft[dft.eventDate.isin(self.dateRanges) ]
            dft['uniqueDeviceCount'] = dft['eventCount']
            # if dft is not None:
            #     for dates in dft.eventDate.unique():
            #         tmp = dft[dft['eventDate'] == dates]
            #         tmp = tmp.groupby('eventDate').agg({'eventCount':sum})
            #         total = tmp['eventCount'].item()
            #         dft = dft.append({'eventDate' : dates , 'eventName' : 'App Download','category':'Total','eventCount':total,'tags1':'Total',
            #         'displayName':'App Download','timestamp':dates+'T00:00:00.000Z'} , ignore_index=True)
            
            return dft
        return None
    @Exception.normalException(utilLogger)
    def getAppDownloadsIOSCount(self,filePath):
        if self.config.countryCode in self.config.featureFilter['appDownload']:
            df = pd.read_csv(filePath)
            df = df.rename(columns={'Date':'eventDate','Installations':'count'})
            df['eventDate'] = pd.to_datetime(df['eventDate'],utc=True)
            df['eventDate'] = df['eventDate'].astype(str).str[:10]
            df = df[df.eventDate.isin(self.dateRanges) ]
            df['eventName'] = 'pulse.firstOpen'
            df['category'] = ''
            return df
        return None

    def getAppInstallCount(self,filePath):
        if self.config.countryCode in self.config.featureFilter['appInstall']:
            appIns = pd.read_csv(filePath).rename(columns={'event_date':'eventDate'})
            appIns['eventDate'] = pd.to_datetime(appIns['eventDate'],dayfirst = True,utc = True)
            appIns['eventDate'] = appIns['eventDate'].astype(str).str[:10]
            appIns = appIns[appIns.eventDate.isin(self.dateRanges) ]
            appIns['eventName'] = 'pulse.firstOpen'
            appIns['category'] = ''
            return appIns
        return None

    def getAppInstallIOSCount(self,filePath):
        if self.config.countryCode in self.config.featureFilter['appInstall']:
            appIns = pd.read_csv(filePath)
            appIns['eventDate'] = pd.to_datetime(appIns['eventDate'],format='%Y%m%d',utc = True)
            appIns['eventDate'] = appIns['eventDate'].astype(str).str[:10]
            appIns = appIns[appIns.eventDate.isin(self.dateRanges) ]
            appIns['eventName'] = 'pulse.firstOpen'
            appIns['category'] = ''
           
            return appIns
        return None

    def getAppRemove(self,filePath):
        if self.config.countryCode in self.config.featureFilter['firebaseAppRemove']:
            appIns = pd.read_csv(filePath)
            appIns['eventName'] = 'AppRemove'
            appIns['eventDate'] = pd.to_datetime(appIns['event_date'],format='%Y%m%d',utc = True)
            appIns['eventDate'] = appIns['eventDate'].astype(str).str[:10]
            
            
            appIns = appIns[appIns['eventDate'].isin(self.dateRanges)]
            return appIns
        return None

    @Exception.normalException(utilLogger)
    def getfailureCodeNameMap(self,path):
        if self.config.countryCode in self.config.featureFilter['loginFailure']:
            mapNam=  pd.read_csv(path, encoding= 'unicode_escape')
            mapNam.set_index('failureCode',inplace=True)
            return mapNam.to_dict()['Name']
        return None


    def getRegistrationEventsCount(self,frame,successRegFilePath):
        tagFilter = {'social':['Social.google','Social.facebook'],'convential':['Conventional.email']}
        tags = [item for sublist in tagFilter.values() for item in sublist]
      
        events = self.getReverseMapNames(self.config.regEvents[self.countryCode])
        res = frame[frame.name.isin(events)]
        if res.empty:
            return None

        if 'attributes._user' in self.config.eventsFilter[self.config.countryCode]:
        #exclude domains
            exclude = res[res['attributes._user'].astype(str).str.contains('ymail|yopmail')]['attributes._user'].unique()

        if 'attributes.success' in self.config.eventsFilter[self.config.countryCode]:    
            res = res[ (~res['attributes._user'].isin(exclude)) & (res['attributes.success'] == True) ]

        ## day wise tag wise count
        result = None
        if 'tags' in self.config.eventsFilter[self.config.countryCode]:
            res['tags'] = res['tags'].apply(lambda x: list( set(x) & set(tags))[0] if (set(x) & set(tags)) else 'Conventional.email')
            result = res.groupby([res['auditDetail.createTime'].astype(str).str[:10], 'name','tags' ]).agg({'attributes._user':pd.Series.nunique}).reset_index().rename(columns= {'auditDetail.createTime':'eventDate','name':'eventName','tags':'category','attributes._user':'count'})
        ## day wise total count
        userField = 'attributes._user' if 'attributes._user' in self.config.eventsFilter[self.config.countryCode] else 'owner'
        total = res.groupby([res['auditDetail.createTime'].astype(str).str[:10], 'name' ]).agg({userField : pd.Series.nunique}).reset_index().rename(columns= {'auditDetail.createTime':'eventDate','name':'eventName',userField:'count'})
        total['category'] = 'Total'
        ## add successfully registered users
        fiter = None
        if self.config.countryCode in self.config.featureFilter['successRegUsers']:
            fiter = pd.read_csv(successRegFilePath)
            fiter.DATE= pd.to_datetime(fiter.DATE).astype(str)
            exclude = fiter[fiter['EMAIL_DOMAIN'].astype(str).str.contains('ymail|yopmail')]['EMAIL_DOMAIN'].unique()
            fiter = fiter[(fiter.DATE.isin(res['auditDetail.createTime'].str[:10].values)) & (~fiter['EMAIL_DOMAIN'].isin(exclude))  ]
            fiter['eventName'] = 'Successfully Registered'
            fiter = fiter.drop(['EMAIL_DOMAIN'],axis=1).rename(columns= {'MODE':'category','COUNT':'count','DATE':'eventDate'})
            fiter = fiter.groupby(['eventDate','eventName','category']).agg({'count':'sum'}).reset_index()

        result = pd.concat([result,fiter,total]).sort_values(['eventDate'])
        
        return result

    def getBabylonCount(self,df):
        
        res = df[(df[self.eventNameField].str.startswith('pulse.babylon')) & (df[self.eventNameField].isin(self.mapName.keys())) ]

        res = res.groupby([res['auditDetail.createTime'].astype(str).str[:10], 'name' ]).agg({'owner':pd.Series.nunique}).reset_index().rename(columns= {'auditDetail.createTime':'eventDate','name':'eventName','owner':'count'})
        res['category'] = ''
        return res        

    def getTotalEventsCounts(self,frame,successRegFilePath):
        countDf = pd.concat([self.AppDownloadsCount, self.AppInstallCount,
                            self.getRegistrationEventsCount( frame.copy(), successRegFilePath),
                            self.getBabylonCount(frame.copy() ) ])


        restDf = frame[ (frame['name'].isin(self.mapName.keys()) ) & (~frame['name'].isin(countDf.eventName.values) ) ].groupby([ frame['auditDetail.createTime'].astype(str).str[:10],'name' ]).agg({'owner':'count'}).reset_index().rename(columns= {'auditDetail.createTime':'eventDate','name':'eventName','owner':'count'})                   

        restDf['category'] = ''
        ## combine all
        restDf = countDf.append([restDf])
        
        restDf['displayName'] = restDf['eventName'].apply(lambda x:  self.mapName[x] if (x in self.mapName.keys()) else x)
        return restDf


    def readCSVFiles(self,path,type):
        if self.config.countryCode in self.config.featureFilter[type]:
            return pd.read_csv(path)
        return None


    def getEventMapName(self):
        # mapNam=  pd.read_csv(self.dataPath+'eventNameMap.csv')
        mapNam=  pd.read_csv('./policySales/eventMap_polSales.csv')
        mapNam['EVENT_NAME'] = mapNam['EVENT_NAME'].apply(lambda x:x.split('-')[0].strip())
        mapNam.set_index('EVENT_NAME',inplace=True)
        return mapNam.to_dict()['NAME']            

    def fixUtcTime(self,df,columnName):
        if self.config.timeOperator == '+':
            df[columnName] = pd.to_datetime(df[columnName],utc=True)
           
            df[columnName] = df[columnName].apply(lambda x: x + dt.timedelta(hours=self.config.timeHourDelta,minutes=self.config.timeMinuteDelta))
            df[columnName] = df[columnName].astype('str') 
            return df[columnName]

    def readStaticData(self,path,type):
       
        all_files = glob.glob( str(Path.joinpath(path , "*.json")) )
        print('Total Files for '+type+' - ',str(all_files))
        allDf = []
        
        for filename in all_files:
            
            if not bool(self.stateInfo) or os.path.basename(filename) not in self.stateInfo['savedObjects'] or not os.path.isfile(self.config.pathToSaveDataObject+ type+'.ftr'):
                print('Reading ' +os.path.basename(filename)+' as a static file..')
                with open(filename,encoding='cp1252') as f: 
                    d = json.load(f)
                d = list(map(lambda x:list(x.values())[0],d))
                dfJs = json_normalize(d)
                
                allDf.append(dfJs)

                # if os.path.basename(filename) not in self.stateInfo.savedObjects:
                if 'savedObjects' in self.stateInfo.keys():
                    self.stateInfo['savedObjects'].append(os.path.basename(filename))
                else:
                    self.stateInfo['savedObjects'] = [os.path.basename(filename)]
            else:
                print('Found in Saved Objects ' +os.path.basename(filename)+'..')    


        if len(allDf)>0:        

            final = pd.concat(allDf, axis=0, ignore_index=True)
            if(type=='Wallet'):
            
                final['points'] =final['loyaltyPoints'].apply(lambda x: 0 if (pd.isna(x)) else int(x[0]['points']) ) 
            
            else: 
                if('startDate' in final.columns):
                    final[self.eventStartField] = final[self.eventStartField].fillna(final.startDate)
                final[self.eventStartField]=self.fixUtcTime(final,self.eventStartField)
                
                ## Review
                
                final["TMP"] = final[self.eventStartField].values                # index is a DateTimeIndex
                final = final[final.TMP.notnull()]                  # remove all NaT values
                final.drop(["TMP"], axis=1, inplace=True) 
                
                
                ##

            if type == 'Customer':
                final = self.getCustomerData(final)
            elif type== 'DPAS_Customer':
                final = self.getDpasCustomer(final)
        

        if len(all_files) > len(allDf) and bool(self.stateInfo):
            print(">>INFO: Reading data Objects for "+type)
            savedDf = pd.read_feather(self.config.pathToSaveDataObject+ type+'.ftr', use_threads=True)
            if len(allDf) > 0:
                final = pd.concat([savedDf,final], axis=0, ignore_index=True)
            else:
                final = savedDf    

        if len(allDf)> 0:
            print(">>INFO: Writing Data Objects for "+type)
            try:
                final.to_feather(self.config.pathToSaveDataObject+ type+'.ftr')
            except Exception as e:
                print('>>ERROR: Data Object writing is failed... /n' + str(e))    
                


        return final

    def replaceEventNames(self,df):
        idToMap = self.config.idToEventMap
        df[self.eventNameField] = df.apply(lambda x : idToMap[x['attributes.id']] if(x['attributes.id'] in idToMap.keys()) else x[self.eventNameField], axis=1)
        return df


    def getMergedData(self):
        if self.config.dataSource == 'druid':
            print('Getting data From druid..')
            druidObj = DruidPython(self.config.druid_dataSource,self.config.druidUrl)
            events_df = druidObj.getEventsData()
        else:
            events_df = self.readStaticData(Path(self.dataPath + '/Events'),'Event')
            events_df = events_df[ self.config.eventsFilter[self.config.countryCode] ]  
        
        events_df['startDate'] = self.fixUtcTime(events_df,'startDate')
        events_df['completionDate'] = self.fixUtcTime(events_df,'completionDate')

        events_df['startDateReal'] = events_df.startDate
        events_df.startDate = events_df.startDate.fillna(events_df.completionDate)
        
        
        if 'attributes.id' in self.config.eventsFilter[self.config.countryCode]:
           
            events_df = self.replaceEventNames(events_df)
        
        

        customer_df = self.customerData
    
        final = pd.merge(events_df, customer_df, left_on='owner',right_on='id',how='left')
        final[['agegroup']]=final[['agegroup']].fillna('Unknown')
        
        return final
    
    def getCustomerData(self,customer_df):
        
        customer_df = customer_df[ self.config.customerFilter ]
        
        customer_df = customer_df.rename(columns = {'auditDetail.createTime':'customer.auditDetail.createTime'})
        customer_df['dob'] = pd.to_datetime(customer_df['dob'],dayfirst = True,errors='coerce')
        now = pd.Timestamp('now')
        customer_df['age'] = (now - customer_df['dob']).astype('<timedelta64[Y]') 
        customer_df['agegroup'] = self.find_age_group(customer_df,'age')
        return customer_df

    def getDpasCustomer(self,dpasCust):
        dpasCust['dob'] = pd.to_datetime(dpasCust['dob'],dayfirst = True,errors='coerce')
        now = pd.Timestamp('now')
        dpasCust['age'] = (now - dpasCust['dob']).astype('<timedelta64[Y]') 
        dpasCust['agegroup'] = self.find_age_group(dpasCust,'age')
        
        return dpasCust

    ## ---------------------------------------- Query result fetcher ----------------------------------

    ##This function should always execute first since 
    ## It keeps track of the latest day to fetch the data
    def getEventsCounts(self):

        self.currentDay = self.nextCurrentDay
    
        # start = self.currentDay
        print(">>INFO: Getting Events Counts .. .. ..")
        if self.currentDay is not None:
            print(">>INFO: Getting data for date: "+ self.currentDay)
        
        ''' ###########tageWise
        df = self.druidPython.daywiseEventsCountsTagWise(self.currentDay)
        
        df['tags2'] = df['tags2'].fillna(df['tags1'])
        df = df.rename(columns= {'name':'eventName','tags2':'category'})
        df = df[df['category'].notnull()]

        unwanted = df.groupby('eventName').agg({'category':pd.Series.nunique}).reset_index()
        unwanted_list = unwanted[unwanted['category']<=1]['eventName']
        df = df[~df['eventName'].isin(unwanted_list)]
        '''

        
        total = self.druidPython.daywiseEventsCountsTotal(self.currentDay)
        total = total.rename(columns= {'name':'eventName'})
        total['category'] = 'Total'
        # total['tags1'] = 'Total'

        # regEvents = self.druidPython.getRegEvents(self.currentDay)
        ''' active Users '''
        dauCounts = self.druidPython.DAUCounts(self.currentDay)
        dauCounts['eventName'] = 'Active Users'
        dauCounts['category'] = 'Total'
        dauCounts['eventCount'] = dauCounts['uniqueOwnerCount']

        ''' ###########productWise '''
        try:
            dfPro = self.druidPython.daywiseEventsCountsProductWise(self.config.druid_productCode,self.currentDay)
            
            dfPro = dfPro.rename(columns= {'name':'eventName','productCode':'category'})
            dfPro = dfPro[dfPro['category'].notnull()]

            unwanted = dfPro.groupby('eventName').agg({'category':pd.Series.nunique}).reset_index()
            unwanted_list = unwanted[unwanted['category']<=1]['eventName']
            dfPro = dfPro[~dfPro['eventName'].isin(unwanted_list)]
        except:
            dfPro = pd.DataFrame()
        

        # ''' Health assessment Completed combined  '''
        # healthAssEnd = self.druidPython.getEventCountsBypattern('%pulse.babylon.healthAssessment.chat.%.end',self.currentDay)
        # healthAssEnd['eventName'] = 'Health Assessment Completed'
        # healthAssEnd['category'] = 'Total'
        # healthAssEnd['eventCount'] = healthAssEnd['uniqueOwnerCount']

        df = pd.concat([total,dauCounts,dfPro],axis=0).sort_values(['eventDate','eventName'])
        

        ##Contents
        '''try:
            self.attributesIDTitleDistinctOwner
        except:
            print("attributesIDTitleDistinctOwner is not initialized")
            content_df = self.druidPython.AttributesIDDistinctOwner(self.currentDay)

        else:
            # content_df =  self.attributesIDTitleDistinctOwner
            content_df = self.druidPython.AttributesIDDistinctOwner(self.currentDay)


        if 'attributes.id' in self.config.eventsFilter[self.config.countryCode]:
           
            content_df = self.replaceEventNames(content_df)
        
        content_df = content_df.groupby(['eventDate','eventName']).agg({"eventCount":sum,"uniqueOwnerCount":sum}).reset_index()
        content_df['category'] = 'Total'
        content_df['tags1'] = 'Total'
        content_df['timestamp'] = content_df['eventDate'].apply(lambda x:x+'T00:00:00.000Z')


        df = df[df['eventName'] != 'pulse.viewContent']
        df = pd.concat([df,content_df],axis=0).sort_values(['eventDate','eventName'])'''
        df['displayName'] = df['eventName'].apply(lambda x:  self.mapName[x] if (x in self.mapName.keys()) else x)
        # df.to_csv('adad.csv')
        
        try:
            self.eventsCountsTmp
        except:
            print("eventsCountsTmp is not initialized")
        
        else:
            print(f'>>INFO: Unique timestamps --  {self.eventsCountsTmp["timestamp"].unique()}')
            print(f'>>INFO: CurrentDay --  {self.currentDay}')
            if self.eventsCountsTmp is not None:
                tmp = self.eventsCountsTmp.copy()
                tmp = tmp.drop(tmp[tmp['timestamp'] >= self.currentDay].index)
                df = pd.concat([tmp, df], axis=0)
        
        self.dateRanges = df['eventDate'].astype(str).unique()        
        self.nextCurrentDay = df.timestamp.unique()[-1]
        self.nextCurrentDay = (pd.to_datetime(self.nextCurrentDay) -  dt.timedelta(hours= 8 )).strftime('%Y-%m-%dT%H:%M:%SZ')
        print(">>INFO: Setting date checkpoint: "+ self.nextCurrentDay)
        df.reset_index(drop=True,inplace=True)

        print("Unique dates : " + str(df['eventDate'].unique()) )

        ##Temp changes make all thing unique
        df['eventCount'] = df['uniqueOwnerCount']
        df[['eventCount','uniqueOwnerCount']] = df[['eventCount','uniqueOwnerCount']].fillna(0)
        # df.to_csv('test.csv')
        return df


    def getHourlyEventsCounts(self):

        print(">>INFO: Getting Hourly Events Counts .. .. ..")

        
        if self.config.inProduction:
            startTime = dt.datetime.now(pytz.timezone('Asia/Singapore')).replace(tzinfo=None)
            starteTime = (pd.to_datetime(startTime)- dt.timedelta(hours=20)).strftime('%Y-%m-%dT%H:%M:%S')
        else:
            starteTime = (pd.to_datetime(self.nextCurrentDay)+ dt.timedelta(hours=0)).strftime('%Y-%m-%dT%H:%M:%S')


        
        total = self.druidPython.daywiseEventsCountsTotal(starteTime,granularity='hour')
        total = total.rename(columns= {'name':'eventName'})
        total['category'] = 'Total'
        total['tags1'] = 'Total'


        ''' ###########productWise '''
        try:
            dfPro = self.druidPython.daywiseEventsCountsProductWise(self.config.druid_productCode,starteTime,granularity='hour')
            
            dfPro = dfPro.rename(columns= {'name':'eventName','productCode':'category'})
            dfPro = dfPro[dfPro['category'].notnull()]

            unwanted = dfPro.groupby('eventName').agg({'category':pd.Series.nunique}).reset_index()
            unwanted_list = unwanted[unwanted['category']<=1]['eventName']
            # print(unwanted)
            dfPro = dfPro[~dfPro['eventName'].isin(unwanted_list)]
        except:
            dfPro = pd.DataFrame()
            
        # regEvents = self.druidPython.getRegEvents(starteTime,granularity='hour')

        # ''' Health assessment Completed combined  '''
        # healthAssEnd = self.druidPython.getEventCountsBypattern('%pulse.babylon.healthAssessment.chat.%.end',starteTime,granularity='hour')
        # healthAssEnd['eventName'] = 'Health Assessment Completed'
        # healthAssEnd['category'] = 'Total'
        # healthAssEnd['eventCount'] = healthAssEnd['uniqueOwnerCount']

        df = pd.concat([total,dfPro],axis=0).sort_values(['eventDate','eventName'])
        df['displayName'] = df['eventName'].apply(lambda x:  self.mapName[x] if (x in self.mapName.keys()) else x)
        # df.to_csv('./hour_counts.csv',index=False)

        df['eventCount'] = df['uniqueOwnerCount']
        return df



    def getDAUCount(self):

        df = self.druidPython.DAUCounts(self.currentDay)
        try:
            self.dauCount
        except:
            print("dauCount is not initialized")
        
        else:
        
            if self.dauCount is not None:
                self.dauCount.drop(self.dauCount[self.dauCount['timestamp'] >= self.currentDay].index, inplace = True)
                df = pd.concat([self.dauCount, df], axis=0)

        df.reset_index(drop=True,inplace=True)
        return df
        
    def getMAUCount(self):

        df = self.druidPython.MAUCount(self.currentDay)
        return df

    def getRegistrationLoginOcrDistinctOwner(self):

        df = self.druidPython.RegistrationLoginOcrDistinctOwner(self.currentDay)
        df['displayName'] = df['eventName'].apply(lambda x:  self.mapName[x] if (x in self.mapName.keys()) else x)
        
        try:
            self.registrationLoginOcrDistinctOwner
        except:
            print("registrationLoginOcrDistinctOwner is not initialized")
        
        else:
        
            if self.registrationLoginOcrDistinctOwner is not None:
                self.registrationLoginOcrDistinctOwner.drop(self.registrationLoginOcrDistinctOwner[self.registrationLoginOcrDistinctOwner['timestamp'] == self.currentDay].index, inplace = True)
                df = pd.concat([self.registrationLoginOcrDistinctOwner, df], axis=0)
        
        df.reset_index(drop=True,inplace=True)
        
        return df

    def getAttributesIDTitleDistinctOwner(self):
        df = self.druidPython.AttributesIDDistinctOwner(self.currentDay)
        df['displayName'] = df['eventName'].apply(lambda x:  self.mapName[x] if (x in self.mapName.keys()) else x)
        
        
        try:
            self.attributesIDTitleDistinctOwner
        except:
            print("attributesIDTitleDistinctOwner is not initialized")
        
        else:
        
            if self.attributesIDTitleDistinctOwner is not None:
                ##chech if it should be timestamp or eventDate
                self.attributesIDTitleDistinctOwner.drop(self.attributesIDTitleDistinctOwner[self.attributesIDTitleDistinctOwner['timestamp'] == self.currentDay].index, inplace = True)
                df = pd.concat([self.attributesIDTitleDistinctOwner, df], axis=0)
        
        df.reset_index(drop=True,inplace=True)
        
        return df


    def getDpasPolicyIdCount(self):
        df = self.druidPython.DpasPolicyIdCount(self.currentDay)

        try:
            self.dpasPolicyIdCount
        except:
            print("dpasPolicyIdCount is not initialized")
        
        else:
        
            if (self.dpasPolicyIdCount is not None):
                if(~ self.dpasPolicyIdCount.empty) :
                    self.dpasPolicyIdCount.drop(self.dpasPolicyIdCount[self.dpasPolicyIdCount['timestamp'] == self.currentDay].index, inplace = True)
                    df = pd.concat([self.dpasPolicyIdCount, df], axis=0)
                    df.reset_index(drop=True,inplace=True)
        return df

    @Exception.normalException(utilLogger)
    def getDpasBeneficiaryRelationTypeCount(self):
        df = self.druidPython.DpasBeneficiaryRelationTypeCount(self.currentDay)
        try:
            self.dpasBeneficiaryRelationTypeCount
        except:
            print("dpasBeneficiaryRelationTypeCount is not initialized")
        
        else:
        
            if (self.dpasBeneficiaryRelationTypeCount is not None):
                if ( ~self.dpasBeneficiaryRelationTypeCount.empty):
                    self.dpasBeneficiaryRelationTypeCount.drop(self.dpasBeneficiaryRelationTypeCount[self.dpasBeneficiaryRelationTypeCount['timestamp'] == self.currentDay].index, inplace = True)
                    df = pd.concat([self.dpasBeneficiaryRelationTypeCount, df], axis=0)
                    df.reset_index(drop=True,inplace=True)
        return df
    
    @Exception.normalException(utilLogger)
    def getReferralGroupSize(self):
        df = self.druidPython.ReferralGroupSize(self.currentDay)
        try:
            self.referralGroupSize
        except:
            print("referralGroupSize is not initialized")
        
        else:
        
            if (self.referralGroupSize is not None):
                if( ~self.referralGroupSize.empty):
                    self.referralGroupSize.drop(self.referralGroupSize[self.referralGroupSize['timestamp'] == self.currentDay].index, inplace = True)
                    df = pd.concat([self.referralGroupSize, df], axis=0)
                    df.reset_index(drop=True,inplace=True)
        return df


    def getEvents(self):
        print('>>INFO: Getting Events and Customer merged data....')

        
        df = self.druidPython.getPulseEventsData()
        
        df['startDate'] = self.fixUtcTime(df,'startDate')
        df['completionDate'] = self.fixUtcTime(df,'completionDate')

        df['startDateReal'] = df.startDate
        df.startDate = df.startDate.fillna(df.completionDate)

        customer_df = self.druidPython.getPulseCustomerData()
        
        customer_df['dob'] = pd.to_datetime(customer_df['dob'],dayfirst = True,errors='coerce')
        now = pd.Timestamp('now')
        customer_df['age'] = (now - customer_df['dob']).astype('<timedelta64[Y]') 
        
        customer_df['agegroup'] = self.find_age_group(customer_df,'age')
        customer_df.replace(r'^\s*$', np.nan, regex=True,inplace=True)

        df = pd.merge(df, customer_df, left_on='owner',right_on='id',how='left')
        df['agegroup']=df['agegroup'].cat.add_categories('Unknown').fillna('Unknown')

        df[['sex']]=df['sex'].str.upper().fillna('Unknown')
        
        print('>>INFO: Successfully created Events and Customer merged data....')
        
        return df

    @Exception.normalException(utilLogger)
    def getDpasMerged(self):
        print('>>INFO: Getting DPAS Policy and DPAS Customer merged data....')
        df = self.druidPython.getDPASPolicyData()
        customer_df = self.druidPython.getDPASCustomerData()
        customer_df['dob'] = pd.to_datetime(customer_df['dob'],dayfirst = True,errors='coerce')
        now = pd.Timestamp('now')
        customer_df['age'] = (now - customer_df['dob']).astype('<timedelta64[Y]') 
        
        customer_df['agegroup'] = self.find_age_group(customer_df,'age')
        customer_df.replace(r'^\s*$', np.nan, regex=True,inplace=True)

        df = pd.merge(df, customer_df, left_on='clients0',right_on='id',how='left')
        df['agegroup']=df['agegroup'].cat.add_categories('Unknown').fillna('Unknown')
        df[['sex']]=df['sex'].str.upper().fillna('Unknown')

        print('>>INFO: Successfully created DPAS Policy and DPAS Customer merged data....')
        
        return df

    def getBenCount(self):
        print('>>INFO: Getting Benificary Count....')
        df= self.druidPython.BenCount()
        
        return df

    @Exception.normalException(utilLogger)
    def getUniqueJoinedBy(self):

        df = self.druidPython.UniqueJoinedBy()
        return df

    @Exception.normalException(utilLogger)
    def getFailureCauseCount(self):

        df = self.druidPython.FailureCauseCount(self.currentDay)
        try:
            self.failureCauseCount
        except:
            print("failureCauseCount is not initialized")
        
        else:
        
            if (self.failureCauseCount is not None):
                if ( ~self.failureCauseCount.empty):
                    self.failureCauseCount.drop(self.failureCauseCount[self.failureCauseCount['timestamp'] == self.currentDay].index, inplace = True)
                    df = pd.concat([self.failureCauseCount, df], axis=0)
                    df.reset_index(drop=True,inplace=True)

        return df



    @Exception.normalException(utilLogger)
    def doUpdateQueryResults(self):
        print('>>INFO: Updating data ...')
        # if(self.currentDay is not None):
        #     self.today = pd.to_datetime(self.currentDay,utc=True).date()
        # self.startDate,self.endDate = self.get_start_end_dates(self.eventsCounts.copy(),'eventDate')
        # print("New Start and End Date "+ str(self.startDate) + ' - ' +str(self.endDate))
         
            

        self.eventsCountsTmp = self.getEventsCounts()
        # self.dauCount = self.getDAUCount()
        # self.mauCount = self.getMAUCount()
        # self.registrationLoginOcrDistinctOwner = self.getRegistrationLoginOcrDistinctOwner()

        # self.attributesIDTitleDistinctOwner = self.getAttributesIDTitleDistinctOwner()
        
        # self.events = self.getEvents()
        print('>>INFO: Updating .. .. ...')
        # self.referralGroupSize = self.getReferralGroupSize()
        # self.failureCauseCount = self.getFailureCauseCount()

        if(self.config.dpasSwitch):
            self.dpasPolicyIdCount = self.getDpasPolicyIdCount()
            self.dpasBeneficiaryRelationTypeCount = self.getDpasBeneficiaryRelationTypeCount()



        ##--------------------------------CSV Refresh-------------------------##
        print('>>INFO: Refreshing CSV\'s ...')

        csvFiles = ['eventNameMap','App_Downloads','App_Downloads_IOS','FIRST_OPEN','FIRST_OPEN_IOS','App_Remove',
        'failureCodeNameMap','FIREBASE_LOGIN','FIREBASE_REGISTRATION','FIREBASE_TRAFFIC_SOURCE','FIREBASE_OS_APP_VERSION',
        'FIREBASE_BRAND','Content_Map']

        modified = []
        try:
            for files in csvFiles:
            
                file_mod_time = dt.datetime.fromtimestamp(os.stat(self.dataPath+files+'.csv').st_mtime) 
                # print(file_mod_time)
                now = dt.datetime.today()
                max_delay = timedelta(minutes= (self.config.refreshInterval / 60) + 5)

                if now-file_mod_time <= max_delay:
                    print('>>INFO: ' + files + ' got Modified Before ' + str(now-file_mod_time))
                    modified.append(files)
                    
            print(">>INFO: Files to be Refreshed " + str(modified))
            if 'eventNameMap' in modified:
                ## added eventNameMap
                self.mapName =  self.getEventMapName()
                self.reverseMapName = {value:key for key, value in self.mapName.items()}
            if 'App_Downloads' in modified:
                self.AppDownloadsCount = self.getAppDownloadsCount(self.dataPath+'App_Downloads.csv')
            if 'App_Downloads_IOS' in modified:
                self.AppDownloadsIOSCount = self.getAppDownloadsIOSCount(self.dataPath+'App_Downloads_IOS.csv')

            if 'FIRST_OPEN' in modified:
                self.AppInstallCount = self.getAppInstallCount(self.dataPath+'FIRST_OPEN.csv')
            if 'FIRST_OPEN_IOS' in modified:
                self.AppInstallIOSCount = self.getAppInstallIOSCount(self.dataPath+'FIRST_OPEN_IOS.csv')
            if 'App_Remove' in modified:
                self.AppRemove = self.getAppRemove(self.dataPath+'App_Remove.csv')
            if 'failureCodeNameMap' in modified:
                self.failureCodeNameMap = self.getfailureCodeNameMap(self.dataPath+'failureCodeNameMap.csv')
        
            if(self.config.firebaseSwitch):
                if 'FIREBASE_LOGIN' in modified:
                    self.firebaseLoginCount = self.readCSVFiles(self.dataPath+'FIREBASE_LOGIN.csv','firebaseLoginReg')
                if 'FIREBASE_REGISTRATION' in modified:
                    self.firebaseRegistrationCount = self.readCSVFiles(self.dataPath+'FIREBASE_REGISTRATION.csv','firebaseLoginReg')
                if 'FIREBASE_TRAFFIC_SOURCE' in modified:
                    self.firebaseTrafficSource = self.readCSVFiles(self.dataPath+'FIREBASE_TRAFFIC_SOURCE.csv','trafficSource')
                if 'FIREBASE_OS_APP_VERSION' in modified:
                    self.firebaseOsAppVersion = self.readCSVFiles(self.dataPath+'FIREBASE_OS_APP_VERSION.csv','firebaseOsApp')
                if 'FIREBASE_BRAND' in modified:
                    self.firebaseBrandModel = self.readCSVFiles(self.dataPath+'FIREBASE_BRAND.csv','firebaseBrand')
            
            if 'Content_Map' in modified:
                self.contentMap = self.readCSVFiles(self.dataPath+'Content_Map.csv','contentMap')
        except:
            print('>>ERROR: Error in csv updation..')

        self.eventsCounts = (self.eventsCountsTmp.append([self.AppDownloadsCount],ignore_index=True)) if(self.AppDownloadsCount is not None) else  self.eventsCountsTmp            
        self.eventsCountsHourly = self.getHourlyEventsCounts()
        if(self.currentDay is not None):
            self.today = (pd.to_datetime(self.currentDay,utc=True) +  pd.DateOffset(hours=8)).date()
            self.defaultDate = pd.to_datetime(self.nextCurrentDay,utc=True) + pd.DateOffset(hours=8)
            self.week_ago = (self.defaultDate - pd.DateOffset(days=7)).tz_convert("UTC")
            print(f'>>INFO: Today Date updated - {self.today}')
        self.startDate,self.endDate = self.get_start_end_dates(self.eventsCounts.copy(),'eventDate')
        print("New Start and End Date "+ str(self.startDate) + ' - ' +str(self.endDate))
        
            

        print('>>INFO: Fetched updated data ...')



    

               
#================================================ Util functions =====================================================================
    def getReverseMapNames(self,events):
        if type(events) != list:
            return self.reverseMapName[events.strip()] if events.strip() in self.reverseMapName.keys() else events.strip()
        return [self.reverseMapName[x.strip()] if x.strip() in self.reverseMapName.keys() else x.strip() for x in events ]

    def find_age_group(self,df,ageColumn):


        bins= [0,17,25,35,45,55,65,90,150]
        labels = ['<18','18-25','26-35','36-45','46-55','56-65','66-90','>90']
        df['ageGroup'] = pd.cut(df[ageColumn], bins=bins, labels=labels, right=True)
        # df['ageGroup'].fillna('Unknown',inplace=True)
        return df['ageGroup']


    def get_annotations(self,text,x,y):
        return [go.layout.Annotation(
            text=text,
            align='right',
            showarrow=False,
            xref='paper',
            yref='paper',
            x=x,
            y=y,
            
        )]

    def getGraphTitle(self,title,y= 0.9):
        return {
        'text': '<b>'+title+'</b>',
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top',
        'font':{
        # 'family':"Courier New, monospace",
        'size':15,
        # 'color':"#7f7f7f"
        }}
    @Exception.normalException(utilLogger)
    def getDataFramebyTime(self, dateColumn,period,df,dateRange=False,startDate=None,endDate=None):

        if(df is not None):   
            if df[dateColumn].size > 0:
                if (not self.defaultDate):
                    self.defaultDate = pd.to_datetime(sorted(df[dateColumn])[-1],utc=True)
                    # print("default",self.defaultDate, self.defaultDate.tzinfo)           

                if(dateRange==True):
                    
                    ##for specifying how many days prior to the current date we need
                    if(isinstance(startDate,int)):
                        print("day wise")
                        startDate = startDate-1

                        end = pd.to_datetime(dt.date.today(), utc=True)
                        ##change to startDate here
                        start = (end - pd.DateOffset(days=startDate)).tz_convert("UTC")
                        print(start,end)
                    else:
                        start = pd.to_datetime(startDate,utc=True)
                        end = pd.to_datetime(endDate,utc=True)

                    if((start==self.startDate) & (end == self.endDate)):
                        res = df
                        
                    else:

                        df['tempdate'] = df[dateColumn].astype(str).str[:10]
                        res = df[( pd.to_datetime(df['tempdate'],utc=True) >= start ) & ( pd.to_datetime(df['tempdate'],utc=True) <=end)].sort_values('tempdate')
                        res = res.drop(columns=['tempdate'])
                
                    


                else:
                    if period == 'today':
                        print('\n DefaultDate ->'+  str(self.defaultDate))
                        res = df[df[dateColumn].astype(str).str[:10] == str(self.defaultDate)[:10] ]
                    
                    elif period == 'lastWeek':
                            self.week_ago = (self.defaultDate - pd.DateOffset(days=7)).tz_convert("UTC")
                            # print("week_ago",self.week_ago)
                            res = df[ (pd.to_datetime(df[dateColumn],utc=True)>= self.week_ago) & (pd.to_datetime(df[dateColumn],utc=True)< self.defaultDate) ] 
                            # print(res)


                    elif period == 'lastMonth':
                            month_ago = (self.defaultDate - pd.DateOffset(days=30)).tz_convert("UTC")
                            res = df[ pd.to_datetime(df[dateColumn],utc=True)>= month_ago ]
                        
                    else:
                        res = df.copy()
                
                
                return res
        else:
            print("Empty Datframe or None object passed")
            return None

    #get percentage of end event to start event
    def get_percentage_cards(self,start_event,end_event):
        
        start_count = self.get_event_count(start_event,'all')[0]
        end_count = self.get_event_count(end_event,'all')[0]
        if start_count and end_count:
            percentage = int((end_count/start_count)*100)
            
            return percentage
        return None
        
    #return the count for each event
    def get_event_count(self,event,filter,dateColumn='eventDate'):     

        res = pd.DataFrame()
        for event_name in event:
            ## To handle category specific events
            category = None
            if len(event_name.split('-')) == 2:
                event_name,category = event_name.split('-')

            event_name = self.getReverseMapNames(event_name) 
            df_tmp = self.eventsCounts[self.eventsCounts.eventName == event_name]
            if not category:
                category = 'Total'
            df_tmp = df_tmp[df_tmp['category'] == category]
            res = res.append(df_tmp)
                
        res = self.getDataFramebyTime(dateColumn,filter,res)
        
        if res is not None :
            if (not res.empty):    
            # return res['count'].sum(), int(res['count'].mean())
        
                return res['eventCount'].sum(), int(res['eventCount'].mean())
        
        return 0,0

    def get_start_end_dates(self,df=None,dateCol=None):
        if df is None:
            # df = self.staticData.copy()
            df = self.eventsCounts.copy()
        if dateCol==None:
            dateCol=self.eventStartField
        start = (pd.to_datetime(sorted(df[dateCol])[0],utc=True)).date()        
        end = (pd.to_datetime(sorted(df[dateCol])[-1],utc=True)).date()
        
        return start,end

    def get_summary_metrics_table(self,start,end,granularity = '',eventFilter=None):

        if granularity == 'hour':
            res = self.eventsCountsHourly.copy()
        else:
            res = self.eventsCounts.copy()

        start = pd.to_datetime(start)
        end = pd.to_datetime(end)
           
    
        

        if (not res.empty):
            
            if granularity != 'hour':
                res = res[( pd.to_datetime(res['eventDate']) >= start ) & ( pd.to_datetime(res['eventDate']) <=end)].sort_values('eventDate')
            if granularity == 'hour':
                res['eventDate'] = res.eventDate.str[8:13]
            if eventFilter:
                eventsFilter = eventFilter
            else:    
                eventsFilter = list(self.mapName.keys()) + ['App Download']
            res = res.query('eventName in @eventsFilter')
            
            res['eventCount'] = res.apply(lambda x: x['uniqueDeviceCount'] if(x['eventName'] in self.getReverseMapNames(['App Install','Initiate Registration','Registration Completed','Registration - Email OTP'])) else x['eventCount'],axis=1)

            pivoted = res.pivot_table(index=['displayName','category'],columns=['eventDate'],values=['eventCount']).reset_index()
            pivoted.columns = [x[0] if x[0] in ['displayName','category'] else x[1] for x in pivoted.columns ] 
            
        
            pivoted.fillna(0,inplace=True)

            ##sorting
            sorterIndex = dict(zip(self.config.sorter,range(len(self.config.sorter))))
            pivoted['Tm_Rank'] = pivoted['displayName'].map(sorterIndex)

            pivoted.sort_values(['Tm_Rank'], ascending = [True], inplace = True)
            pivoted.drop('Tm_Rank', 1, inplace = True)
           
            datau  = pivoted.to_dict('records')

            ## using this to add fixed size for all date columns
            # print(pivoted.columns[2:])
            # styleDataColumns = [{'if': {'column_id': c},'width': '100px'} for c in pivoted.columns[2:] ]

            return dash_table.DataTable(
                                        
                                        data = datau,
                                        columns=[
                                            {"name": i, "id": i} for i in pivoted.columns
                                        ],
                                        page_size=30,
                                        sort_action='native',
                                        sort_mode='multi',
                                        filter_action="native",
                                        fixed_columns={'headers': True, 'data': 2 },
                                        style_table={'overflowX': 'scroll','maxWidth': '1200px', 'text-align':'center'},
                                        # 'padding-left':'20px','padding-right':'20px'},
                                        # style_data = {'text-align':'center',
                                        # 'whiteSpace': 'normal',
                                        # 'height': 'auto',
                                        # 'width':'auto'
                                        # },
                                        # style_data={'whiteSpace': 'normal'},
                                        # content_style='grow',
                                        style_cell = {"fontFamily": "Arial", "size": 10, 'textAlign': 'center',
                                        'whiteSpace': 'normal',
                                        'height': 'auto',
                                        'width':'auto',
                                        'minWidth': '100px',
                                        'maxWidth': '180px'
                                        # 'width': '150px','minWidth': '150px','maxWidth': '150px'
                                        },
                                        style_header={
                                            'backgroundColor': 'rgb(230, 230, 230)',
                                            'fontWeight': 'bold'
                                        },
                                        style_cell_conditional=[
                                            {'if': {'column_id': 'displayName'},
                                            'width': '180px'},
                                            {'if': {'column_id': 'category'},
                                            'width': '100px'}
                                        ],
                                        style_data_conditional=[
                                            {
                                                'if': {'row_index': 'odd'},
                                                'backgroundColor': 'rgb(248, 248, 248)'
                                            }
                                        ],
                                        export_format = 'csv',
                                        export_headers ='names',
                                        
                                  css=[{'selector': '.dash-cell div.dash-cell-value', 'rule': 'display: inline; white-space: inherit; overflow: inherit; text-overflow: inherit;'}],
                                    
                                    )   
    # def eventsForPolicyOwners(self):

    #     # dfCust = self.druidPython.getPulseCustomerData()
    #     dpas_cust = self.druidPython.getDPASCustomerData()
    #     dfEvent = self.events

    #     merged = pd.merge(dfCust[['id','contactDetailsemailvalue']],dpas_cust[dpas_cust.policies0.notnull()][['contactDetailsEMAILvalue']],left_on='contactDetailsemailvalue',right_on='contactDetailsEMAILvalue' )


    #     final = pd.merge(merged ,dfEvent,left_on='id',right_on='owner')

    #     return final

    def getUserScoresData(self,dfEvent):
            dfEvent['auditDetail.createTime'] = dfEvent['auditDetail.createTime'].astype(str).str[:10]
            # dfEvent = 
            txUser = dfEvent.groupby('owner').agg(ActiveDays=('auditDetail.createTime',pd.Series.nunique )).reset_index()
            txUser['activeRatio'] = txUser['ActiveDays']/txUser['ActiveDays'].max()
            
            events = ['pulse.babylon.healthAssessment.chat.activity.start','pulse.babylon.healthAssessment.chat.fullAssessment.start','pulse.babylon.healthAssessment.chat.mood.start','pulse.babylon.healthAssessment.chat.nutrition.start','pulse.babylon.healthAssessment.viewDigitalTwin','pulse.babylon.symptomChecker.chat.start','notification/viewed','notification/actioned','pulse.viewContent']
            txFreq = dfEvent[dfEvent.name.isin(events)].groupby(['owner']).agg(freqCount=('name','count')).reset_index()
            txUser = pd.merge(txUser,txFreq,on='owner')
            
            txUser['freqAvg'] = txUser['freqCount']/txUser['ActiveDays']
            txRefer = dfEvent[dfEvent.name == 'platform.AffinityGroup.joinReferralGroup'].groupby('owner').agg(referCount=('name','count')).reset_index()
            txUser = pd.merge(txUser,txRefer,on='owner')
            txUser['referAvg'] = txUser['referCount']/txUser['ActiveDays']
            
            txUser['overAllScore'] = txUser['freqAvg']+txUser['referAvg']
            
            txUser['zScore'] = np.abs(stats.zscore(txUser.overAllScore))
            txUser = txUser[txUser.zScore>3]   
            txUser['Score'] = (txUser['overAllScore']/txUser['overAllScore'].max())*10
            return txUser.drop(['overAllScore','zScore'],axis=1)       

## =========================================== Charts Functions =========================================================

    ##generic bar charts
    def getBarChart(self, x , y , names , xaxis_title, yaxis_title, title, orientation='v',stack_or_group = None, stacks=1,groups=1,annotations=None,dataLabel = True,offsetList=None,textformat=False):

        data = []
        text,texttemplate = None,None   
               

        for (xs,ys,index) in zip(x,y,range(len(x))):
            if(stack_or_group == 'group'):
                offset = index+1
            elif((stack_or_group == 'stack') or (stack_or_group == None) ):
                offset = 1
            elif(stack_or_group == 'both'):
                if offsetList is not None:
                    offset = offsetList[index]
                    
                else:
                    offset = int((index+1) / stacks) + ((index+1) % stacks)

            else :
                offset=1

            if dataLabel:
                text = ys if orientation == 'v' else xs
                if(textformat):
                    # texttemplate='%{text:.3s}'
                    texttemplate='%{text:.1f}'
                    
                else:
                    texttemplate='%{text}'

            data.append(go.Bar(name=names[index], x=xs, y=ys, 
                               orientation=orientation,
                               offsetgroup=offset, text =text, textposition='auto',texttemplate=texttemplate))
         
        fig = go.Figure(data=data)

        fig.layout.update(title=self.getGraphTitle(title),xaxis_title = xaxis_title, yaxis_title= yaxis_title,yaxis={'autorange': True, 'automargin': True},xaxis={'autorange': True, 'automargin': True},annotations=annotations)
        return fig

    
     ##generic line charts
    
    def getLineChart(self,x,y,names,xaxis_title,yaxis_title,title,hoverinfo='all',mode='lines+markers',legend_orientation="h"):

        fig = go.Figure()

        for (xs,ys,index) in zip(x,y,range(len(x))):
            fig.add_trace(go.Scatter(x=xs, y=ys,name =names[index],mode=mode))
        
        fig.layout.update(legend_orientation=legend_orientation,xaxis_title=xaxis_title,yaxis_title=yaxis_title,xaxis={'automargin': True},yaxis={'automargin': True},title=self.getGraphTitle(title))

        return fig

    ##generic funnel graph
    @Exception.figureException(utilLogger,data=False)
    def getFunnelChart(self,x,y,names,title,textinfo='value+percent initial',annotations=None):
        
        
        fig = go.Figure()
        
        for (xs,ys,index) in zip(x,y,range(len(x))):

            # text = list(xs)
            # texttemplate='%{text:.1f}'

            fig.add_trace(go.Funnel(
                name = names[index],
                y = list(ys) ,
                x = list(xs),
                textinfo = textinfo
                # texttemplate='%{x:.2s}'
                ))

            
        fig.update_layout(title = self.getGraphTitle(title),yaxis={'automargin': True},annotations=annotations) 
        # fig.update_layout(updatemenus=[dict(
        #     type="buttons",
        #     direction="left",
        #     buttons=list([dict(label= '<a href=csv_string download="rawdata.csv" target="_blank">Download Data</a>',
        #     method='restyle',args=[])]),
        #     pad={"r":10,"t":10},
        #     showactive=True,
        #     x=0.11,
        #     xanchor="left",
        #     y=1.1,
        #     yanchor="top"
        # )])
        return fig     

    @Exception.figureException(utilLogger)
    def getEventsLineGraph(self,categories,filter,title,chartType = 'line'):
        fig = go.Figure()
        if filter == 'hourly':
            res = self.eventsCountsHourly.copy()        
        else:
            res = self.getDataFramebyTime('eventDate',filter, self.eventsCounts.copy())
        ## exclude these, bcz extra category 'Total' , already in this counts, so to prevent to get double count
        # res = res[~res['category'].isin(['Conventional.email','Social.facebook','Social.google']) ]
        categories = self.getReverseMapNames(categories)
        res = res[(res['eventName'].isin(categories) )& (res['category'] == 'Total') ]
        
        # res['eventCount'] = res.apply(lambda x: x['uniqueDeviceCount'] if(x['eventName'] == self.reverseMapName['App Install']) else x['eventCount'],axis=1)
        
        # res = res.groupby(['eventDate','eventName']).agg({'eventCount':sum}).reset_index()
        # categories = self.getReverseMapNames(categories)
        # print(res)
        res_filtered = res[res['eventName'].isin(categories)]
        x,y,names= list(),list(),list()
        for cat in categories:
            x.append(res[res['eventName'] == cat]['eventDate'])
            y.append(res[res['eventName'] == cat]['eventCount'])
            names.append(self.mapName[cat])

        if chartType == 'line':
            fig = self.getLineChart(x,y,names,'Dates','Count',title,hoverinfo='all',mode='lines+markers',legend_orientation="v")
        else:
            fig = self.getBarChart(x,y,names,'Dates','Count',title,orientation='v',stack_or_group = 'group')
        
        
        return [fig,res_filtered]


    def get_trend_lines(self,events,width):
        
        event = events[0] #default first event
        if(event!='mau'):
            
            res = pd.DataFrame()
            category = None
            if(len(event.split('-')) == 2):
                event,category = event.split('-')
            event = self.getReverseMapNames(event)

            if(event == 'dau'):
                # res = self.staticData.copy()
                # res['DATE'] = pd.to_datetime(pd.Series(res[self.eventStartField]), format="%Y/%m/%d")
 
                # res['DATE'] = res['DATE'].apply(lambda x: x.strftime("%Y/%m/%d"))
                res= self.dauCount
                res.sort_values(by=['eventDate'],inplace=True)
                # res = res.groupby('DATE').agg({'owner': pd.Series.nunique}).reset_index()
                res['x'] = res['eventDate']
                res['y'] = res['uniqueOwnerCount']


            else:
                eventcount = self.eventsCounts.copy()
               
                if(len(events) > 1):
                    df = pd.DataFrame()
                    for evt in events:
                        
                        evt = self.getReverseMapNames(evt)
                        count = eventcount[eventcount['eventName'] == evt]
                        if(category is not None):
                            count  = count[count['category']==category]
                        df = df.append(count)
                        

                    eventcount = df.groupby('eventDate').agg({'eventCount':sum}).reset_index()
                    

                else:
                    eventcount = eventcount[eventcount['eventName'] == event]
                    if(category is not None):
                        eventcount  = eventcount[eventcount['category']==category]

               
                eventcount['displayDate'] = pd.to_datetime(eventcount['eventDate'],utc=True)
               
                eventcount['displayDate'] = eventcount['displayDate'].apply(lambda x:x.strftime("%m/%d"))
                eventcount.sort_values(by=['displayDate'],inplace=True)
                res['x'] = eventcount['displayDate']
                res['y'] = eventcount['eventCount']

       
        if(event!='mau'):
            fig=go.Figure()
            fig.add_trace(go.Scatter(x=res['x'].values, y=res['y'].values, name=event,mode='lines'))
            fig.update_layout(xaxis={'tickmode':'auto','nticks':3},yaxis={'tickmode':'auto','nticks':2},
            font=dict(size=15))
            # fig.update_layout(font=dict(size=15))
            fig.update_layout(
                xaxis=dict(
                    showline=True,
                    showgrid=False,
                    showticklabels=True,
                    linecolor='rgb(204, 204, 204)',
                    linewidth=2,
                    ticks='outside',
                    tickmode = 'auto',
                    nticks=3,
                    tickfont=dict(
                        # family='Arial',
                        size=10,
                        # color='rgb(82, 82, 82)',
                    ),
                ),
                yaxis=dict(
                    # showgrid=False,
                    # zeroline=False,
                    # showline=False,
                    # showticklabels=False,
                    showline=True,
                    showgrid=False,
                    showticklabels=True,
                    linecolor='rgb(204, 204, 204)',
                    linewidth=2,
                    ticks='outside',
                    tickmode = 'auto',
                    nticks=2,
                    tickfont=dict(
                        # family='Arial',
                        size=10,
                        # color='rgb(82, 82, 82)',
                    ),

                ),
                # autosize=False,
                margin=dict(
                    # autoexpand=False,
                   l=0, r=0, t=0, b=0, pad=0
                ),
                showlegend=False,
                plot_bgcolor='white'
            )

            return dcc.Graph(
                    #style={"width": width, "height": '70px'},
                    style={"height": '70px'},
                    config={
                        "responsive": True,
                        "staticPlot": False,
                        "editable": False,
                        "displayModeBar": False,
                    },
                    figure = fig )
                    

    @Exception.figureException(utilLogger)
    def get_stacked_area_chart(self,df,events,title,start=None,end=None,annot_text=''):

        if((start is not None) & (end is not None)):
            # df = self.getDataFrameByTime('event_date','',df,True,start,end)
            df = self.getDataFramebyTime('event_date','', df,True,start,end)
        df['event_date'] = df['event_date'].astype(str)
        # print("In stacked area chart",df['event_date'].unique())
        df['date'] = df['event_date'].apply(lambda x:pd.to_datetime(str(x), format='%Y%m%d'))
        df = df[df['event_name'].isin(events)]
       

        fig = go.Figure()

        
        for event in events:
            tmp = df[df['event_name'] == event]
           
            fig.add_trace(go.Scatter(
                    name = self.customNameMap[event],
                    x=tmp['date'], y=tmp['count'].values,
                    hoverinfo='x+y',
                    mode='lines',
                    stackgroup='one' # define stack group
                ))

        fig.layout.update(title=self.getGraphTitle(title),yaxis_title='Count',
        annotations= self.get_annotations(annot_text,0,-0.30)
        
        )
        return fig,df[['date','event_name','count']]

    ## Cohort Analysis

    def plotCohort(self,retention,duration='day'):
        df_ = retention.T
        cohort_values = []
        cohort_display_values = []
        cohort_labels = []

        for x in df_.columns.tolist():
            cohort_values.append(df_[x].tolist())
            cohort_display_values.append(df_[x][0:].fillna(0).apply(lambda x: x*100).tolist())
            cohort_labels.append(df_[x][0:].fillna('').apply(lambda x: str(round(x*100,2) )+'%' if x!='' else x).tolist())
            # cohort_display_values.append([df_[x]['New user']]+df_[x][1:].fillna(0).apply(lambda x: x*100).tolist())
            # cohort_labels.append([df_[x]['New user'].astype('str')] +df_[x][1:].fillna('').apply(lambda x: str(round(x*100,2) )+'%' if x!='' else x).tolist())

        
        colorscale=[[0, "rgb(250, 235, 195)"],
                [1./10000, "rgb(254,224,144)"],
                [1./1000, "rgb(253,174,97)"],
                [1./100, "rgb(244,109,67)"],
                [1./10, "rgb(215,48,39)"],
                [1., "rgb(165,0,38)"]]

        xtext = 'Day' if(duration=='day') else 'Week'
        ytext = 'Daily ' if(duration=='day') else 'Weekly '
        fig = ff.create_annotated_heatmap(
            z=cohort_display_values[::-1], 
            x=list(map(lambda x: xtext +' '+str(x) if x != 'New user' else x,df_.index.values)),
            y=df_.columns.values.tolist()[::-1],
            annotation_text=cohort_labels[::-1],
            colorscale=colorscale,
            hovertemplate =
            'Cohort: %{y}<br>'+
            xtext+'s after registration: %{x}<br>'+
            'Retention percentage: %{z}%<br>' +
            "<extra></extra>"
        )
        
        fig.layout.yaxis.type = 'category'
        fig.layout.update(height = 500,
            xaxis_title= "Number of "+ xtext +"s after registration",
            yaxis_title="Cohorts - "+ytext+"registration",
            yaxis={'automargin': True}
        )
        
        return fig



    ###week/day wise cohort
    @Exception.figureException(utilLogger,data=False)
    def getCohort(self, df, duration ='day',period=None,start=None,end=None):
        if(period is None):
            df1 = self.getDataFramebyTime(self.eventStartField,'', df.copy(),True,start,end)
        else:
            df1 = self.getDataFramebyTime(self.eventStartField,period, df.copy())
            
        df1 = df1.dropna(subset=['owner'])
        df1[self.eventStartField] = pd.to_datetime(df1[self.eventStartField],utc= True)
        df1['startDay'] = df1[self.eventStartField].apply(lambda x: dt.datetime(x.year, x.month, x.day))
        # print(df1)
        
        if(duration=='week'):
            minDate = df1[self.eventStartField].min()
            print(minDate)
            weekstart = minDate - timedelta(days=minDate.weekday())
            weekstart = date(weekstart.year,weekstart.month,weekstart.day)

            df1['startweek'] = df1['startDay'].apply(lambda x : int(((date(x.year,x.month,x.day)-weekstart).days)/7))
            df1['CohortDay'] = df1.groupby('owner')['startweek'].transform('min')
            df1['CohortIndex'] =df1['startweek']-df1['CohortDay'] 
       
        elif(duration=='day'):
            owners = df1[df[self.eventNameField] == self.getReverseMapNames('Registration Completed')][['owner','startDay']].drop_duplicates().rename(columns={'startDay':'CohortDay'})
            # print("owners",owners.columns)
            # df1['CohortDay'] = df1.groupby('owner')['startDay'].transform('min')
            # print("after cohort",df1.columns)
            df1 = pd.merge(df1,owners,on='owner')
            # print("after merge",df1)
            invoice_year, invoice_month, start_day = df1['startDay'].dt.year,df1['startDay'].dt.month,df1['startDay'].dt.day
            cohort_year, cohort_month, cohort_day= df1['CohortDay'].dt.year,df1['CohortDay'].dt.month,df1['CohortDay'].dt.day
            year_diff = invoice_year - cohort_year
            month_diff = invoice_month - cohort_month
            day_diff = start_day - cohort_day
            df1['CohortIndex'] = year_diff * 12 + month_diff*30 + day_diff
            df1 = df1[df1.CohortIndex >= 0]
        
        cohort_data = df1.groupby(['CohortDay', 'CohortIndex'])['owner'].apply(pd.Series.nunique).reset_index()
        cohort_count = cohort_data.pivot_table(index = 'CohortDay',
                                        columns = 'CohortIndex',
                                        values = 'owner')
        
        cohort_size = cohort_count.iloc[:,0]
        retention = cohort_count.divide(cohort_size, axis = 0)
        
        allUsers = cohort_count[0].values
        # retention.insert(0,'New user', allUsers)
        
        retention.index = list(map(lambda x: str(x)[:10],retention.index))
       
        if duration == 'day':
            retention = retention.drop([0],axis = 1)
        fig = self.plotCohort(retention,duration)
        
        return fig                     


## ============================================ HTML Layout functions ====================================================

    def getNavbar(self):
        return dbc.Navbar(
            [
                dbc.Row(
                    [
                        dbc.Col(html.Img(src=self.app.get_asset_url('pulse_image.jpg'), height="105px"),
                                width={"size": 5, "order": 1},),
                        dbc.Col([dbc.Row(
                            dbc.NavbarBrand("Pulse", className="ml-2"),
                        ),
                            dbc.Row(
                            dbc.NavbarBrand(
                                "Real-time Analytics Dashboard", className="ml-2")
                        )], width={"size": 4, "order": "last", "offset": 1}, align="right"),
                    ],
                ),
                # html.Hr()
            ],
            # color='#D3D3D3',
            light=True,

        )

    def get_section_headers(self,heading , desc = ''):

      return  html.Div(
                [   html.Br(),
                    dbc.Card(
                        dbc.CardHeader([html.H5(heading,style = {'display':'inline-block'}, className = "card-title"),
                        html.P(desc,style = {'display':'inline-block','padding-left':'60px'})]),
                        
                        className="mb-3",color = "light",outline = True
                    )
                ]
            )
    
    def style_graph(self,id,figure,margintop,overflow='hidden',height=None):
        TITLE_FONT= dict(
                    family = 'Helvetica',
                    # color = '#6f199c'
                    size = self.chartFontSize #font size 
            )

        df=None
        
        if(isinstance(figure,(tuple,list))):
           
            df = figure[1]
            figure = figure[0]     

        
        ### TO Write images ....
        # if(id!='user-journey-graph' and figure is not None):
        #     figure['layout']['font'].update(**TITLE_FONT)
        #     if not self.config.inProduction:
        #         figure.write_image("plots/"+str(id)+".png", width=1000, height=500, scale=2) 
            
        if(df is not None):
            csv_string = df.to_csv(index=False, encoding='utf-8')
            csv_string = "data:text/csv;charset=utf-8," + urllib.parse.quote(csv_string)

            height = height if height else 450

            return html.Div(
                        [
                            html.A(
                                html.Img(src=self.app.get_asset_url('download_icon.png'), className='download-icon',style={'height':'16px','width':'16px'}),
                                # html.Button('Download data', className='three columns'),
                                href=csv_string,download= str(id)+".csv",target="_blank"),
                           
                            
                        dcc.Graph(id=id,figure=figure,style = {'margin-top': margintop,'overflowX':overflow,
                        'border':'thin lightgrey solid', 'height':height
                            
                            },config={'toImageButtonOptions': {'width': None,'height': None}})
                        ],    
                    )
        else:
            height = height if height else 550
            return html.Div(
                        [
                           

                        dcc.Graph(id=id,figure=figure,style = {'margin-top': margintop,'overflowX':overflow,
                        'border':'thin lightgrey solid', 'height':height
                            
                            },config={'toImageButtonOptions': {'width': None,'height': None}})
                        ],    
                    )

    def getDateRangePicker(self,id,start=None,end=None,minDate=None,maxDate=None,label='Date Range'):
        if(start is None):
            start=self.get_start_end_dates()[0]
        if (end is None):
            # end=self.get_start_end_dates()[-1]
            end = self.today

        if((minDate is None) and (maxDate is None)):
            minDate=self.get_start_end_dates()[0]
            # maxDate=self.get_start_end_dates()[-1]
            maxDate = self.today
        
        return [html.Label(label, className='dateRangeLabel'),
                            dcc.DatePickerRange(
                                id=id,
                                min_date_allowed=minDate,
                                max_date_allowed=maxDate+ timedelta(days=1),
                                start_date = start,
                                end_date = end,
                                
                                
                              
                            )]