from pydruid import *
from pydruid.client import *
from pydruid.utils.aggregators import *
from pydruid.utils.filters import *
from pydruid.db import connect
import pandas as pd
import datetime as dt
from util.utilDecorator import timeit

class DruidPython:

    def __init__(self,druid_url,druid_host,druid_port,druid_username,druid_password, druid_events,druid_customer,dpasPolicy,dpasBen,dpasCust,connect_param = 'druid/v2'):
        self.druid_url = druid_url
        self.druid_host = druid_host
        self.druid_port = druid_port
        self.druid_username = druid_username
        self.druid_password = druid_password
        self.datasource = druid_events
        self.druid_customer = druid_customer
        self.dpasPolicy = dpasPolicy
        self.dpasBen = dpasBen
        self.dpasCust = dpasCust
        self.start = '2020-10-14T16:00Z'
        self.end = '3000-05-04T00:00Z'
        self.granu_map = {'day':'P1D','hour':'PT1H'}

        self.queryObj  = PyDruid(self.druid_url, connect_param)
        if self.druid_username and self.druid_password:
            self.queryObj.set_basic_auth_credentials(self.druid_username,self.druid_password)
        
        self.testUsers = []

    def getEventsData(self):

        group = self.queryObj.groupby(
        datasource=self.datasource,
        granularity='all',
        intervals='2000-01-01T00:00Z/3000-01-01T00:00Z',
        dimensions=[
            {
                "type": "extraction",
                "dimension": "__time",
                "outputName": "auditDetail.createTime",
                "extractionFn": {
                    "type": "timeFormat",
                    
                }
                
            },
            {
                "type": "extraction",
                "dimension": "startDate",
                "extractionFn": {
                    "type": "timeFormat",
                    
                }
                
            },
            {
                "type": "extraction",
                "dimension": "completionDate",
                "extractionFn": {
                    "type": "timeFormat",
                    
                }
                
            },
            '_user', 'deviceId', 'failureCode', 'id', 'name','owner', 'region', 'screen', 'success', 'tags0', 'tags1', 'tags2', 'type', 'type_'
        ],
        filter=Dimension("type_") == "Event" 
        
        )
# in ("pulse.firstOpen","platform.UserRegistration.createCustomer.InitialRegistration") )

        df_druid  = group.export_pandas()
        df_druid = df_druid.rename(columns = {'_user':'attributes._user',
                                            'failureCode':'attributes.failureCode',
                                            'screen':'attributes.screen',
                                            'success':'attributes.success',
                                            })
        df_druid['tags'] = df_druid['tags2'].apply(lambda x: [x] if x is not None else [])
        df_druid['attributes.success'] = df_druid['attributes.success'].astype('bool')
        
        return df_druid

    @timeit('getTest users list ')
    def filterTestUsers(self,start  ,end=None):
        if start is None:
            start = '2020-08-01T00:00Z'
        if end is None:
            end = self.end
        print('Start ->'+ str(start))
        conn = connect(host=self.druid_host, port=self.druid_port,user=self.druid_username,password=self.druid_password, path='/druid/v2/sql/', scheme='http')
        curs = conn.cursor()
        
        curs.execute("""
            SELECT DISTINCT(id)
            FROM {}
            WHERE "type_" = 'customer' 
            AND ("contactDetails.email.value" LIKE '%yopmail%' 
            OR "contactDetails.email.value" LIKE '%ymail%')
            AND "__time" >=\'{}\' AND "__time"<=\'{}\'
        """.format(self.datasource,str(start),str(end)))

        df = pd.DataFrame(curs)
        testUsers = [] if df.empty else df.id.to_list()
        print('testUser size -> '+ str(len(testUsers)))

        # if len(testUsers)>0:
        #     resArr = '('
        #     for user in testUsers:
        #         resArr+= "'"+user+"',"
        #     resArr = resArr[:-1] + ')'
        
        # self.testUsers = resArr
        
        conn.close()

        return testUsers


    def daywiseEventsCountsTagWise(self, start  ,end=None,granularity = 'day' ):

        if start is None:
            start = self.start
        if end is None:
            end = self.end

        self.testUsers = self.filterTestUsers(start,end)
        
        
        format = "yyyy-MM-dd"
        if granularity == 'hour':
            format =  "yyyy-MM-dd'T'HH"
        
        group = self.queryObj.groupby(
                    datasource=self.datasource,
                    granularity=granularity,
                    intervals=start+'/'+end,
                    dimensions=[
                    {
                        "type": "extraction",
                        "dimension": "__time",
                        "outputName": "eventDate",
                        "extractionFn": {
                            "type": "timeFormat",
                            "format" : format
                        }

                    },
                    "name","tags1","tags2"
                ],
                filter=( (Dimension("type_") == "Event" ) &
                        (~Filter(type='in', dimension='owner', values=self.testUsers)) 
                        # (Dimension("name") != "platform.UserRegistration.createCustomer.InitialRegistration") &
                        # (Dimension("name") != "platform.UserRegistration.createCustomer.TermsAcceptedState") 
                ),

                    aggregations=
                    { 
                        "eventCount":count("owner"),
                        "uniqueOwnerCount":cardinality("owner")
                        
                    }

                )
        df = group.export_pandas()
        df.uniqueOwnerCount = df.uniqueOwnerCount.round().astype(int)
        
        return df
    
    def daywiseEventsCountsProductWise(self, dataSource,start  ,end=None,granularity = 'day' ):

        if start is None:
            start = self.start
        if end is None:
            end = self.end

        # self.testUsers = self.filterTestUsers(start,end)
        
        
        format = '%Y-%m-%d'
        if granularity == 'hour':
            format =  '%Y-%m-%dT%H'
        
        group = self.queryObj.groupby(
                    datasource=dataSource,
                    granularity={"type": "period", "period": self.granu_map[granularity],"timeZone": "Asia/Singapore"},
                    intervals=start+'/'+end,
                    dimensions=[
                    # {
                    #     "type": "extraction",
                    #     "dimension": "__time",
                    #     "outputName": "eventDate",
                    #     "extractionFn": {
                    #         "type": "timeFormat",
                    #         "format" : format
                    #     }

                    # },
                    "name","productCode"
                ],
                filter=( (Dimension("type_") == "Event" ) &
                        (~Filter(type='in', dimension='owner', values=self.testUsers)) 
                        # (Dimension("name") != "platform.UserRegistration.createCustomer.InitialRegistration") &
                        # (Dimension("name") != "platform.UserRegistration.createCustomer.TermsAcceptedState") 
                ),

                    aggregations=
                    { 
                        "eventCount":count("owner"),
                        "uniqueOwnerCount":cardinality("owner")
                        
                    }

                )
        df = group.export_pandas()
        df.uniqueOwnerCount = df.uniqueOwnerCount.round().astype(int)
        # print(df)
        # df.to_csv(f'./prodeuctWise{granularity}.csv')
        df['eventDate'] = pd.to_datetime(df['timestamp']).dt.strftime(format)
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        return df
    
    @timeit('getEventsCounts ')
    def daywiseEventsCountsTotal(self, start  ,end=None,granularity = 'day' ):

        # if granularity == 'day':
        #     testUsers_loc = self.filterTestUsers(start,end)
        #     self.testUsers = list(set(self.testUsers + testUsers_loc))

        if start is None:
            start = self.start
        if end is None:
            end = self.end


        format = '%Y-%m-%d'
        if granularity == 'hour':
            format =  '%Y-%m-%dT%H'
        
        group = self.queryObj.groupby(
                    datasource=self.datasource,
                    granularity={"type": "period", "period": self.granu_map[granularity],"timeZone": "Asia/Singapore"},
                    intervals=start+'/'+end,
                    dimensions=[
                    # {
                    #     "type": "extraction",
                    #     "dimension": "__time",
                    #     "outputName": "eventDate",
                    #     "extractionFn": {
                    #         "type": "timeFormat",
                    #         "format" : format
                    #     }

                    # },
                        "name"
                    
                ],
                filter=( 
                        (Dimension("type_") == "Event" ) &
                        (~Filter(type='in', dimension='owner', values=self.testUsers))
                        # (Dimension("name") != "platform.UserRegistration.createCustomer.InitialRegistration") &
                        # (Dimension("name") != "platform.UserRegistration.createCustomer.TermsAcceptedState") 
                ),

                    aggregations=
                    { 
                        "eventCount":count("owner"),
                        "uniqueOwnerCount":cardinality("owner"),
                        "uniqueDeviceCount":cardinality("userAgent.deviceId")
                    }

                )
        df = group.export_pandas()
        df.uniqueOwnerCount = df.uniqueOwnerCount.round().astype(int)
        df.uniqueDeviceCount = df.uniqueDeviceCount.round().astype(int)

        df['eventDate'] = pd.to_datetime(df['timestamp']).dt.strftime(format)
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        return df

    @timeit('Activeusers -  ')
    def DAUCounts(self, start  ,end=None ):
        if start is None:
            start = self.start
        if end is None:
            end = self.end
    
        group = self.queryObj.groupby(
                    datasource=self.datasource,
                    granularity={"type": "period", "period": self.granu_map['day'],"timeZone": "Asia/Singapore"},
                    intervals=start+'/'+end,
                #     dimensions=[
                #     {
                #         "type": "extraction",
                #         "dimension": "__time",
                #         "outputName": "eventDate",
                #         "extractionFn": {
                #             "type": "timeFormat",
                #             "format" : "yyyy-MM-dd"
                #         }

                #     },

                # ],
                filter=(    (Dimension("type_") == "Event")&
                            (Filter(type='like', dimension='name', pattern = 'pulse%') |
                                Filter(type='in', dimension='name', values=["PulseUsage","platform.UserLogin.login" ])) &
                            (Dimension("name") != "pulse.notification.recieved")&
                            (~Filter(type='in', dimension='owner', values=self.testUsers)) 
                            
                ),
                aggregations=
                    { 

                        "uniqueOwnerCount":cardinality("owner")
                    }

                )
        df = group.export_pandas()
        df.uniqueOwnerCount = df.uniqueOwnerCount.round().astype(int)
        df['eventDate'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%d')
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%Y-%m-%dT%H:%M:%SZ')
        
        return df

    def MAUCount(self, start  ,end =None):
        if start is None:
            start = self.start
        if end is None:
            end = self.end
        
        group = self.queryObj.groupby(
                    datasource=self.datasource,
                    granularity='month',
                    intervals=start+'/'+end,
                    dimensions=[
                    {
                        "type": "extraction",
                        "dimension": "__time",
                        "outputName": "eventDate",
                        "extractionFn": {
                            "type": "timeFormat",
                            "format" : "yyyy-MM"
                        }

                    },
                    
                ],
                filter=Dimension("type_") == "Event" ,
                aggregations=
                    { 
                        "uniqueOwnerCount":cardinality("owner")
                    
                    }

                )
        df = group.export_pandas()
        df.uniqueOwnerCount = df.uniqueOwnerCount.round().astype(int)
        
        return df
    
    def RegistrationLoginOcrDistinctOwner(self, start  ,end = None ):
        
        if start is None:
            start = self.start
        if end is None:
            end = self.end

        group = self.queryObj.groupby(
                datasource=self.datasource,
                granularity='day',
                intervals=start+'/'+end,
                dimensions=[
                {
                    "type": "extraction",
                    "dimension": "__time",
                    "outputName": "eventDate",
                    "extractionFn": {
                        "type": "timeFormat",
                        "format" : "yyyy-MM-dd"
                    }

                },
                {
                        "dimension": "name",
                        "outputName": "eventName",
                  
                },
                    "attributes.success"

                
            ],
         filter=(
                (Dimension("type_") == "Event" ) &
                (Dimension("name") == "platform.UserRegistration.createCustomer.InitialRegistration") |
                (Dimension("name") == "platform.UserRegistration.createCustomer.TermsAcceptedState") |
                (Dimension("name") == "platform.UserRegistration.createCustomer.EmailOTPVerification") |
                (Dimension("name") == "platform.UserLogin.login") |
                (Dimension("name") == "platform.OcrJob.findOCRDocument") 
                
            ),
            aggregations=
                { 
                    "eventCount":count("owner"),
                    "uniqueOwnerCount":cardinality("owner"),
                    "uniqueDeviceCount":cardinality("userAgent.deviceId")
                  
                }

            )
        df = group.export_pandas()
        df.uniqueOwnerCount = df.uniqueOwnerCount.round().astype(int)
        df.uniqueDeviceCount = df.uniqueDeviceCount.round().astype(int)
        
        return df

    def AttributesIDDistinctOwner(self, start  ,end = None ):

        if start is None:
            start = self.start
        if end is None:
            end = self.end
        
        group = self.queryObj.groupby(
                    datasource=self.datasource,
                    granularity='day',
                    intervals=start+'/'+end,
                    dimensions=[
                    {
                        "type": "extraction",
                        "dimension": "__time",
                        "outputName": "eventDate",
                        "extractionFn": {
                            "type": "timeFormat",
                            "format" : "yyyy-MM-dd"
                        }

                    },
                    {
                        "dimension": "name",
                        "outputName": "eventName",
                  
                    },
                        "attributes.id","attributes.title"

                    
                ],
            filter=(
                    (Dimension("type_") == "Event" ) &
                    (Dimension("name") == "pulse.viewContent")
                    
                    
                ),
                aggregations=
                    { 
                        "eventCount":count("owner"),
                        "uniqueOwnerCount":cardinality("owner")
                    
                    }

                )
        df = group.export_pandas()
        df.uniqueOwnerCount = df.uniqueOwnerCount.round().astype(int)
        
        return df

    def DpasPolicyIdCount(self, start  ,end = None):

        if start is None:
            start = self.start
        if end is None:
            end = self.end
        
        group = self.queryObj.groupby(
                    datasource=self.dpasPolicy, #change to dpas policy 
                    granularity='day',
                    intervals=start+'/'+end,
                    dimensions=[
                    {
                        "type": "extraction",
                        "dimension": "__time",
                        "outputName": "eventDate",
                        "extractionFn": {
                            "type": "timeFormat",
                            "format" : "yyyy-MM-dd"
                        }

                    },
                        

                    
                ],
            
                aggregations=
                    { 
                        "policyIdCount":count("id")
                    
                    }

                )
        df = group.export_pandas()
        # print("DpasPolicyIdCount",df)
    
        return df

    def DpasBeneficiaryRelationTypeCount(self, start  ,end=None ):
        if start is None:
            start = self.start
        if end is None:
            end = self.end
    
        group = self.queryObj.groupby(
                    datasource=self.dpasBen, 
                    granularity='day',
                    intervals=start+'/'+end,
                    dimensions=[
                    {
                        "type": "extraction",
                        "dimension": "__time",
                        "outputName": "eventDate",
                        "extractionFn": {
                            "type": "timeFormat",
                            "format" : "yyyy-MM-dd"
                        }

                    },
                        "relationshipType"

                    
                ],
            
                aggregations=
                    { 
                        "BeneficiaryRelationTypeCount":count("relationshipType"),
                    
                    }

                )
        df = group.export_pandas()
        # print("DpasBeneficiaryRelationTypeCount",df)
        return df

    @timeit('Getting Events data ...... -  ')
    def getPulseEventsData(self,eventFilter,dataSource = None,lastMonth = 1):
        if dataSource is None:
            dataSource = self.datasource
        conn = connect(host=self.druid_host, port=self.druid_port,user=self.druid_username,password=self.druid_password, path='/druid/v2/sql/', scheme='http')
        curs = conn.cursor()
        # WHERE "__time" >= CURRENT_TIMESTAMP - INTERVAL '27' DAY
        curs.execute("""
            SELECT __time AS createTime, owner, name 
            FROM {}
            where type_ = 'Event'
            AND (name LIKE 'pulse%'
            OR name in ('platform.ProductJourneys.getOcrData',
            'platform.ProductJourneys.getPolicyNumberSuccess',
            'platform.ProductJourneys.submitProposal',
            'platform.UserLogin.login',
            'platform.UserRegistration.createCustomer.TermsAcceptedState',
            'platform.Wearables.Connection.Success',
            'platform.WellnessGoals.joinCustomerActionPlan',
            'platform.WellnessGoals.unlockHabit',
            'platform.WellnessGoals.updateHabitMilestone',
            'platform.cep.match.ReferFriendJoinedRulePHPROD',
            'platform.cep.match.ReviveCovidPolicy',
            'platform.subscriptionJourney.createSubscriptionSuccess')
            )
            AND "__time" >= CURRENT_TIMESTAMP - INTERVAL \'{}\' MONTH
            
            
        """.format(dataSource,lastMonth))    ## 27 days from Today - Indicating 4 weeks of data
        df = pd.DataFrame(curs)
        conn.close()

        return df

    def getPulseCustomerData(self):
        conn = connect(host=self.druid_host, port=self.druid_port,user=self.druid_username,password=self.druid_password, path='/druid/v2/sql/', scheme='http')
        curs = conn.cursor()

        curs.execute("""
            SELECT __time AS CustDate, id, sex,dob,firstName, surName,"contactDetails.email.value" as contactDetailsemailvalue
            FROM {}
        """.format(self.druid_customer))
        
        df_cust = pd.DataFrame(curs)
        conn.close()
        return df_cust


    def BenCount(self):

        conn = connect(host=self.druid_host, port=self.druid_port,user=self.druid_username,password=self.druid_password, path='/druid/v2/sql/', scheme='http')
        curs = conn.cursor()
        
        curs.execute("""
            SELECT  COUNT(*) - COUNT(beneficiary0) AS "nullBen", COUNT(beneficiary0) AS "notNullBen",
            TIME_FORMAT("__time", 'y-M-d') AS "eventDate"

            from {} 

            GROUP BY  TIME_FORMAT("__time", 'y-M-d')
        """.format(self.dpasPolicy))    
        df = pd.DataFrame(curs)
        conn.close()

        return df

    def ReferralGroupSize(self, start  ,end = None ):
        if start is None:
            start = self.start
        if end is None:
            end = self.end
    
        group = self.queryObj.groupby(
                    datasource= self.datasource,
                    granularity='day',
                    intervals=start+'/'+end,
                    dimensions=[
                    {
                        "type": "extraction",
                        "dimension": "__time",
                        "outputName": "eventDate",
                        "extractionFn": {
                            "type": "timeFormat",
                            "format" : "yyyy-MM-dd"
                        }

                    },
                        "owner"

                    
                ],
                filter=Dimension("type_") == "Event" ,
            
                aggregations=
                    { 
                        "referralCount":count("attributes.joinedBy"),
                        
                    }

                )
        df = group.export_pandas()
        
        return df

    def getDPASCustomerData(self):
        conn = connect(host=self.druid_host, port=self.druid_port,user=self.druid_username,password=self.druid_password, path='/druid/v2/sql/', scheme='http')
        curs = conn.cursor()

        curs.execute("""
            SELECT __time AS CustDate, id, sex,dob,"externalIds.customer_master" as customer_master,
            "contactDetails.EMAIL.value" as contactDetailsEMAILvalue , policies0
            FROM {}
        """.format(self.dpasCust))
        
        df_cust = pd.DataFrame(curs)
        conn.close()
        
        return df_cust

    def getDPASPolicyData(self):
        conn = connect(host=self.druid_host, port=self.druid_port,user=self.druid_username,password=self.druid_password, path='/druid/v2/sql/', scheme='http')
        curs = conn.cursor()

        curs.execute("""
            SELECT __time AS PolicyDate, clients0
            FROM {}
        """.format(self.dpasPolicy))
        
        df_pol = pd.DataFrame(curs)
        conn.close()
        return df_pol

        

    def UniqueJoinedBy(self):

        conn = connect(host=self.druid_host, port=self.druid_port,user=self.druid_username,password=self.druid_password, path='/druid/v2/sql/', scheme='http')
        curs = conn.cursor()
        
        curs.execute("""
            SELECT DISTINCT "attributes.joinedBy" as joinedBy FROM {}
        WHERE name = 'platform.AffinityGroup.joinReferralGroup'
        """.format(self.datasource))    
        df = pd.DataFrame(curs)
        conn.close()
        # print(df)
        return df
    
    def getRegEvents(self,start  ,end = None,granularity = 'day'):
        
        if start is None:
            start = self.start
        if end is None:
            end = self.end
        
        format = 'y-MM-dd'
        if granularity == 'hour':
            format =  'y-MM-dd:HH'

        conn = connect(host=self.druid_host, port=self.druid_port,user=self.druid_username,password=self.druid_password, path='/druid/v2/sql/', scheme='http')
        curs = conn.cursor()
        
        curs.execute("""
            SELECT  TIME_FORMAT("__time", \'{}\') AS "eventDate", name AS "eventName" 
            , COUNT(owner) AS "eventCount"
            , COUNT(DISTINCT "owner") AS "uniqueOwnerCount" 
            , COUNT(DISTINCT "userAgent.deviceId") AS "uniqueDeviceCount"
            FROM {}
            WHERE "type_" = 'Event' 
            AND "name" IN ('platform.UserRegistration.createCustomer.InitialRegistration','platform.UserRegistration.createCustomer.TermsAcceptedState') 
            AND "attributes.success" = 'true'
            AND "attributes._user" NOT LIKE '%yopmail%' 
            AND "attributes._user" NOT LIKE '%ymail%'
            AND "__time" >=\'{}\' AND "__time"<=\'{}\'
            GROUP BY TIME_FORMAT("__time", \'{}\'), name
        """.format(format,self.datasource,str(start),str(end),format))    
        total = pd.DataFrame(curs)
        total['category'] = 'Total'
        total['tags1'] = 'Total'
        
        

        curs.execute("""
            SELECT  TIME_FORMAT("__time", \'{}\') AS "eventDate", name AS "eventName" ,tags2
            , COUNT(owner) AS "eventCount"
            , COUNT(DISTINCT "owner") AS "uniqueOwnerCount" 
            , COUNT(DISTINCT "userAgent.deviceId") AS "uniqueDeviceCount"
            FROM {}
            WHERE "type_" = 'Event' 
            AND "name" IN ('platform.UserRegistration.createCustomer.InitialRegistration','platform.UserRegistration.createCustomer.TermsAcceptedState') 
            AND "attributes.success" = 'true'
            AND "attributes._user" NOT LIKE '%yopmail%' 
            AND "attributes._user" NOT LIKE '%ymail%'
            AND "__time" >=\'{}\' AND "__time"<=\'{}\'
            GROUP BY TIME_FORMAT("__time", \'{}\'), name, tags2
        """.format(format,self.datasource,str(start),str(end),format))
        tagWise = pd.DataFrame(curs)
        tagWise['tags1'] = tagWise['tags2']
        tagWise['category'] = tagWise['tags2']
        

        total = total.append([tagWise], ignore_index = True)
        if granularity == 'day':
            total['eventDate'] = pd.to_datetime(total['eventDate']).dt.date.astype(str)
            total['timestamp'] = total['eventDate'].apply(lambda x:x+'T00:00:00.000Z')
        else:
            total['eventDate'] = total['eventDate'].str.replace(':','T')
            total['timestamp'] = total['eventDate'].apply(lambda x:x+':00:00.000Z')
        total['displayName'] = total['eventName']
        
        conn.close()
        
        return total
        
    
    def FailureCauseCount(self, start  ,end = None ):
        if start is None:
            start = self.start
        if end is None:
            end = self.end

        group = self.queryObj.groupby(
                    datasource = self.datasource,
                    granularity='day',
                    intervals=start+'/'+end,
                    dimensions=[
                    {
                        "type": "extraction",
                        "dimension": "__time",
                        "outputName": "eventDate",
                        "extractionFn": {
                            "type": "timeFormat",
                            "format" : "yyyy-MM-dd"
                        }

                    },
                        "attributes.failureCode"

                    
                ],
                filter=(
                    (Dimension("type_") == "Event" ) &
                (Dimension("attributes.success") == "false")
                
                
                ),
            
                aggregations=
                    { 
                        "userCount":count("attributes._user"),
                        
                    }

                )
        df = group.export_pandas()
        
        return df
    

    ##Funnel
    @timeit('Funnel Query -  ')
    def funnelCounts(self,eventList,startDate,endDate):

        startDate = (pd.to_datetime(startDate) -  dt.timedelta(hours= 8 )).strftime('%Y-%m-%dT%H:%M:%SZ')
        ## add 24 hours to include current day, and sybtract 8 to get singapore time
        endDate = (pd.to_datetime(endDate) -  dt.timedelta(hours= 8-24 )).strftime('%Y-%m-%dT%H:%M:%SZ')
        print(startDate,endDate)

        group = self.queryObj.groupby(
                    datasource=self.datasource,
                    granularity='all',
                    intervals=str(startDate)+'/'+endDate,
                    dimensions=[
                    'owner','name'

                ],
                filter=(
                    (Dimension("type_") == "Event")&
                    Filter(type='in', dimension='name', values=eventList) 
                    # (~Filter(type='in', dimension='owner', values=self.testUsers))
                    
                )

                )
        dft = group.export_pandas()
        testu = self.testUsers
        dft = dft.query('owner not in @testu')
        return dft
    


    def getEventCountsBypattern(self,pattern,start  ,end=None,granularity = 'day' ):
        if start is None:
            start = self.start
        if end is None:
            end = self.end
        
        format = "yyyy-MM-dd"
        if granularity == 'hour':
            format =  "yyyy-MM-dd'T'HH"
    
        group = self.queryObj.groupby(
                    datasource=self.datasource,
                    granularity=granularity,
                    intervals=start+'/'+end,
                    dimensions=[
                    {
                        "type": "extraction",
                        "dimension": "__time",
                        "outputName": "eventDate",
                        "extractionFn": {
                            "type": "timeFormat",
                            "format" : format
                        }

                    },

                ],
                filter=(
                            Filter(type='like', dimension='name', pattern = pattern) &
                            (~Filter(type='in', dimension='owner', values=self.testUsers)) &
                            (Dimension("type_") == "Event")
                        ),
                aggregations=
                    { 

                        "uniqueOwnerCount":cardinality("owner")
                    }

                )
        df = group.export_pandas()
        df.uniqueOwnerCount = df.uniqueOwnerCount.round().astype(int)
        
        return df