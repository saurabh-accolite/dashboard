countryCode = ['PH', 'VN', 'TH', "MY",'ID']
dataSource = 'static'
# dataSource = 'druid'

##don't add '/' in the end
druid_url = 'http://localhost:8888/'
druid_host = 'localhost'
druid_port = '8888'
druid_username = None
druid_password = None

druid_events = 'PH_data' 
druid_productCode = 'polSal'
druid_customer = 'PH_PlseCustomer'
druid_dpasPolicy = 'PH_PolicyDPAS'
druid_dpasBen = 'PH_BeneficiaryDPAS'
druid_dpasCust = 'PH_CustomerDPAS'

##Couchbase
cb_url = 'couchbase://127.0.0.1'
cb_username = 'Administrator'
cb_password = 'saurabh'
cb_buckets = 'data_wiz'
##VN,PH,TH,MY
# in seconds(minuts*60)
refreshInterval = 300

##For How many days the charts should render by default
##For Two weeks -> Change it to 14 days
defaultDuration = 114 ##(In Days) 


whatsNewToast = ['PolicySales- counts(unique owner) based on Singapore(UTC+8) tz',
                     'Anomaly detection(Beta)',
                     'Customer segmentation ']

eventsFilter = {'PH': ['id', 'owner', 'name', 'startDate', 'completionDate','attributes.id','attributes.title','attributes.joinedBy','userAgent.deviceId',
                       'auditDetail.createTime', 'attributes._user', 'attributes.success', 'attributes.screen', 'attributes.failureCode', 'tags','type','userAgent.os']
                }

customerFilter = ['dob','sex','id','contactDetails.email.value','auditDetail.createTime','firstName','surName']

featureFilter = {"appDownload":['PH'],'firebaseLoginReg':['PH'], "contentMap":['PH'],"referfriend":['PH'],'loginFailure':['PH'],'pruShoppe':['PH'],'firebaseBrand':['PH'],
                "appInstall":['PH'],"successRegUsers":['PH'],"notification":['PH'],'tagWiseRegistration': ['PH'],'trafficSource':['PH'],'firebaseOsApp':['PH'],
                'firebaseAppRemove':['PH'],"appRemove":['PH']}

babylonEvents = {"PH":['Health Assessment(Activity) Start', 'Health Assessment(Full) Start', 'Health Assessment(Mood) Start', 'Health Assessment(Nutrition) Start', 'Health Assessment(Activity) End', 'Health Assessment(Full) End', 'Babylon Mood Assessment End', 'Babylon Nutrition Assessment End', 'Babylon View Digital Twin', 'Babylon Registration Completed', 'Babylon Registration Initiated', 'Symptom Checker Result', 'Symptom Checker Start', 'Symptom Checker End', 'Symptom Checker Chat History', 'Symptom Checker Key Search']}

regEvents = {"PH":['Initiate Registration', 'Registration - Email OTP','Registration Completed']}

dpasSwitch = False     ##Set to True or False
firebaseSwitch = True ##Set to True or False

# For correcting the time
timeOperator = '+'
timeHourDelta = 0
timeMinuteDelta = 0

## Adding EventName by content
idToEventMap = {'ac0a7d1d8b374e96beefc70c2ee198e7':'Refer a Friend',
              '96f04db4-b0e4-4b74-8b3a-c57b7ff87e5d':'My Policy',
              '0796c3de-563d-45c5-abcb-c88a98932be8':'Click on Buy COVID19 Cover',
              'c72ae8113fa54b33b6a051dea25a51ac':'PRUShoppe',
              '29d0afcd-f136-445c-ad17-36b869def7f0':'Click on Buy COVID19 Cover'}


sorter = ['Registered on Pulse', 'Logged into Pulse','Babylon Registration Confirmed','Full Assessment Completed',
          'Clicked on iProtect Tile','Product Details Screen Loaded','Clicked on View more details', 'Clicked on Show more packages', 
          'Completed Product Customization', 'OTP request', 'Proposal Submitted Successfully', 'Proposal Submitted', 'Payment Initiated', 
          'Payment Success', 'Policy Confirmation']


pathToSaveDataObject = '/home/koolmukul/workspace/pulseAnalysis/PulseDashboard/dataObjects/'
# pathToSaveDataObject = 'dataObjects/'
hardReload = False
inProduction = False
