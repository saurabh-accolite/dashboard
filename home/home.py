import dash_bootstrap_components as dbc
import dash_html_components as html
from datetime import timedelta,datetime  

# import dash
from dash.dependencies import Input, Output,State
import dash_core_components as dcc
from home.homeUtil import HomeUtil
import dash 
import pandas as pd

homeUtil = HomeUtil()
util = homeUtil.util
app = util.app

import util.appLogger as appLogger

logger = appLogger.get_logger(__name__)
print = logger.debug

startDate = pd.to_datetime('2020-12-01')
endDate = pd.to_datetime('2020-12-31')
minDate = pd.to_datetime('2020-12-01')
maxDate = pd.to_datetime('2020-12-31')

def get_cards(count, id, description):
        return dbc.Card(
            dbc.CardBody(
                [
                    html.H1(children=count,
                            id=id, className="card-title", style={'display': 'inline-block'}),
                    html.P(description, style={
                        'display': 'inline-block', 'text-indent': '12px'},className="card-title"),
                ]
            )
        )

initial_usage_cards_1 = dbc.CardDeck(
    [   
        get_cards(homeUtil.foRegCard('PH', '2020-12-01', '2020-12-31')[0],'first-open-count',"Total First Open"),
        get_cards(homeUtil.foRegCard('PH', '2020-12-01', '2020-12-31')[1],'reg-count',"Total Registration"),
        get_cards(homeUtil.babylonCard('PH', '2020-12-01', '2020-12-31')[0],'babylon-ha-count',"Total Health Assesment "),
        get_cards(homeUtil.babylonCard('PH', '2020-12-01', '2020-12-31')[1],'babylon-sc-count',"Total Symptom Checker"), 
    ])


# countryDropdown = html.Div([

#                         html.Div([
#                         dcc.Dropdown(
#                             id='country-dropdown',
#                             options=[{'label':value, 'value':value} for value in util.config.countryCode],
#                             placeholder="Select Country",
#                             value="PH",
#                         )
#                     ], style={'width':'10%','margin':'auto','margin-right':'0px'}),

#                     html.Div([
#                         html.Div(
                            
#                                     util.getDateRangePicker('date-picker-range'),
#                                     style={'text-align':'left'}),
#                     ]) 

# initial_usage_cards = dbc.CardDeck(
#     [
#         homeUtil.get_cards(['Registration Completed-Total'],'registered-user-count',"Users on Pulse"),
#         homeUtil.get_cards(['Health Assessment(Full) Start'],'health-assessment-count',"Availed Full Health Assessment"),
#         homeUtil.get_cards(['Symptom Checker Start'],'symptom-checker-count',"Availed Symptom Checker")
#     ])

bigNoChart = html.Div([
    util.get_section_headers("Feature Trendlines"),

    dbc.Row(
        [
            dbc.Col(
                [
                    dbc.Row([
                        dbc.Col(
                            html.Div([
                            #dropdown
                            dcc.Dropdown(
                            id='feature-dropdown',
                            options=[{'label':value, 'value':key} for key,value in homeUtil.featureDict.items()],
                            placeholder="Select Feature",
                            # value="PH",
                            # multi=True
                            # style={'width':'50%'}
                        )
                    ], style={'width':'40%','margin':'auto','margin-left':'0px'}
                    ),
                        )
                    ]

                    ),
                    html.Br(),
                    dbc.Row(
                        [
                            dbc.Col(
                                html.Div(
                                     id='big-no-trendline-div',
                                    style={'margin': 'auto'}

                                )
                            ),
                        ]
                    ),
                ]
            )
        ]
    )
]
)


dropdownAndDatePicker = html.Div([
    
    # html.Div([
                html.Div(
                    util.getDateRangePicker('date-picker-range', startDate, endDate, minDate, maxDate),
                    style={'text-align':'left', 'display': 'inline-block','margin':'auto','margin-left':'500px','margin-top':'13px'}
                ),
                # 'margin-left':'680px'
                html.Div([
                    dcc.Dropdown(
                        id='country-dropdown',
                        options=[{'label':value, 'value':value} for value in util.config.countryCode],
                        placeholder="Select Country",
                        value="PH",
                    )
                ], style={'display': 'inline-block','width':'25%','margin':'auto','float':'right'}), 
        # ]),
])

layout = html.Div([
    html.Hr(),
    dropdownAndDatePicker,
    html.Br(),
    initial_usage_cards_1,
    bigNoChart
])

#=============================================== callbacks ====================================================

@app.callback([Output('first-open-count', 'children'), Output('reg-count', 'children'),  
               Output('babylon-ha-count', 'children'),  Output('babylon-sc-count', 'children')], 
              [Input('country-dropdown','value'), Input('date-picker-range','start_date'),
            Input('date-picker-range','end_date')])

def update_cards(value, startDate, endDate):
    print(">>CALLBACK:----realTime event-line Callback-------")
    print(value)
    print(startDate)
    print(endDate)
    if value is not None:
        return [homeUtil.foRegCard(value, startDate, endDate)[0], homeUtil.foRegCard(value, startDate, endDate)[1], homeUtil.babylonCard(value, startDate, endDate)[0],  homeUtil.babylonCard(value, startDate, endDate)[1]]
    return [None,None,None,None]

@app.callback([Output('big-no-trendline-div','children')],
            [Input('feature-dropdown','value'),Input('country-dropdown','value')])
def get_trend_lines(feature,country):
    if feature is not None and country is not None:
        if(feature == 'fo'):
            # print(homeUtil.regDf['date'])
            df = homeUtil.regDf[homeUtil.regDf['countryCode'] == country]
            df.sort_values(by=['date'],inplace=True)
            x = df['date']
            y = df['FirstOpen']
            title = 'First Open'
        elif (feature == 'reg'):
            df = homeUtil.regDf[homeUtil.regDf['countryCode'] == country]
            df.sort_values(by=['date'],inplace=True)
            x = df.date
            y = df.Registration
            title = 'Registration'
        elif(feature=='ha'):
            df = homeUtil.babylonDf[homeUtil.regDf['countryCode'] == country]
            df.sort_values(by=['date'],inplace=True)
            x = df.date
            y = df.ha
            title = 'Health Assessment'
        elif(feature=='sc'):
            df = homeUtil.babylonDf[homeUtil.regDf['countryCode'] == country]
            df.sort_values(by=['date'],inplace=True)
            x = df.date
            y = df.sc
            title = 'Symptom Checker'

        return util.style_graph(id='big-no-trendline', figure=util.getLineChart([x],[y],[title],'Date','Count',title,hoverinfo='all',mode='lines+markers',legend_orientation="h"),
                                                     margintop='0.1mm', height=350),
    return [None]


