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


def get_cards(count, id, description):
        ## Give yes or no -- for trendlines
        # event_count = self.util.get_event_count(event, 'today')[0]
        # event_count = 0 if pd.isna(event_count) else event_count
        return dbc.Card(
            dbc.CardBody(
                [
                    # count
                    # dbc.Spinner(
                    #     [
                    html.H1(children=count,id=id, className="card-title", style={'display': 'inline-block'}),
                    # description
                    html.P(description, style={'display': 'inline-block', 'text-indent': '12px'},className="card-title")
                    # html.P(" ", style={'display':'inline-block','text-indent':'6px'}, className="card-title"),
                    # html.Hr(),
                    # html.Div(homeUtil.customTrendLine(df,x,y,country), id=id+'trendline',
                    #         style={'text-align': 'center', 'align': 'center'})
                    #     ],
                    # color="primary",
                    #     type="grow"
                    # ),
                ])
        )

initial_usage_cards_1 = dbc.CardDeck(
    [   
        get_cards(homeUtil.foRegCard('PH')[0],'first-open-count',"Total First Open"),
        get_cards(homeUtil.foRegCard('PH')[1],'reg-count',"Total Registration"),
        get_cards(homeUtil.babylonCard('PH')[0],'babylon-ha-count',"Total Health Assesment "),
        get_cards(homeUtil.babylonCard('PH')[1],'babylon-sc-count',"Total Symptom Checker"),
        # get_cards(homeUtil.firstOpenCard('P'),'first-open-count',"Total First Open Users"),
        # get_cards(homeUtil.firstOpenCard('PH'),'first-open-count',"Total First Open Users"),
        # get_cards([decrpt_merge['ehr_MY.encounterId'].nunique()],'ehr_registered-user-count',"Total Encounters"),
        # EHRUtil.get_cards(['Full Assessment Completed-Total'],'ehr_healthAsses-user-count',"Full Assessment Completed"),
        
    ])


countryDropdown = html.Div([

                        html.Div([
                    #dropdown
                        dcc.Dropdown(
                            id='country-dropdown',
                            options=[{'label':value, 'value':value} for value in util.config.countryCode],
                            placeholder="Select Country",
                            value="PH",
                            # multi=True
                            # style={'width':'50%'}
                        )
                    ], style={'width':'10%','margin':'auto','margin-right':'10px'}
                    ),

])

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


layout = html.Div([
        # dbc.Badge(
        #     f"Date range - {EHRUtil.decrpt_merge['ehr_MY.auditDetail.createTime'].min()[:10]}  -  {EHRUtil.decrpt_merge['ehr_MY.auditDetail.createTime'].max()[:10]}", 
        #     color="light", className="mr-1",style={"float":"right"}
        #     ),
        # html.Br(),
        countryDropdown,
        # dropdwn,
        initial_usage_cards_1,
        # html.Hr(),
        # html.Br(),
        
        bigNoChart,
        # usageChart,
        # overlapChart,
        # ChurnChart
        
        
    ])

#=============================================== callbacks ====================================================

@app.callback([Output('first-open-count', 'children'), Output('reg-count', 'children'),  Output('babylon-ha-count', 'children'),  Output('babylon-sc-count', 'children')], 
              [Input('country-dropdown','value')
              ])
def update_cards(value):
    
    print(">>CALLBACK:----realTime event-line Callback-------")
    print(value)
    # print(dash.callback_context.triggered)
    # util.doUpdateQueryResults()
    if value is not None:
        return [homeUtil.foRegCard(value)[0], homeUtil.foRegCard(value)[1], homeUtil.babylonCard(value)[0],  homeUtil.babylonCard(value)[1]]
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


