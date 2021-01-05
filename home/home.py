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
                        'display': 'inline-block', 'text-indent': '12px'}),
                ]
            )
        )

initial_usage_cards_1 = dbc.CardDeck(
    [   
        get_cards(homeUtil.firstOpenCard('PH', '2020-12-01', '2020-12-31'),'first-open-count',"Total First Open Users"),
        get_cards(homeUtil.regCard('PH', '2020-12-01', '2020-12-31'),'reg-count',"Total Registration"),
        get_cards(homeUtil.babylonCard('PH', '2020-12-01', '2020-12-31')[0],'babylon-ha-count',"Total Babylon Health Assesment "),
        get_cards(homeUtil.babylonCard('PH', '2020-12-01', '2020-12-31')[1],'babylon-sc-count',"Total Babylon Symptoms Checker"),        
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

#         ])


dropdownAndDatePicker = html.Div([
    
    # html.Div([
                html.Div(
                    util.getDateRangePicker('date-picker-range', startDate, endDate, minDate, maxDate),
                    style={'text-align':'left', 'display': 'inline-block','margin':'auto','margin-left':'680px'}
                ),

                html.Div([
                    dcc.Dropdown(
                        id='country-dropdown',
                        options=[{'label':value, 'value':value} for value in util.config.countryCode],
                        placeholder="Select Country",
                        value="PH",
                    )
                ], style={'display': 'inline-block','width':'15%','margin':'auto','float':'right'}), 
        # ]),
])

layout = html.Div([
    dropdownAndDatePicker,
    html.Hr(),
    html.Br(),
    initial_usage_cards_1
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
        return [homeUtil.firstOpenCard(value, startDate, endDate), homeUtil.regCard(value, startDate, endDate), homeUtil.babylonCard(value, startDate, endDate)[0],  homeUtil.babylonCard(value, startDate, endDate)[1]]





