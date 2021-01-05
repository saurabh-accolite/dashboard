from pathlib import Path

import sys
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output,State
import dash_daq as daq
import dash_bootstrap_components as dbc
import dash_table
import pandas as pd
import datetime as dt
import time

# starting time
start = time.time()

#set path enviroment for local imports
print(str(Path().absolute()))
sys.path.append(str(Path().absolute()))

Path('./Logs').mkdir(parents=True, exist_ok=True)


## Local imports
from app import app
from util.util import Util
import config

## Instantiate singlton class Util object 
util = Util.getInstance(app,config)
## Local imports
from home import home
# from iprotect import iprotect
# from policySales import polSales
# from customer_seg import custSegment
# from registration import registration
# from anomaly import anomaly
# from heatMap import heatMap
# from EHR import EHR
# from EHR.PH import EHR as EHR_PH
# from pulseGold import pulseGold


import util.appLogger as appLogger

logger = appLogger.get_logger(__name__)
print = logger.debug

# from util.searchEngine import SearchEngine
# searchObj = SearchEngine(util.dataPath+'chartSearch.csv')


debug = False
searchResults = []


tabs = dbc.Tabs(
    [
        # dbc.Tab(label='Home', tab_id='home',
        # children=[
        #     home.layout
        #     ]
        #     ),
        # dbc.Tab(label='Policy Sales', tab_id='polSales',
        # children=[
        #    polSales.layout
        # ]
        # ),
        # dbc.Tab(label='EHR', tab_id='ehrTab',
        # children=[
        #    EHR.layout
        # ]
        # ),
        # dbc.Tab(label='Anomaly Detection', tab_id='anomaly-detection',
        # children=[
        #    anomaly.layout
        # ]
        # ),
        # dbc.Tab(label='Registration', tab_id='registration',
        # children=[
        #     registration.layout
        #  ]),
        

        # dbc.Tab(label='Heat Map', tab_id='heat-map',
        # children=[
        #     heatMap.layout
        # ]
        # ),
        # dbc.Tab(label='Pulse Health', tab_id='pulse-health',
        # children=[
        #     html.Div([pulseHealth.layout])
           
        # ]),
        # # if(config.dpasSwitch):
        # dbc.Tab(label='Sales', tab_id='sales',
        # children=[
        #     html.Div([sales.layout])
        
        # ]),
        # dbc.Tab(label='Customer', tab_id='custSeg',
        # children=[
        #     html.Div([custSegment.layout])
           
        # ]),
        dbc.Tab(label='Home', tab_id='home',
        children=[
            html.Div([home.layout])
           
        ]),
        
        # # dcc.Tab(label='User Behaviour', value='user-behaviour',
        # # children=[]),
    ], id="tabs-styled-with-props", active_tab="home"
    )


# def make_item(i):
#     # we use this function to make the example items to avoid code duplication
#     return dbc.Card(
#         [
#             dbc.CardHeader(
#                 html.H2(
#                     dbc.Button(
#                         f"Collapsible group #{i}",
#                         color="link",
#                         id=f"group-{i}-toggle",
#                     )
#                 )
#             ),
#             dbc.Collapse(
#                 dbc.CardBody(f"This is the content of group {i}...",id= f"collapse-card-{i}"),
#                 id=f"collapse-{i}",
#             ),
#         ]
#     )


# accordion = html.Div(
#     [make_item(1), make_item(2), make_item(3)], className="accordion"
# )

# searchLayout = html.Div(
#     [
#         dbc.Input(id="search-input", placeholder="Type something...", type="text"),
#         html.Br(),
#         html.P(id="search-output"),
#         accordion,
#         html.P(id="search-output_1")
#     ]
# )

def serveLayout():
    return html.Div([

            html.Div([
                    
                        
                    dbc.Badge(id = 'last-updated',children = 'Last updated on - '+util.lastupdated, color="light", className="mr-1",style={"float":"right"}),
                        
                    
                    html.Br(),       
                    util.getNavbar(),
                    # html.Div([
                    #     daq.ToggleSwitch(
                    #         label='Large font',
                    #         labelPosition='',
                    #         id='font-toggle-switch',
                    #         value=False
                    #     ),
                    #     # html.Div(id='font-switch-output',style={'display':'none'}),
                    #     dcc.Store(id='font-switch-output', storage_type='local')
                    # ],style = {'width':'15%','float':'right'}),
                    html.Br(),
                    html.Hr(),
                    html.Div([
                    # searchLayout,    
                    tabs,
                    html.Br(),
                    html.Br(),
                    # dcc.Interval(id='update-components',interval=intervalTimer,n_intervals = 0)
                    dcc.Interval(
                        id='interval-component',
                        interval=util.config.refreshInterval*1000, # in milliseconds
                        n_intervals=0
                    )


            ]
            , style = {'margin-left': '5%','margin-right': '5%','margin-bottom': '5%'}
            )
        ]),


        dbc.Toast(
            html.Div([html.Li(msg) for msg in config.whatsNewToast]),
            id="positioned-toast",
            header="Whats new",
            is_open=True,
            dismissable=True,
            icon="primary",
            # top: 66 positions the toast below the navbar
            style={"position": "fixed", "top": 50, "right": 10, "width": 350,"background-color":'rgba(16, 28, 28, 0.1)','font-size':'small'},
        )
    ]
    , style = {'backgroundColor':'#F5F5F5'}
    )


app.layout = serveLayout


# @app.callback(
#     [Output(f"collapse-{i}", "is_open") for i in range(1, 4)]+[Output(f"collapse-card-{i}", "children") for i in range(1, 4)],
#     [Input(f"group-{i}-toggle", "n_clicks") for i in range(1, 4)],
#     [State(f"collapse-{i}", "is_open") for i in range(1, 4)]+[State(f"{i}", "children") for i in searchResults ]
# )
# def toggle_accordion(n1, n2, n3, is_open1, is_open2, is_open3,graph_1,graph_2,graph_3):
#     ctx = dash.callback_context
#     # print({"id": c.component_id, "property": c.component_property} for c in [State('babylon-gender-pie', "children")] )
#     if not ctx.triggered:
#         return ""
#     else:
#         button_id = ctx.triggered[0]["prop_id"].split(".")[0]

#     if button_id == "group-1-toggle" and n1:
#         return not is_open1, False, False,graph_1,None,None
#     elif button_id == "group-2-toggle" and n2:
#         return False, not is_open2, False,None,graph_2,None
#     elif button_id == "group-3-toggle" and n3:
#         return False, False, not is_open3,None,None,graph_3
#     return False, False, False,None,None,None

# @app.callback([Output(f"group-{i}-toggle", "children") for i in range(1, 4)], [Input("search-input", "value")])
# def output_text(value):
#     if value is not None and len(value) > 2 :
#         result = searchObj.getResults(value)
#         global searchResults
#         searchResults = result[:3]
#         # return ', '.join(result[:5])
#         listGroups = []
#         # for ele in result[:5]:
#             # searchResults.append(ele+'_list')
#             # listGroups.append(getListItem(ele))
#         return result[:3]
# 

# @app.callback(
#     Output('font-switch-output', 'data'),
#     [Input('font-toggle-switch', 'value')],
#     [State('font-switch-output', 'data')])
# def update_font(value,data):
#     data = data or {'fontSize': 12}
#     if value == True:
#         data['fontSize'] = 14
#     else:
#         data['fontSize'] = 12
#     util.chartFontSize = data['fontSize']

#     return data


@app.callback(Output('last-updated', 'children'), 
              [Input('interval-component','n_intervals')]
            #   [State('font-switch-output', 'data')]
              )
def update_results(n_intervals):
    # util.chartFontSize = data['fontSize']

    if not dash.callback_context.triggered:
        return 'Last updated on - '+util.lastupdated
        raise dash.exceptions.PreventUpdate()
    # if value:
    #     print('inData  ->' + str(value))
    #     raise dash.exceptions.PreventUpdate()
    
    print(">>CALLBACK: ----Initial Callback-------")
    try:
        # util.doUpdateQueryResults()
        print('--')
    except Exception as e:
        print(">>ERROR : Error in updation ... "+ str(e))    
    util.lastupdated = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print('>>INFO: Setting Last updated to -> ' + str(util.lastupdated) )

    return 'Last updated on - '+util.lastupdated
    

    # if(util.chartFontSize == 14 ):
    #     return [ 'Last updated on - '+util.lastupdated,
    #             True]
    # else:
    #     return [ 'Last updated on - '+util.lastupdated,
    #             False]

print("""\n\n---------------------------------------------------------------------------------
Total time taken(H:M:S) - {} 
---------------------------------------------------------------------------------""".format(time.strftime("%H:%M:%S", time.gmtime( time.time() - start ) )))


if __name__ == '__main__':
    app.run_server(host='0.0.0.0', debug=debug, port=8050)

