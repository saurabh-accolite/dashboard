import dash_bootstrap_components as dbc
import dash_html_components as html
from datetime import timedelta,datetime 

# import dash
from dash.dependencies import Input, Output,State 
import dash_core_components as dcc
from pulseGold.pulseGoldUtil import PulseGoldUtil
import dash 

pulseGoldUtil = PulseGoldUtil()
util = pulseGoldUtil.util
app = util.app

import util.appLogger as appLogger

logger = appLogger.get_logger(__name__)
print = logger.debug


# initial_usage_cards_1 = dbc.CardDeck(
#     [   
#         EHRUtil.get_cards([EHRUtil.decrpt_merge['ehr_MY.subjectId'].nunique()],'ehr_active-user-count',"Total Users"),
#         EHRUtil.get_cards([EHRUtil.decrpt_merge['ehr_MY.encounterId'].nunique()],'ehr_registered-user-count',"Total Encounters"),
#         # EHRUtil.get_cards(['Full Assessment Completed-Total'],'ehr_healthAsses-user-count',"Full Assessment Completed"),
        
#     ])

# initial_usage_cards_2 = dbc.CardDeck(
#     [   
#         EHRUtil.get_cards(['Clicked on iProtect Tile-Total'],'ehr_iprotect_click',"Clicked on iProtect Tile"),
#         EHRUtil.get_cards(['Proposal Submitted'],'ehr_health-assessment-count',"Proposal Submitted"),
#         EHRUtil.get_cards(['Policy Confirmation'],'ehr_symptom-checker-count',"Policy Confirmation")
#     ])


goldCharts = html.Div([
    util.get_section_headers("Pulse Gold Indonesia overview"),

    dbc.Row(
        [
            dbc.Col(
                [
                    dbc.Row(
                        [
                            dbc.Col(
                                html.Div(
                                    util.style_graph(id='gold_numberTable_bar', figure=pulseGoldUtil.getNumberTable(),
                                                     margintop='0.1mm', height=350), id='gold_numberTable_div',
                                    style={'margin': 'auto'}

                                )
                            ),
                        ]
                    ),
                    dbc.Row(dbc.Col(html.H5('27% of subscribers are returning users',style={'background-color':'darkkhaki','margintop':'1mm'}))),
                    dbc.Row(
                        [
                            dbc.Col(

                                html.Div(
                                    util.style_graph(id='gold_daysSubsTable', figure=pulseGoldUtil.tablePurchaseDaysAfterRegistration(),
                                                     margintop='-1mm', height=380), id='gold_daysSubsTable_div',
                                    # style={'margin': 'auto'}
                                )
                            ),
                        ]
                    ),


                ],

                width=3),
            dbc.Col([

                dbc.Row(
                    [
                        dbc.Col(
                            html.Div(
                                util.style_graph(id='gold_subsPurchase_bar', figure=pulseGoldUtil.subscriptionPurchasedPlot(),
                                                 margintop='0.1mm', height=250), id='gold_subsPurchase_bar_div',
                                style={'margin': 'auto'}
                            )
                        ),
                    ]
                ),

                dbc.Row(
                    [
                        dbc.Col(
                            html.Div(
                                util.style_graph(id='gold_subsCancel_bar', figure=pulseGoldUtil.cancelledPurchasedPlot(),
                                                 margintop='0.3mm', height=250), id='gold_subsCancel_bar_div'
                            )
                        ),
                    ]
                ),
                dbc.Row(
                    [
                        dbc.Col(
                            html.Div(
                                util.style_graph(id='gold_subsUninstall_bar', figure=pulseGoldUtil.uninstalledPurchasedPlot(),
                                                 margintop='0.3mm', height=250), id='gold_subsUninstall_bar_div'
                            )
                        ),
                    ]
                )
            ])
        ],
        no_gutters=True)
])


overlapChart = html.Div([
    util.get_section_headers("Pulse Gold - Feature Usage"),
    dbc.Row(
        [   

            dbc.Col(
                html.Div(
                    util.style_graph(id='gold_dayWiseTable', figure=pulseGoldUtil.getDayWiseVisitsTable(),
                                     margintop='0.3mm', height=550), id='gold_dayWiseTable_div'
                ),

             width=3.5),

            dbc.Col(
                html.Div(
                    util.style_graph(id='gold_overlapMatrix', figure=pulseGoldUtil.getOverlapMatrix(),
                                     margintop='0.3mm', height=550), id='gold_overlapMatrix_div'
                )

            )


        ],
    no_gutters=True)

])

ChurnChart = html.Div([
    util.get_section_headers("Churn analysis"),
    dbc.Row(
        dbc.Col(
            html.Div(
                util.style_graph(id='gold_churnScatter', figure=pulseGoldUtil.scatterChurnPlot('minutesToChurnAfterPurchase','minutesToChurnAfterRegistration'),
                                 margintop='0.3mm', height=350), id='gold_churnScatter_div'
            ),
        )
    ),
    dbc.Row(
        dbc.Col(
            html.Div(
                util.style_graph(id='gold_regionWiseBar', figure=pulseGoldUtil.regionWiseBarPlot(),
                                 margintop='1mm', height=500), id='gold_regionWiseBar_div'
            ),
        )
    )


])

usageChart = html.Div([
    util.get_section_headers("Feature Usage for Gold members"),
    dbc.Row(
        dbc.Col(
            html.Div(
                util.style_graph(id='gold_PremiumFeaturesTable', figure=pulseGoldUtil.getPremiumFeaturesTable(),
                                 margintop='0.3mm', height=450), id='gold_PremiumFeaturesTable_div'
            ),
        )
    ),
    dbc.Row(
        dbc.Col(
            html.Div(
                util.style_graph(id='gold_NonPremiumFeatureTable', figure=pulseGoldUtil.getNonPremiumFeatureTable(),
                                 margintop='2mm', height=500), id='gold_NonPremiumFeatureTable_div'
            ),
        )
    )


])




# searchChart = html.Div([
#     util.get_section_headers("User's Search and selection overview"),
    
    

#     html.Div([
#     html.Div(util.style_graph(id='ehr_searchTable',figure=EHRUtil.getTopSearchTable(EHRUtil.dfSerach),
#                 margintop='-1mm',height=450),id = 'ehr_searchTable_div',
#                 style={'display': 'inline-block','width':'35%','margin':'auto','margin-left':'10px'}
#         ),
    
#     html.Div(util.style_graph(id='ehr_search_chart',figure=EHRUtil.getTopSearchChart(EHRUtil.dfSerach),
#                 margintop='-1mm',height=450),id = 'ehr_search_chart_div',
#                 style={'display': 'inline-block','width':'60%','margin':'auto','margin-left':'10px'}
#         )

#     ])
# ])










layout = html.Div([
            # dbc.Badge(
            #     f"Date range - {EHRUtil.decrpt_merge['ehr_MY.auditDetail.createTime'].min()[:10]}  -  {EHRUtil.decrpt_merge['ehr_MY.auditDetail.createTime'].max()[:10]}", 
            #     color="light", className="mr-1",style={"float":"right"}
            #     ),
            # html.Br(),
            # initial_usage_cards_1,
            html.Hr(),
            html.Br(),
            
            goldCharts,
            usageChart,
            overlapChart,
            ChurnChart
           
            
        ])

#=============================================== callbacks ====================================================

# @app.callback(Output('ehr_realtime-event-line-div', 'children'), 
#               [Input('ehr_event-line-dropdown','value'),
#               Input('ehr_period-line-dropdown','value'),
#               Input('ehr_chartType-line-dropdown','value'),
#               Input('last-updated','children')
#               ])
# def update_realtime_event_line(value,period,chartType,last_updated):
    
#     print(">>CALLBACK:----realTime event-line Callback-------")
#     # print(dash.callback_context.triggered)
#     # util.doUpdateQueryResults()
#     if value is not None:
#         return util.style_graph(id='ehr_realtime-event-line',figure=util.getEventsLineGraph(value,period,'',chartType =chartType ),
#                 margintop='-1mm')

# @app.callback(Output('ehr_summary-metrics','children'),
#             [Input('ehr_date-picker-range','start_date'),
#             Input('ehr_date-picker-range','end_date'),
#             Input('ehr_summaryTable-hour-dropdown','value'),
#             Input('last-updated','children')
#             ])
# def update_summary_metrics(start,end,granularity,last_updated):

#     print(">>CALLBACK: Summary metrics table")
    
#     # if not dash.callback_context.triggered:
#     #     raise dash.exceptions.PreventUpdate()
#     return util.get_summary_metrics_table(start,end,granularity = granularity,eventFilter=list(EHRUtil.eventMap.keys()))



# @app.callback([
#                 Output('ehr_active-user-count','children'),
#                 Output('ehr_registered-user-count','children'),
#                 Output('ehr_healthAsses-user-count','children'),
#                 Output('ehr_iprotect_click','children'),
#                 Output('ehr_health-assessment-count','children'),
#                 Output('ehr_symptom-checker-count','children'),
                
#                 # Output('ehr_download-count','children'),
#                 # Output('ehr_unique-device-count','children'),
#                 # Output('ehr_mau-count','children'),
#                 # Output('ehr_dau-count','children'),
#                 # Output('ehr_download-average','children'),
#                 # Output('ehr_unique-average','children'),
#                 # Output('ehr_mau-average','children'),
#                 Output('ehr_active-user-counttrendline','children'),
#                 Output('ehr_registered-user-counttrendline','children'),
#                 Output('ehr_healthAsses-user-counttrendline','children'),
#                 Output('ehr_iprotect_clicktrendline','children'),
#                 Output('ehr_health-assessment-counttrendline','children'),
#                 Output('ehr_symptom-checker-counttrendline','children'),
                
#                 # Output('ehr_download-counttrendline','children'),
#                 # Output('ehr_unique-device-counttrendline','children'),
#                 # Output('ehr_mau-counttrendline','children'),
#                 # Output('ehr_dau-count-trendline','children'),
#                 # Output('ehr_realtime-event-count-table','children')

#              ],
#             [Input('last-updated','children')])
# def update_cards(last_updated):

#     print(">>CALLBACK: Home charts update based on last-updated")
#     # if not dash.callback_context.triggered:
#     #     raise dash.exceptions.PreventUpdate()
    
#     return [
#     util.get_event_count(['Active Users-Total'], 'today')[0],
#     util.get_event_count(['Registered on Pulse-Total'], 'today')[0],
#     util.get_event_count(['Full Assessment Completed-Total'], 'today')[0],
#     util.get_event_count(['Clicked on iProtect Tile-Total'], 'today')[0],
#     util.get_event_count(['Proposal Submitted'], 'today')[0],
#     util.get_event_count(['Policy Confirmation'], 'today')[0],
#     # str(EHRUtil.get_download_count('Unique and Returning devices')[0])+'  ',
#     # str(EHRUtil.get_download_count('Unique devices')[0])+'  ',
#     # str(EHRUtil.get_mau_count()[0])+'  ',
#     # EHRUtil.get_dau_count(),
#     # EHRUtil.get_download_count('Unique and Returning devices')[1],
#     # EHRUtil.get_download_count('Unique devices')[1],
#     # EHRUtil.get_mau_count()[1],
#     util.get_trend_lines(['Active Users-Total'], '300px'),
#     util.get_trend_lines(['Registered on Pulse-Total'], '300px'),
#     util.get_trend_lines(['Full Assessment Completed-Total'], '300px'),
#     util.get_trend_lines(['Clicked on iProtect Tile-Total'], '300px'),
#     util.get_trend_lines(['Proposal Submitted'], '300px'),
#     util.get_trend_lines(['Policy Confirmation'], '300px'),
#     # util.get_trend_lines(['App Download-Unique and Returning devices'],'240px'),
#     # util.get_trend_lines(['App Download-Unique devices'],'240px'),
#     # util.get_trend_lines(['mau'],'240px'),
#     # util.get_trend_lines(['dau'],'240px'),
#     # EHRUtil.get_realtime_event_count_table()
#     ]

