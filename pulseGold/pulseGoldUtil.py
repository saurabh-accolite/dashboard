import re
import json
import glob
from datetime import timezone, timedelta
from datetime import date
import datetime as dt
import time
import matplotlib.pyplot as plt
import matplotlib
# Third party imports 
import dash_table
import pandas as pd
import numpy as np
import traceback
from pandas import json_normalize
import plotly
from plotly.tools import mpl_to_plotly
from plotly.subplots import make_subplots
# from plotly.offline import *
from plotly import graph_objs as go
import plotly.express as px
import plotly.figure_factory as ff
import dash_bootstrap_components as dbc
import dash_html_components as html
import dash
import dash_core_components as dcc
# Local Imports
from util.util import Util
import util.appLogger as appLogger
from util.exceptionHandler import Exception

from itertools import permutations

logger = appLogger.get_logger(__name__)
print = logger.debug


class PulseGoldUtil:

    def __init__(self):
        self.util = Util.getInstance()
        # self.eventMap = self.getIprotectMap()
        self.dfcus = pd.read_json('./pulseGold/data/211_cus_lat.json')
        self.dfsub = pd.read_json('./pulseGold/data/211_sub_1.json')
        self.dfowner2advid = pd.read_csv('./pulseGold/data/211_adv.csv')
        self.dfdemo = pd.read_csv('./pulseGold/data/pulseGold211UsersDemo.csv')
        self.dfappremove = pd.read_csv('./pulseGold/data/pulseGoldChurn211Users.csv')
        self.dfmerged = self.mergedDataSetPulseGold(
            self.dfcus, self.dfsub, self.dfowner2advid, self.dfdemo, self.dfappremove)

        self.dfEv = self.getEventSubsMerge('./pulseGold/data')
        self.dfGold = self.dfEv.query('inGoldPeriod == True')


# =================================== HTML Functions =======================================================

    def getEHRData(self):
        print('>>INFO: Getting EHR Data ... ')
        df1 = pd.read_json('./EHR/decrpt_merge_MY.json', orient='index')
        return df1

    def getSerachDataMY(self):
        dfSerach = pd.read_csv('./EHR/att_sea_sel.csv')
        dfSerach['searched'] = dfSerach['searched'].replace('[null]', np.nan).apply(
            lambda x: str(eval(x)[0]).lower().strip() if not pd.isna(x) and x != '' else x)
        dfSerach['selected'] = dfSerach['selected'].replace('[null]', np.nan).apply(
            lambda x: str(eval(x)[0]).lower().strip() if not pd.isna(x) and x != '' else x)
        return dfSerach

    def get_cards(self, event, id, description):
        # event_count = self.util.get_event_count(event, 'today')[0]
        # event_count = 0 if pd.isna(event_count) else event_count
        return dbc.Card(
            dbc.CardBody(
                [
                    # count
                    # dbc.Spinner(
                    #     [
                    html.H1(children=event[0],
                            id=id, className="card-title", style={'display': 'inline-block'}),


                    # description
                    html.P(description, style={
                        'display': 'inline-block', 'text-indent': '12px'}),
                    # line
                    # html.Hr(),
                    # html.Div(self.util.get_trend_lines(event, '300px'), id=id+'trendline',
                    #         style={'text-align': 'center', 'align': 'center'})
                    #     ],
                    # color="primary",
                    #     type="grow"
                    # ),
                ]
            )
        )

    def get_summary_cards(self, head, id, event, avg_id, avg_text):
        return dbc.Card(
            dbc.CardBody(
                [
                    html.H6(head),
                    # html.P(desc),
                    html.H1(str(self.summarCardsValues[id])+'  ', id=id, style={
                            'color': 'red', 'display': 'inline-block', 'padding-right': '10px'}, className="card-title"),
                    # html.P(' ', style={'display':'inline-block','text-indent':'12px'}, className="card-title"),
                    html.Img(src=self.util.app.get_asset_url('arrow.png'), style={
                             'display': 'inline-block', 'height': '16px', 'width': '16px', 'text-indent': '6px'}, className="card-title"),
                    html.P(self.summarCardsValues[avg_id], id=avg_id, style={
                           'display': 'inline-block', 'text-indent': '6px'}, className="card-title"),
                    html.P(avg_text, style={'display': 'inline-block',
                                            'text-indent': '6px'}, className="card-title"),
                    # if(event!='mau'):
                    html.Hr(),

                    html.Div(self.util.get_trend_lines(event, '240px'), id=id+'trendline',
                             style={'text-align': 'center', 'align': 'center'}),
                ]
            )
        )


# ----------------------------------------------------Figure  ----------------------------------------------------- --


    def gradient_image(self, ax, extent, direction=0.3, cmap_range=(0, 1), **kwargs):
        phi = direction * np.pi / 2
        v = np.array([np.cos(phi), np.sin(phi)])
        X = np.array([[v @ [1, 0], v @ [1, 1]],
                      [v @ [0, 0], v @ [0, 1]]])
        a, b = cmap_range
        X = a + (b - a) / X.max() * X
        im = ax.imshow(X, extent=extent, interpolation='bicubic',
                       vmin=0, vmax=1, **kwargs)
        return im

    def imageToFigure(self, imagePath):
        fig = go.Figure()

        # Constants
        img_width = 1600
        img_height = 500
        scale_factor = 0.5

        fig.add_trace(
            go.Scatter(
                x=[0, img_width * scale_factor],
                y=[0, img_height * scale_factor],
                mode="markers",
                marker_opacity=0
            )
        )

        # Configure axes
        fig.update_xaxes(
            visible=False,
            range=[0, img_width * scale_factor]
        )

        fig.update_yaxes(
            visible=False,
            range=[0, img_height * scale_factor],
            # the scaleanchor attribute ensures that the aspect ratio stays constant
            scaleanchor="x"
        )

        # Add image
        fig.add_layout_image(
            dict(
                x=0,
                sizex=img_width * scale_factor,
                y=img_height * scale_factor,
                sizey=img_height * scale_factor,
                xref="x",
                yref="y",
                opacity=1.0,
                layer="below",
                sizing="stretch",
                source=imagePath)
        )

        # Configure other layout
        fig.update_layout(
            width=img_width * scale_factor,
            height=img_height * scale_factor,
            margin={"l": 0, "r": 0, "t": 10, "b": 10},
        )
        # fig.show()
        return fig

    @Exception.figureException(logger)
    def getNumberTable(self):
        values = [
            ['<b>Purchased</b>', '<b>Subscription Renewed</b>', 'Cancelled', 'Uninstalled app', 'Due for renewal on 25-Dec',
             'Due for renewal on 26-Dec', 'Payment Received'],
            ['211', '1', '21', '123', '11(7 Churned)', '8(6 Churned)', 'USD 40']
        ]
        fig = go.Figure(data=[go.Table(
            columnorder=[1, 2],
            columnwidth=[40, 30],
            cells=dict(
                values=values,
                line_color='darkslategray',
                fill=dict(color=['rgb(107, 174, 214,0.6)', 'rgb(239, 243, 255)']),
                align=['left', 'center'],
                font_size=12,
                height=30)
        )
        ])
        fig.layout['template']['data']['table'][0]['header']['fill']['color'] = 'rgba(0,0,0,0)'
        fig.update_layout(margin=dict(l=0, r=0, t=10, b=00), width=250)
        return fig

    @Exception.figureException(logger)
    def uninstalledPurchasedPlot(self):
        df = pd.read_csv('./pulseGold/data/ID_pulseGoldChurnUsersCountDareWise211Users.csv')
        df.columns = ['uninstallDate1', 'totalOwners1', 'uninstallDate', 'totalOwners']
        fig, ax = plt.subplots()

        self.gradient_image(ax, direction=0, extent=(0, 2, 0, 2), transform=ax.transAxes,
                            cmap=plt.cm.Blues, cmap_range=(0.1, 0.7))

        ax.set_aspect('auto')
        # plt.show()
        langs = df.uninstallDate.to_list()
        students = df.totalOwners.to_list()
        ax.bar(langs, students, color='yellow', width=0.5)
        ax.set_yticklabels([])
        rects = ax.patches

        ax.set_ylim(0, max(students)+3)

        for rect, label in zip(rects, students):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, label,
                    ha='center', va='bottom')
        ax.text(.5, .88, 'UNINSTALLED', fontsize=20,
                horizontalalignment='center',
                transform=ax.transAxes)
        for spine in ax.spines:
            ax.spines[spine].set_visible(False)
        fig = plt.gcf()
        fig.set_size_inches(8.5, 2.5)
        from textwrap import wrap
        labels = ['\n'.join(wrap(i, 3)) for i in langs]
        ax.set_xticklabels(labels)
        matplotlib.rcParams.update({'font.size': 15})
        plt.tight_layout()
        # plt.savefig('./pulseGold/data/uninstalledPurchasedPlotGraph.png')
        # return self.imageToFigure('./pulseGold/data/uninstalledPurchasedPlotGraph.png'), df
        return mpl_to_plotly(fig)

    @Exception.figureException(logger)
    def cancelledPurchasedPlot(self):

        df = pd.read_csv('./pulseGold/data/ID_pulseGoldCancelDate211Users.csv')
        df.columns = ['cancelDate1', 'totalOwners1', 'cancelDate', 'totalOwners']
        fig, ax = plt.subplots()
        self.gradient_image(ax, direction=0, extent=(0, 2, 0, 2), transform=ax.transAxes,
                            cmap=plt.cm.RdPu, cmap_range=(0.1, 0.7))

        ax.set_aspect('auto')
        langs = df.cancelDate.to_list()
        students = df.totalOwners.to_list()
        ax.bar(langs, students, color='fuchsia', width=0.5)
        ax.set_yticklabels([])
        rects = ax.patches

        ax.set_ylim(0, max(students)+1)

        for rect, label in zip(rects, students):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, label,
                    ha='center', va='bottom')
        ax.text(.5, .88, 'CANCELLED', fontsize=20,
                horizontalalignment='center',
                transform=ax.transAxes)
        for spine in ax.spines:
            ax.spines[spine].set_visible(False)
        fig = plt.gcf()
        fig.set_size_inches(8.5, 2.5)
        from textwrap import wrap
        labels = ['\n'.join(wrap(i, 3)) for i in langs]
        ax.set_xticklabels(labels)
        import matplotlib
        matplotlib.rcParams.update({'font.size': 15})
        plt.tight_layout()
        # plt.savefig('./pulseGold/data/cancelledPurchasedPlotGraph.png')
        # return self.imageToFigure('./pulseGold/data/cancelledPurchasedPlotGraph.png'), df
        return mpl_to_plotly(fig)

    @Exception.figureException(logger)
    def subscriptionPurchasedPlot(self):
        df = pd.read_csv('./pulseGold/data/ID_pulseGoldpurchaseSubscriptionDateWise211Users.csv')
        df.columns = ['purchaseSubscriptionDate1', 'totalOwners1',
                      'purchaseSubscriptionDate', 'totalOwners']

        fig, ax = plt.subplots()

        # background image
        self.gradient_image(ax, direction=0, extent=(0, 2, 0, 2), transform=ax.transAxes,
                            cmap=plt.cm.Blues, cmap_range=(0.1, 0.6))

        ax.set_aspect('auto')
        # plt.show()
        langs = df.purchaseSubscriptionDate.to_list()
        students = df.totalOwners.to_list()
        ax.bar(langs, students, color='b', width=0.5)
        ax.set_yticklabels([])
        rects = ax.patches
        # ax.set_xticklabels(students)

        # Make some labels.
        #labels = ["label%d" % i for i in xrange(len(rects))]
        ax.set_ylim(0, max(students)+2.5)

        for rect, label in zip(rects, students):
            height = rect.get_height()
            ax.text(rect.get_x() + rect.get_width() / 2, height, label,
                    ha='center', va='bottom')
        ax.text(.5, .92, 'SUBSCRIPTION PURCHASED', fontsize=20,
                horizontalalignment='center',
                transform=ax.transAxes)
        #plt.title("SUBSCRIPTION PURCHASED", fontsize=18)
        for spine in ax.spines:
            ax.spines[spine].set_visible(False)
        fig = plt.gcf()
        fig.set_size_inches(8.5, 2.5)
        from textwrap import wrap
        labels = ['\n'.join(wrap(i, 3)) for i in langs]
        ax.set_xticklabels(labels)
        matplotlib.rcParams.update({'font.size': 15})
        #ax.tick_params(axis="x",direction="in", pad=10)

        plt.tight_layout()
        # plt.show()
        # plt.savefig('./pulseGold/data/subscriptionPurchasedGraph.png')
        # return self.imageToFigure('./pulseGold/data/subscriptionPurchasedGraph.png'), df
        return mpl_to_plotly(fig)

    @Exception.figureException(logger)
    def tablePurchaseDaysAfterRegistration(self):
        self.dfmerged['daysToPurchaseAfterRegistration'] = (
            self.dfmerged['purchaseTime'] - self.dfmerged['registrationTime']).dt.days
        self.dfmerged = self.createDaysBandRegistration(
            self.dfmerged, 'daysToPurchaseAfterRegistration')
        df1 = self.dfmerged.groupby('daysAfterRegGroup').agg(
            totalOwners=('owner', 'nunique')).reset_index()
        df1.columns = ['Days After Registration', 'Number of Subscribers']
        fig = go.Figure(data=[go.Table(
            columnwidth=[30, 30],
            #rowwidth = [30,30,30,30,30,30,30,30,30],
            #style_table={'height': '300px', 'overflowY': 'auto'},
            header=dict(
                values=list(df1.columns),
                line_color='darkslategray',
                fill_color='royalblue',
                align=['left', 'center'],
                font=dict(color='white', size=12),
                height=40
            ),
            cells=dict(values=[df1['Days After Registration'], df1['Number of Subscribers']],
                       fill_color='lavender',
                       align='center'))
        ])
        fig.update_layout(
            title_x=0.5,
            width=250,
            margin=dict(l=0, r=0, t=10, b=0)
        )
        # fig.show()
        return fig, None

    @Exception.figureException(logger)
    def getOverlapMatrix(self):
        dfOverlap_res = self.createOverlapMatrixData()
        fig = go.Figure(data=[go.Table(
            columnorder=list(range(1, len(dfOverlap_res.columns)+1)),
            columnwidth=[50]+[35]*len(dfOverlap_res.columns),
            header=dict(
                values=list([f'<b>{x}</b>' for x in dfOverlap_res.columns]),
                line_color='rgb(188, 205, 255,0.5)',
                fill_color='rgb(107, 174, 214,0.6)',
                align=['left', 'center'],
                font=dict(size=11),
                height=40
            ),
            cells=dict(
                values=list(dfOverlap_res.to_dict(orient='list').values()),
                line_color='rgb(188, 205, 255,0.1)',
                fill=dict(color=['rgb(107, 174, 214,0.6)', 'rgb(239, 243, 255)']),
                align=['left', 'center'],
                font_size=12,
                height=30)
        )
        ])
        # fig.layout['template']['data']['table'][0]['header']['fill']['color']='rgba(0,0,0,0)'
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=1), width=700,
                          title='Association Matrix for Premium Feature Visits')
        return fig, dfOverlap_res

    def getDayWiseVisitsTable(self):
        dfScoreRes = self.getDayWiseVisits()
        fig = go.Figure(data=[go.Table(
            columnorder=list(range(1, len(dfScoreRes.columns)+1)),
            columnwidth=[15]+[20]*len(dfScoreRes.columns),
            header=dict(
                values=list([f'<b>{x}</b>' for x in ['Day after Purchase', 'Visited App',
                                                     'Users with Non-premium Visits', 'Users with Premium Visits']]),
                line_color='rgb(188, 205, 255,0.5)',
                fill_color='rgb(164, 229, 255)',
                align=['left', 'center'],
                font=dict(size=11),
                height=40
            ),
            cells=dict(
                values=list(dfScoreRes.to_dict(orient='list').values()),
                line_color='rgb(188, 205, 255,0.1)',
                fill=dict(color=['rgb(164, 229, 255)', 'rgb(239, 243, 255)']),
                align=['left', 'center'],
                font_size=12,
                height=30)
        )
        ])
        import textwrap
        split_text = textwrap.wrap('Premium and Non-Premium Feature Usage over Days after Purchase',
                                   width=35)
        fig.update_layout(margin=dict(l=10, r=10, t=70, b=1),
                          width=350, title='<br>'.join(split_text))

        return fig, None
        # fig.show()

    def scatterChurnPlot(self, minutesToChurnAfterPurchase, minutesToChurnAfterRegistration):
        dfchurn = self.dfmerged[self.dfmerged.userStatus ==
                                'Churned'][self.dfmerged.minutesToChurnAfterPurchase > 0]
        fig = px.scatter(dfchurn, x=minutesToChurnAfterPurchase, y=minutesToChurnAfterRegistration, labels={
            "minutesToChurnAfterRegistration": "Time to Churn Post Registration (10 Times)",
            "minutesToChurnAfterPurchase": "Time to Churn Post Purchase",
            "species": "Species of Iris"
        },)
        fig.update_layout(
            margin=dict(l=10, r=10, t=50, b=1),
            title='Time to churn Post Registration Vs Post Purchase',
            title_x=0.5,
            yaxis=dict(
                #tickmode = 'array',
                tickvals=[50000, 100000, 150000, 200000, 250000, 300000, 350000, 400000, 450000],
                ticktext=['5K', '10K', '15K', '20K', '25K', '30K', '35K', '40K', '45K']
            )
        )

        # fig.show()
        return fig, dfchurn

    def regionWiseBarPlot(self):
        dfm = self.dfmerged.groupby(['region']).agg(totalOwner=('owner', 'nunique')).sort_values('totalOwner', ascending=False).reset_index()\
            .merge(self.dfmerged[self.dfmerged.userStatus == 'Churned'].groupby(['region'])
                   .agg(totalOwner=('owner', 'nunique')).sort_values('totalOwner', ascending=False).reset_index(), on='region', how='outer')
        dfm.columns = ['region', 'totalOwnersBoughtSubscription', 'totalOwnersChurned']
        dfm.fillna(0)
        dfm['percentageOwners'] = (dfm['totalOwnersBoughtSubscription']/211)
        dfm['percentageChurn'] = (dfm['totalOwnersChurned']/123)
        dfm['percentageOwners'] = pd.Series([round(val, 2)
                                             for val in dfm['percentageOwners']], index=dfm.index)
        dfm['percentageOwners'] = pd.Series(
            ["{0:.2f}%".format(val * 100) for val in dfm['percentageOwners']], index=dfm.index)
        dfm['percentageChurn'] = pd.Series([round(val, 2)
                                            for val in dfm['percentageChurn']], index=dfm.index)
        dfm['percentageChurn'] = pd.Series(
            ["{0:.2f}%".format(val * 100) for val in dfm['percentageChurn']], index=dfm.index)
        df123 = dfm[['region', 'percentageOwners', 'percentageChurn']]
        df123 = df123.reindex(index=df123.index[::-1])
        fig = go.Figure(data=[
            go.Bar(name='Percentage of Churn', y=df123.region.to_list()[8:], x=df123.percentageChurn.to_list()[8:], orientation='h', text=df123.percentageChurn.to_list()[8:],
                   textposition='outside'),
            go.Bar(name='Percentage of Total', y=df123.region.to_list()[8:], x=df123.percentageOwners.to_list()[8:], orientation='h', text=df123.percentageOwners.to_list()[8:],
                   textposition='outside')
        ])
        # Change the bar mode

        fig.update_layout(barmode='group', height=500, showlegend=True)
        fig.update_layout(legend=dict(
            orientation='h',
            yanchor="top",
            y=-0.1,
            xanchor="left",
            x=0.5
        ))
        #fig.update_traces(texttemplate='%{text:.2s}', textposition='outside')
        fig.update_layout(uniformtext_minsize=16, uniformtext_mode='hide', font=dict(
            size=13,
        ), title='Region-Wise', title_x=0.5, margin=dict(l=10, r=10, t=50, b=1))
        # fig.show()
        return fig, dfm

    def addTableFig(self, fig, cols, values, row, col, cellHeight=20):
        fig = fig.add_trace(go.Table(
            columnorder=list(range(1, len(cols)+1)),
            columnwidth=[50, 80, 60],
            header=dict(
                values=list([f'<b>{x}</b>' for x in cols]),
                line_color='rgb(188, 205, 255,0.5)',
                fill_color='rgb(107, 174, 214,0.6)',
                align=['left', 'center'],
                font=dict(size=12),
                height=20
            ),
            cells=dict(
                values=values,
                #     line_color='rgb(188, 205, 255,0.1)',
                fill=dict(color=['rgb(239, 243, 255)']),
                align=['left'],
                font_size=12,
                height=cellHeight
            )
        ),
            row=row, col=col
        )
        return fig

    def getPremiumFeaturesTable(self):
        fig = make_subplots(rows=2, cols=1,
                            vertical_spacing=0.0001,
                            shared_xaxes=True,
                            specs=[[{"type": "table"}],
                                   [{"type": "table"}],
                                   ],
                            row_width=[0.6, 0.4]
                            )

        cols = ['Food', 'Usage', 'Observations']

        values = [
            ['Meal Planner', 'Food Journal', 'Consult Nutritionist', 'Healthy Eating Wellness Goal'],
            ["""* 5 users opted for meal plans<br>* Viewed meal plans 15 times<br>* Changed recipes 5 times""",
             '* 1 users logged 2 journal entries', '* 3 user consulted nutritionist over chat',
             '* 1 user activated a habit (yet to becompleted)'],
            ['', '', '', '']
        ]

        fig = self.addTableFig(fig, cols, values, row=1, col=1)

        cols = ['Fitness', 'Usage', 'Observations']

        values = [
            ['Exercise Buddy', 'Workout Videos', 'Move More Wellness Goal', 'Weight Loss Wellness Goal'],
            ["""* 7 users started and completed 11 exercises""",
             '* 15 users watched 30 videos', '* 3 users activated 3 habits (yet to be completed)',
             '* 4 users activated 5 habits (yet to be completed)'],
            ["* Popular exercises<br> &nbsp;&nbsp;* Squat hold<br>&nbsp;&nbsp;* Goblet squats<br>&nbsp;&nbsp;* Dumbbell Curls<br>&nbsp;&nbsp;* Plank", '*Popular videos<br>&nbsp;&nbsp;* Squat hold<br>&nbsp;&nbsp;* Plank reach', '',
             '* Popular habits<br>&nbsp;&nbsp;* Drink water after meals<br>&nbsp;&nbsp;* Fruits and Veggies<br>&nbsp;&nbsp;* Walk 100 steps']
        ]

        fig = self.addTableFig(fig, cols, values, row=2, col=1)
        fig.update_layout(margin=dict(l=10, r=10, t=40, b=1), width=850,
                          title='Premium Feature Usage among Gold Members')

        return fig, None

    def getNonPremiumFeatureTable(self):
        fig = make_subplots(rows=5, cols=1,
                    shared_xaxes=True,
                    vertical_spacing=0,
                   specs=[
                       [{"type": "table"}],
                       [{"type": "table"}],
                       [{"type": "table"}],
                       [{"type": "table"}],
                       [{"type": "table"}]
                   ],
                    row_width=[0.2, 0.2, 0.6,0.2,0.2]
            )
        
        cols = ['Babylon','Usage','Observations']
        values = [
            ['Health Assessments','Symptom Checker'],
            ["""* 24 users have taken up 25 health assessments""",
            '* 5 users have taken up 7 Symptom Checks'],
            ['','']
        ]

        fig = self.addTableFig(fig,cols,values,row=1,col=1)

        cols = ['Fitness Activity Tracker','','']
        values = [
            [''],
            ["* 13 users connected their wearables to Pulse and are tracking activity"],
            ['* Googlefit is the wearable used by all the 13 users']
        ]

        fig = self.addTableFig(fig,cols,values,row=2,col=1)

        cols = ['Communities','','']
        values = [
            ['',''],
            ["* 23 users read 72 posts<br>* 16 posts liked, 2 posts shared and 4 posts unliked"],
            ['* Popular Communities:<br>&nbsp;&nbsp;* Health Awareness<br>&nbsp;&nbsp;* Global Event and Occasion\
            <br>&nbsp;&nbsp;* Halal Lifestyle<br>&nbsp;&nbsp;* My Workout Guide\
            <br>* Popular posts:<br>&nbsp;&nbsp;* Workout Guide Squat Hold<br>&nbsp;&nbsp;* Kids Teach Adult Series: Happiness']
        ]
        fig = self.addTableFig(fig,cols,values,row=3,col=1)

        cols = ['Teleconsultation','','']
        values = [
            [''],
            ["* 2 users consulted the doctors"],
            ['']
        ]
        fig = self.addTableFig(fig,cols,values,row=4,col=1)
        cols = ['Insaan','','']
        values = [
            [''],
            ["* 9 users actively using Insaan for features like Mosque locator"],
            ['']
        ]
        fig = self.addTableFig(fig,cols,values,row=5,col=1,cellHeight=10)

        fig.update_layout(margin=dict(l=10, r=10, t=40, b=1),width = 850,title = 'Non-premium Feature Usage among Gold Members')
        return fig,None


# =============================================== Util functions ==================================================


    def from_dob_to_age(self, born):
        today = dt.date.today()
        # print(today)
        return today.year - born.year - ((today.month, today.day) < (born.month, born.day))

    def createDaysBandRegistration(self, df, ageColumn):
        bins = [-1, 1, 10, 20, 50, 100, 200, 1000]
        labels = ['0', '1-10', '11-20', '21-50', '51-100', '101-200', '>200']
        df['daysAfterRegGroup'] = pd.cut(df[ageColumn], bins=bins, labels=labels, right=True)
        return df

    def createAgeBand(self, df, ageColumn):
        bins = [-1, 17, 20, 25, 30, 35, 40, 50, 500]
        labels = ['<18', '18-20', '21-25', '26-30', '31-35', '36-40', '41-50', '>50']
        df['ageGroup'] = pd.cut(df[ageColumn], bins=bins, labels=labels, right=True)
        return df

    def createDaysBand(self, df, ageColumn):
        bins = [-1, 4, 10, 15, 1000]
        labels = ['<5', '5-10', '11-15', '>15']
        df['activeSinceGroup'] = pd.cut(df[ageColumn], bins=bins, labels=labels, right=True)
        return df

    def createDaysBandPurchase(self, df, ageColumn):
        bins = [-1, 1, 10, 20, 50, 100, 200, 1000]
        labels = ['0', '1-10', '11-20', '21-50', '51-100', '101-200', '>200']
        df['daysAfterRegGroup'] = pd.cut(df[ageColumn], bins=bins, labels=labels, right=True)
        return df

    @Exception.normalException(logger)
    def mergedDataSetPulseGold(self, dfcus, dfsub, dfowner2advid, dfdemo, dfappremove):
        print('>>INFO: Merging Data for Pulse Gold .. .. .. ..')
        dfcus1 = dfcus.copy()
        dfcus1['dob'] = pd.to_datetime(dfcus1.dob)
        dfcus1['age'] = dfcus1['dob'].apply(lambda x: self.from_dob_to_age(x))
        dfcus1 = self.createAgeBand(dfcus1, 'age')
        dfcus1 = dfcus1.rename(columns={'createTime': 'registrationTime', 'id': 'owner'})
        dfsub = dfsub.rename(columns={'customerId': 'owner'})
        dfowner2advid = dfowner2advid[['owner', 'advId']]
        dfdemo = dfdemo.drop_duplicates(subset=['advId', 'operating_system'], keep='last')
        dfappremove.columns = ['advId', 'ap_name', 'ap_date', 'ap_time']
        dfappremove = dfappremove.rename(columns={'ap_date': 'uninstallDate', 'devv': 'advId'})
        dfappremove = dfappremove[dfappremove.ap_name == 'app_remove']
        dfsubreg = dfsub.merge(dfowner2advid, on='owner', how='outer').merge(
            dfdemo, on='advId', how='outer')
        dfsubregcus = dfcus1.merge(dfsubreg, on='owner', how='outer')
        dfsubregcusf = dfsubregcus[['owner', 'advId', 'registrationTime', 'sex', 'ageGroup', 'purchaseTime',
                                    'endDate', 'cancelDate', 'operating_system', 'region', 'city', 'mobile_marketing_name', 'mobile_model_name']]
        dfsubregcusf = dfsubregcusf.merge(dfappremove, on='advId', how='left')
        del dfsubregcusf['ap_name']
        del dfsubregcusf['uninstallDate']
        dfsubregcusf['registrationTime'] = pd.to_datetime(
            dfsubregcusf['registrationTime'], infer_datetime_format=True, utc=True, errors='coerce')
        dfsubregcusf['purchaseTime'] = pd.to_datetime(
            dfsubregcusf['purchaseTime'], infer_datetime_format=True, utc=True, errors='coerce')
        dfsubregcusf['cancelDate'] = pd.to_datetime(
            dfsubregcusf['cancelDate'], infer_datetime_format=True, utc=True, errors='coerce')
        dfsubregcusf['endDate'] = pd.to_datetime(
            dfsubregcusf['endDate'], infer_datetime_format=True, utc=True, errors='coerce')
        dfsubregcusf['uninstallTime'] = pd.to_datetime(dfsubregcusf['ap_time'], utc=True)
        dfsubregcusf['daysToChurnAfterPurchase'] = (
            dfsubregcusf['uninstallTime'] - dfsubregcusf['purchaseTime']).dt.days
        dfsubregcusf['daysToChurnAfterRegistration'] = (
            dfsubregcusf['uninstallTime'] - dfsubregcusf['registrationTime']).dt.days
        dfsubregcusf['minutesToChurnAfterPurchase'] = (
            dfsubregcusf['uninstallTime'] - dfsubregcusf['purchaseTime']).dt.seconds
        dfsubregcusf['minutesToChurnAfterPurchase'] = dfsubregcusf['daysToChurnAfterPurchase'] * \
            24*60 + (dfsubregcusf['minutesToChurnAfterPurchase']/60)
        dfsubregcusf['minutesToChurnAfterRegistration'] = (
            dfsubregcusf['uninstallTime'] - dfsubregcusf['registrationTime']).dt.seconds
        dfsubregcusf['minutesToChurnAfterRegistration'] = dfsubregcusf['daysToChurnAfterRegistration'] * \
            24*60 + dfsubregcusf['minutesToChurnAfterRegistration']/60
        del dfsubregcusf['ap_time']
        dfsubregcusf['userStatus'] = 'Active'
        for i in range(len(dfsubregcusf)):
            if pd.notnull(dfsubregcusf.loc[i, 'cancelDate']):
                dfsubregcusf['userStatus'].loc[i] = 'Cancelled'
            if pd.notnull(dfsubregcusf.loc[i, 'uninstallTime']):
                dfsubregcusf['userStatus'].loc[i] = 'Churned'
        dfsubregcusf['activeSince'] = 0
        for i in range(len(dfsubregcusf)):
            if dfsubregcusf['userStatus'].loc[i] == 'Active':
                dfsubregcusf['activeSince'].loc[i] = (pd.to_datetime(
                    dt.datetime.utcnow(), utc=True) - dfsubregcusf['purchaseTime'].loc[i]).days
            elif dfsubregcusf['userStatus'].loc[i] == 'Churned':
                dfsubregcusf['activeSince'].loc[i] = 0
            elif dfsubregcusf['userStatus'].loc[i] == 'Cancelled':
                dfsubregcusf['activeSince'].loc[i] = 0
        dfsubregcusf = self.createDaysBand(dfsubregcusf, 'activeSince')
        return dfsubregcusf

    def mergeWithEvents(self, row, dfEv, evMap):
        quli = row['Qualification']
        if pd.isna(quli) or quli is None:
            filterQuali = set(evMap[evMap.name == row['name']].Qualification.dropna().unique())
            mask = dfEv.extraAttr.apply(lambda x: len(set(x) & filterQuali) == 0)
        else:
            mask = dfEv.extraAttr.apply(lambda x: quli in x)

        dfEv.loc[(dfEv.name == row['name']) & (mask), ['Usage', 'Feature', 'Activity', 'Premium', 'Category']]\
            = row[['Usage', 'Feature', 'Activity', 'Premium', 'Category']].values

    @Exception.normalException(logger)
    def getEventSubsMerge(self, basePath):
        print('>>INFO: Getting Events and subs merge Data ... ')

        return pd.read_parquet('./pulseGold/data/dfEv_merged_ID.parquet.gzip')

        dfEv = pd.read_json(f'{basePath}/211_lat.json')
        dfEv['extraAttr'] = [x[1:] if type(x).__name__ == 'list' else []
                             for x in dfEv.name.str.split("/")]
        dfEv['name'] = [x[0] if type(x).__name__ !=
                        'float' else x for x in dfEv.name.str.split("/")]

        evMap = pd.read_excel(f'{basePath}/EventMapping_PulseGoldNew.xlsx')
        # print(evMap)
        cols = ['name', 'Qualification', 'Usage', 'Feature', 'Activity', 'Premium', 'Category']
        evMap[cols] = evMap[cols].apply(lambda x: x.str.strip())
        evMap = evMap.replace('', np.nan)

        dfEv['Usage'], dfEv['Feature'], dfEv['Activity'], dfEv['Premium'], dfEv['Category'] = [
            np.nan, np.nan, np.nan, np.nan, np.nan]

        t = evMap.apply(lambda row: self.mergeWithEvents(row, dfEv, evMap), axis=1)

        dfEv['Activity'] = dfEv.apply(lambda x: x['Activity'] if not pd.isna(
            x['Activity']) else x['name'], axis=1)

        restDf = self.dfmerged[['owner', 'registrationTime', 'purchaseTime', 'endDate', 'cancelDate', 'uninstallTime',
                                'daysToChurnAfterPurchase', 'userStatus', 'activeSince', 'activeSinceGroup']]
        restDf = restDf.drop_duplicates()

        dfEv = dfEv.merge(restDf, on='owner', how='left')

        dfEv['inGoldPeriod'] = np.where((pd.to_datetime(dfEv['createTime'], infer_datetime_format=True)) > (
            pd.to_datetime(dfEv['purchaseTime'], infer_datetime_format=True)), True, False)

        return dfEv

    def createOverlapMatrixData(self):
        dfOverlap = self.dfGold.query('Premium == "Y" and Usage == "Convert"')[
            ['Feature', 'owner']].drop_duplicates().dropna()
        count_dct = dfOverlap.groupby('Feature').agg({'owner': 'nunique'}).to_dict()
        count_dct = list(count_dct.values())[0]
        unique_grp = dfOverlap['Feature'].unique()  # get the unique groups
        unique_atr = dfOverlap['owner'].unique()  # get the unique attributes

        combos = list(permutations(unique_grp, 2))

        comp_df = pd.DataFrame(data=(combos), columns=['Feature', 'nextFeature'])
        comp_df['CommonUser'] = 0
        for atr in unique_atr:
            # break dataframe into pieces that only contain the attribute being looked at during that iteration
            temp_df = dfOverlap[dfOverlap['owner'] == atr]

            # returns the pairs that have the attribute in common as a tuple
            myl = list(permutations(temp_df['Feature'], 2))
            for comb in myl:
                # increments the CommonUser column where the Group column is equal to the first entry in the previously mentioned tuple, and the nextFeature column is equal to the second entry.
                comp_df.loc[(comp_df['Feature'] == comb[0]) & (
                    comp_df['nextFeature'] == comb[1]), 'CommonUser'] += 1

        for key, val in count_dct.items():  # put the previously computed TotalCount into the comparison dataframe
            comp_df.loc[comp_df['Feature'] == key, 'TotalCount'] = int(val)

        dfOverlap_res = pd.pivot_table(comp_df, values='CommonUser', index=['Feature', 'TotalCount'],
                                       columns=['nextFeature'], fill_value=0).reset_index()

        for col in dfOverlap_res.columns[2:]:
            dfOverlap_res[col] = dfOverlap_res.apply(
                lambda x: count_dct[col] if col == x.Feature else x[col], axis=1)

        return dfOverlap_res

    def getDayWiseVisits(self):
        self.dfGold = self.dfEv.query('inGoldPeriod == True')
        self.dfGold['createTime'] = pd.to_datetime(
            self.dfGold['createTime'], infer_datetime_format=True, utc=True)
        self.dfGold['purchaseTime'] = pd.to_datetime(
            self.dfGold['purchaseTime'], infer_datetime_format=True, utc=True)
        self.dfGold['diffDaysOfEvent'] = (
            (self.dfGold['createTime'] - self.dfGold['purchaseTime'])/np.timedelta64(1, 'D')).astype('int')

        UsageFilter = ['Visit', 'Convert']
        dfScore = self.dfGold.query('Usage in @UsageFilter and diffDaysOfEvent < 11')\
            .groupby(['diffDaysOfEvent', 'owner', 'Usage', 'Premium'])\
            .agg(eventCount=('name', 'count')).reset_index()

        dfScore = dfScore.assign(
            category=lambda x: (dfScore['Premium'].apply(
                lambda x: 'Non-Premium' if x in ['N'] else 'Premium')) + ' ' + x.Usage
        )

        dfScoreRes = pd.pivot_table(dfScore, values='eventCount', index=['diffDaysOfEvent', 'owner'],
                                    columns=['category'], fill_value=0).reset_index()

        dfScoreRes = dfScoreRes.groupby('diffDaysOfEvent').agg(
            totalUser=('owner', 'nunique'),
            # NonPremiumConvert_user =('Non-Premium Convert',np.count_nonzero),
            NonPremiumVisit_user=('Non-Premium Visit', np.count_nonzero),
            # PremiumConvert_user =('Premium Convert',np.count_nonzero),
            PremiumVisit_user=('Premium Visit', np.count_nonzero),
            # NonPremiumConvert_event =('Non-Premium Convert','sum'),
            # NonPremiumVisit_event =('Non-Premium Visit','sum'),
            # PremiumConvert_event =('Premium Convert','sum'),
            # PremiumVisit_event =('Premium Visit','sum'),
        ).reset_index()

        return dfScoreRes
