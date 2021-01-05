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
from util.couchbase import CouchbasePython


from itertools import permutations

logger = appLogger.get_logger(__name__)
print = logger.debug


class HomeUtil:
    def __init__(self):
        self.util = Util.getInstance()
        self.cbPython = CouchbasePython(self.util.config.cb_url,self.util.config.cb_username,self.util.config.cb_password)
        # self.firstOpenDf = self.cbPython.getFirstOpenCount(self.util.config.cb_buckets, None)
        self.regDf = self.cbPython.getRegCount(self.util.config.cb_buckets, None)
        self.babylonDf = self.cbPython.getBabylonCount(self.util.config.cb_buckets, None)
        self.featureDict = {'fo':'FirstOpen','reg':'Registration','ha':'HealthAssessment','sc':'SymptomChecker'}

    # def firstOpenCard(self, country):
        
    #     sumFirstOpen = self.firstOpenDf[self.firstOpenDf['countryCode'] == country].distinctCount.sum()

    #     return sumFirstOpen

    def foRegCard(self, country, startDate, endDate):
        df = self.regDf[self.regDf['date'] > startDate] 
        df = df[df['date'] < endDate]
        regCount = df[df['countryCode'] == country].Registration.sum()
        foCount = df[df['countryCode'] == country].FirstOpen.sum()

        return foCount,regCount
    # def firstOpenCard(self, country, startDate, endDate):
    #     df = self.firstOpenDf[self.firstOpenDf['date'] > startDate] 
    #     df = df[df['date'] < endDate] 

    #     sumFirstOpen = df[df['countryCode'] == country].distinctCount.sum()
    #     return sumFirstOpen

    # def regCard(self, country, startDate, endDate):
    #     df = self.regDf[self.regDf['date'] > startDate] 
    #     df = df[df['date'] < endDate]

    #     regCount = df[df['countryCode'] == country].Registration.sum()
    #     return regCount
    
    def babylonCard(self, country, startDate, endDate):
        df = self.babylonDf[self.babylonDf['date'] > startDate] 
        df = df[df['date'] < endDate]

        babylonHACount = df[df['countryCode'] == country].ha.sum()
        babylonSCCount = df[df['countryCode'] == country].sc.sum()
        return babylonHACount, babylonSCCount

    
    def customTrendLine(self,df,x,y,country):

        res = df.copy()
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
            
