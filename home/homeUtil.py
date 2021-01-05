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
from util.couchbase import CouchbasePython


from itertools import permutations

logger = appLogger.get_logger(__name__)
print = logger.debug


class HomeUtil:
    def __init__(self):
        self.util = Util.getInstance()
        self.cbPython = CouchbasePython(self.util.config.cb_url,self.util.config.cb_username,self.util.config.cb_password)
        self.firstOpenDf = self.cbPython.getFirstOpenCount(self.util.config.cb_buckets, None)
        self.regDf = self.cbPython.getRegCount(self.util.config.cb_buckets, None)
        self.babylonDf = self.cbPython.getBabylonCount(self.util.config.cb_buckets, None)

    def firstOpenCard(self, country):
        
        sumFirstOpen = self.firstOpenDf[self.firstOpenDf['countryCode'] == country].distinctCount.sum()

        return sumFirstOpen

    def regCard(self, country):
        
        regCount = self.regDf[self.regDf['countryCode'] == country].Registration.sum()

        return regCount
    
    def babylonCard(self, country):
        babylonHACount = self.babylonDf[self.babylonDf['countryCode'] == country].ha.sum()
        babylonSCCount = self.babylonDf[self.babylonDf['countryCode'] == country].sc.sum()
        return babylonHACount, babylonSCCount
    
