from functools import wraps
from plotly import graph_objs as go
import pandas as pd

class Exception:
    def __init__(self):
        pass

    @staticmethod    
    def normalException(logger): 
        
        # logger is the logging object 
        # exception is the decorator objects  
        # that logs every exception into log file 
        def decorator(func): 
            
            @wraps(func) 
            def wrapper(*args, **kwargs): 
                
                try: 
                    return func(*args, **kwargs) 
                
                except: 
                    issue = " **Exception in "+func.__name__+"\n"
                    issue = issue+"-------------------------------------------------------------------------\n" 
                    logger.exception(issue) 
                # raise             
            
            return wrapper 
        return decorator

    @staticmethod    
    def figureException(logger,data=True):  
        def decorator(func): 
            
            @wraps(func) 
            def wrapper(*args, **kwargs): 
                
                try: 
                    return func(*args, **kwargs) 
                
                except: 
                    issue = " **Exception in "+func.__name__+"\n"
                    issue = issue+"-------------------------------------------------------------------------\n" 
                    logger.exception(issue)
                    if data:
                        return Exception.getErrorFigure(), pd.DataFrame()
                    else:
                        return Exception.getErrorFigure()        
                # raise             
            
            return wrapper 
        return decorator
    
    @staticmethod
    def getErrorFigure():
        fig = go.Figure()
        fig.update_layout(
            showlegend=False,
            annotations=[
                dict(
                    text="Figure not shown, exception/data unavailable",
                    showarrow=False,
                    font=dict(
                        family="sans serif",
                        size=16,
                        color="crimson"
                        ),
                    align="center",
                    bordercolor="#c7c7c7",
                    borderwidth=2,
                    borderpad=4, 
                )
            ]
        )

        return fig         

