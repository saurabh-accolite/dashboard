import time
import pandas as pd
import numpy as np


'''
    Decorator to capture time taken by function
    Input - text(text description)
'''
def timeit(text): 
    def Inner(func):
        def wrapper(*args, **kwargs): 
            ts = time.time()
            # startTime = time.ctime()

            returned_value = func(*args, **kwargs) 

            # endTime = time.ctime()
            totalTime = time.strftime("%H:%M:%S", time.gmtime( time.time() - ts ) )
            print("\n{} total time taken(H:M:S) - {}".format(text,totalTime))
            # if dfT is not None:
            #     # idx = 0 if not list(dfT.index) else dfT.index[-1]+1
            #     # print(idx)
            #     # dfT.loc[idx] = [text,startTime,endTime,totalTime]
            #     dfT[text+' '+'StartTime' ] = startTime
            #     dfT[text+' '+'EndTime' ] = endTime
            #     dfT[text+' '+'TotalTime' ] = totalTime

                
            return returned_value              
        return wrapper
    return Inner