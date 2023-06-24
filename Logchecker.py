#!/usr/bin/python3
# Authors: Renee Wilkening

'''
NOTE: This code may not function exactly as expected since it was
anonymized code to protect client identity. Mostly file names/paths
were modified and can be updated to preserve functionality.
'''

import os, re, csv, sys, glob, json, time, signal, sqlite3, argparse, subprocess, pprint
sys.path.append('/proj/abcde/robot/support/robot-libs')
import pandas as pd
import datetime as dt # for timedelta function

from robot.libraries.BuiltIn import BuiltIn
from datetime import datetime
from Logger import Logger
from TimeConvert import TimeConvert

# Instantiate the regular dependencies
Log = Logger()
TB = TimeConvert()

if __name__ == '__main__':
    print("This module is meant to be run as an imported resource")
    exit(0)
else:
    class Logchecker:
        """ The Logchecker Class serves to provide the necessary methods for searching
            the logs generated during Testing.
        """
        def __init__(self):
            self.basePath = '/proj/abcde/archive/'

        def getArgs(self, logObjDict):
            """ Extracts arguments from the logObj string passed by the robot script
            """
            argsDict=json.loads(logObjDict)
            searchStr=argsDict['logchkparams'][0]['searchstr']
            searchTime=argsDict['logchkparams'][0]['searchtime']
            logName=argsDict['logchkparams'][0]['logname']
            return searchStr, searchTime, logName

        def fmtSearchTime(self, searchTime):
            dateDate=datetime.strptime(searchTime,"%Y-%m-%d %H:%M:%S.%f")
            return dateDate

        def getAEFile(self, fileStr, fileType):

            #Set the current year
            yearFolder=os.path.join(self.basePath+fileStr, datetime.now().strftime('%Y'))
            #Get the last two months in case the test crosses month boundary
            latestFolders=sorted(glob.glob(os.path.join(yearFolder, '*/')), key=os.path.getmtime, reverse=True)[:2]
            #Get all files in the last two months
            allFiles = []
            for i in latestFolders:
                allFiles.extend(glob.glob(i+'**/'+fileType, recursive=True))
            #Get the latest log from the last two months
            latestLog=max(allFiles, key=os.path.getmtime)
            return latestLog

        def getBFile(self, fileStr, fileType):

            #Navigate file path:
            yearFolder=os.path.join(self.basePath+fileStr, datetime.now().strftime('%Y'))
            #Get all files in the current year
            allFiles=glob.glob(yearFolder+'/**/'+fileType, recursive=True)
            #Get the latest log file from the current year
            latestLog=max(allFiles, key=os.path.getmtime)
            return latestLog

        def getCDFile(self, fileStr, fileType, filterStr):
            #Navigate File Path:
            #Set the current year
            yearFolder=os.path.join(self.basePath+fileStr, datetime.now().strftime('%Y'))
            #Get the last two months in case the test crosses month boundary
            latestFolders=sorted(glob.glob(os.path.join(yearFolder, '*/')), key=os.path.getmtime, reverse=True)[:2]
            #Get all files in the last two months
            allFiles = []
            for i in latestFolders:
                allFiles.extend(glob.glob(i+'**/'+fileType, recursive=True))
            #Filter files for passed str (pwr_cmd or pwr_log) and get the most recent:
            latestLog = max([i for i in allFiles if re.search(filterStr, i) != None],
                    key=lambda f: os.stat(f).st_mtime)
            return latestLog

        def choosePath(self, logName):
            """
            Takes in the logName then calls appropriate getFile(), returning the
            latestLog of specified type.
            """
            if logName == 'a':
                filePath=self.getAEFile('a/', 'a_*.log')
            elif logName == 'b':
                filePath=self.getBFile('b/', 'b_*')
            elif logName == 'c':
                filePath=self.getCDFile('pwr/','*.txt', 'c_')
            elif logName == 'd':
                filePath=self.getCDFile('pwr/','*.txt', 'd_')
            elif logName == 'e':
                filePath=self.getAEFile('cut/e/', 'e_*.log')
            return filePath

        def getDF(self, filePath):
            """
            Takes in filePath, opens and reads it into a 2 column pandas DataFrame[[dateTime, logData]]
            A pandas DF is a fast and easy way to filter on later
            """

            dateTime = []
            logData = []
            with open(filePath) as f:
                for line in f:
                    split = line.split(' ', 1)
                    dateTime.append(split[0].strip('[]'))
                    logData.append(split[1])
            df=pd.DataFrame({'dateTime': dateTime,'logData':logData})
            return df

        def restrictTimeSpace(self, df, searchBegTimeSecs):
            """
            Restricts the dataframe passed to only the beginning time passed plus 30 seconds

            Args:
            - DataFrame
            - The beginning time to start search in seconds (function creates end time: begTime+30 secs)

            Returns:
            - DataFrame restricted to time range [searchBegTimeSecs, +30secs]
            """
            df['dateTime']=pd.to_datetime(df['dateTime'])
            searchEndTimeSecs = searchBegTimeSecs + dt.timedelta(seconds=30) #add 30 secs
            dfSearchSpace=df[(df['dateTime']>searchBegTimeSecs) & (df['dateTime'] < searchEndTimeSecs)].copy()
            return dfSearchSpace

        def searchTimeSpace(self, dfSearchSpace, searchPattern):
            """
            Search the restricted search space for the pattern

            Args:
            - dfSearchSpace: the restricted space we want to search (can pass the whole df if desired.)
            - searchPattern: (The pattern or string we're looking for)

            Returns:
            - True/False (if found)
            - The first instance (row) where True (Nones if it wasn't found)
            """
            dfSearchSpace['foundPat']=dfSearchSpace.drop('dateTime',axis=1).applymap(lambda row: re.search(searchPattern, row)).any(1)
            if dfSearchSpace.size == 0:
                success=False
                evals=f'Search pattern [{searchPattern}] not found within time bounds.'
            else:
                success=dfSearchSpace['foundPat'].max() #True/False
                if success == False:
                    evals=f'Search pattern [{searchPattern}] not found within time bounds.'
                else:
                    evals=dfSearchSpace.loc[dfSearchSpace['foundPat']==True].min() #First Instance
            return success, evals

        def latencyChecker(self, df, searchBegTimeSecs, searchPattern, file, success, evals):
            """
            Checks if the file has logged enough time for the search space (default 30 secs).
            Since the files are a live feed, it's possible that the log hasn't populated enough
            data entries, due to lag times.
            If the pattern wasn't found, checks if there's at least 30 secs of data.
            If not: sleeps for 10 secs, checks again etc. (max of 3 checks)

            Args:
            - DF (Search Space)
            - SearchBegTimeSecs (the begging search time)
            - LatestTime (The last Time in the file)
            - Success (if it was found or not), will skip latency if found even if time hasn't passed
            - Evals (previous evals)

            Returns:
            - Success: True/False if pattern was found (with or without latency)
            - Evals: first instance where True, Nones if it wasn't found
            """

            SearchEndTimeSecs = searchBegTimeSecs + dt.timedelta(seconds=30)
            if df.size == 0:
                latestLogTime = datetime.fromtimestamp(0)
            else:
                latestLogTime=df['dateTime'].max()

            latSuccess, latEvals = success, evals
            loopIteration=0
            if success == True or latestLogTime > SearchEndTimeSecs:
                Log.debug('No latency needed')
                pass
            else:
                while latestLogTime <= SearchEndTimeSecs and latSuccess != True:
                    Log.toAll('adding 10 secs latency')
                    BuiltIn().log_to_console('adding 10 secs latency')
                    time.sleep(10)
                    df=self.getDF(file)
                    dfSearchSpace=self.restrictTimeSpace(df, searchBegTimeSecs)
                    latSuccess, latEvals = self.searchTimeSpace(dfSearchSpace, searchPattern)
                    loopIteration+=1

                    if df.size == 0:
                        latestLogTime = datetime.fromtimestamp(0)
                    else:
                        latestLogTime=df['dateTime'].max()
                    if loopIteration >= 3:
                        Log.toAll('maximum latency')
                        BuiltIn().log_to_console('maximum latency')
                        break
            return latSuccess, latEvals

        def matchLogObj(self, logObjStr):
            """
            Wrapper function for the various methods to check the logs.
            This is the function that is called by the robot scripts

            Args:
            - logObjStr (the string that contains the log search parameters)

            Returns:
            - Success: True/False if pattern was found (with or without latency)
            - Evals: first instance where True, Nones if it wasn't found
            """
            successStr = '0'
            Log.debug('logObjStr: ' + logObjStr)
            searchPattern, searchTime, logName = self.getArgs(logObjStr)
            Log.debug('parsed logObjStr:' + 'searchPattern: ' + searchPattern+', searchTime: '+ searchTime + ', logName: ' + logName+'\n')

            searchTimeDt=self.fmtSearchTime(searchTime) #searchTime in date-time format
            Log.debug('searchTime in dt format: ' + searchTimeDt.strftime("%Y-%m-%d %H:%M:%S.%f") + '\n')
            file=self.choosePath(logName)
            Log.debug('file for log checking: ' + file+'\n')
            df=self.getDF(file)
            dfSearchSpace=self.restrictTimeSpace(df, searchTimeDt)
            successImm, evalsImm=self.searchTimeSpace(dfSearchSpace, searchPattern)
            success, evals=self.latencyChecker(df, searchTimeDt, searchPattern, file, successImm, evalsImm)
            if isinstance(evals, str):
                # handles when no pattern was found within time bounds
                evalsStr = evals + f' File: [{file}]'
            else:
                evalsStr = evals.get(key = 'dateTime').strftime("%Y-%m-%d %H:%M:%S.%f") + ' ' + evals.get(key = 'logData')
            if success == True:
                successStr = '1'
            return successStr, evalsStr

