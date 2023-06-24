#!/Users/rwilkening/opt/anaconda3/bin/python3

# Author: Renee Wilkening

"""
Simulate environment log files for testing

Example usage, from the terminal, assuming logs are in the same directory as script:

./log_simulator.py a_20221108T171619.log b_20221220T192552 c_20221108T171500.txt d_20221108T171500.txt e_2022_12_20_T01_52_ace.log
"""

import os
from datetime import datetime
import sys
from time import sleep
import subprocess

class logsimulator:
    def __init__(self):
        print('logsimulator initialized')
        linuxUser = os.getlogin()
        if linuxUser == 'ace':
            self.path_base = '/archive'
        else:
            self.path_base = '/Users/rwilkening/Desktop/Data_Science/LogCheckerFastTest/archive'

    def file_parser(self, filename):
        """Determine the type of log file and create the directory + file name"""

        """
        Types of logs:
        - e : e_2022_12_20_T01_52_ace.log
        - b : b_20221220T192552
        - d : d_20221108T171500.txt
        - c : c_20221108T171500.txt
        - a : a_20221108T171619.log
        """

        now = datetime.now()
        year = now.strftime('%Y')
        mth = now.strftime('%m')
        day = now.strftime('%d')
        doy = now.strftime('%j')
        hr = now.strftime('%H')
        mint = now.strftime('%M')
        sec = now.strftime('%S')
        ftype = ''
        strfmt = ''  # format of the time-stamp of the log
        if 'e_' in filename:
            ftype = 'e'
            path_archive = f"""/cut/e/{year}/{mth}/{day}"""
            fname = f"""/e_{year}_{mth}_{day}_T{hr}_{mint}_ace.log"""
            path = self.path_base + path_archive + fname
            os.makedirs(self.path_base+path_archive,exist_ok=True)
            strfmt = '[%Y%m%dT%H%M%S]'
        elif 'b_' in filename:
            ftype = 'b'
            path_archive = f"""/b/{year}/{doy}"""
            fname = f"""/b_{year}{mth}{day}T{hr}{mint}{sec}"""
            path = self.path_base + path_archive + fname
            os.makedirs(self.path_base+path_archive,exist_ok=True)
            strfmt = '%Y%m%dT%H%M%S'
        elif 'd_' in filename:
            ftype = 'd'
            path_archive = f"""/pwr/{year}/{mth}/{day}"""
            fname = f"""/d_{year}{mth}{day}T{hr}{mint}{sec}.txt"""
            path = self.path_base + path_archive + fname
            os.makedirs(self.path_base+path_archive,exist_ok=True)
            strfmt = '%Y%m%dT%H%M%S'
        elif 'c_' in filename:
            ftype = 'c'
            path_archive = f"""/pwr/{year}/{mth}/{day}"""
            fname = f"""/c_{year}{mth}{day}T{hr}{mint}{sec}.txt"""
            path = self.path_base + path_archive + fname
            os.makedirs(self.path_base+path_archive,exist_ok=True)
            strfmt = '%Y%m%dT%H%M%S'
        elif 'a_' in filename:
            ftype = 'a'
            path_archive = f"""/a/{year}/{mth}/{day}"""
            fname = f"""/a_{year}{mth}{day}T{hr}{mint}{sec}.log"""
            path = self.path_base + path_archive + fname
            os.makedirs(self.path_base+path_archive,exist_ok=True)
            strfmt = '%Y%m%dT%H:%M:%S.%f'
        else:
            ftype = 'NA'

        return ftype, path, strfmt


    def startSimulation(self, files):

        num_files = len(files)
        line_number = [0] * num_files
        file_id_list = []
        file_data = []
        file_len = []
        file_type = []
        # collect the first 100k bytes from each file
        for file in files:
            file_id_list.append(open(file,'r'))
            file_data.append(file_id_list[-1].readlines(100000)[:140])
            file_len.append(len(file_data[-1]))
            file_type.append(self.file_parser(file))
            file_id_list[-1].close()

        print("file lengths:", file_len)

        print('Writing file data to new files.')
        # open new files, one for each given file
        file_id_list_new = []
        file_type_list = []
        strfmt_list = []
        for file in files:
            # open new file, with current time stamp
            ftype, fpath, strfmt = self.file_parser(file)
            print(fpath)
            fid = open(fpath,'w')
            file_type_list.append(ftype)
            file_id_list_new.append(fid)
            strfmt_list.append(strfmt)

        # continuously write to files, with new time-stamps
        # if have reached the end of the list of lines, restart at beginning
        while True:
            for j in range(0,num_files):
                time_stamp = datetime.now().strftime(strfmt_list[j])
                if file_type_list[j] == 'a':
                    time_stamp = time_stamp[:-3]

                line = time_stamp + ' ' + file_data[j][line_number[j]].split(' ', 1)[1]
                file_id_list_new[j].write(line)
                line_number[j] += 1
                if line_number[j] >= file_len[j]:
                    line_number[j] = 0
            sleep(0.1)

# implement ability to call file from command line
if __name__ == '__main__':

    files = sys.argv[1:]
    LS = logsimulator()
    LS.startSimulation(files)

    # log_simulate = './log_simulator.py a_20221108T171619.log ' + \
    #             'b_20221220T192552 c_20221108T171500.txt ' + \
    #             'd_20221108T171500.txt e_2022_12_20_T01_52_ace.log'
    # process  = subprocess.Popen([log_simulate],
    #       stdout = subprocess.PIPE,
    #       stderr = subprocess.PIPE,
    #       universal_newlines=True,
    #       shell=True)
    # print(process.pid)

    # process.kill()

