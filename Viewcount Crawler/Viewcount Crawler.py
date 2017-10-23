# -*- coding: utf-8 -*-
"""
Created on Mon Jan  2 12:44:39 2017

@author: Junxian Li (Joseph), email: junxianli.93@gmail.com
"""
import sys
import pandas as pd
global destination
global crawler_dest
global input_name


"""
This is an implementation of a YouTube video viewcount based on ytcrawler created by https://github.com/yuhonglin/YTCrawl
It can also crawl subscribers/shares/watchtimes when available) history.
This tool is implemented under Python 2 environment.
The input should be a column of Youtube video ID, entitled by "ID"
Destination: where you store the input and package 'YTCrawl-master'
Please edit the following

"""
##############################################################################

destination = 'C:\\My Folder\\Dropbox\\Implementation\\Viewcount Crawler\\'  #where you store the input and package 'YTCrawl-master'
input_name = destination + 'Youtube Information.csv'   #your input

output_path = destination + 'output\\'   #where you want the data be stored

##############################################################################


crawler_dest = destination + 'YTCrawl-master\\'
sys.path.append(crawler_dest)

from crawler import Crawler

c = Crawler()
c._crawl_delay_time = 1
c._cookie_update_delay_time = 1


input_data = pd.read_csv(input_name)

print('start parsing')

c.batch_crawl(input_name, output_path) 






