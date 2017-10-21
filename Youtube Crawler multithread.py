# -*- coding: utf-8 -*-
"""
Created on Mon Jan  2 12:44:39 2017

@author: Junxian Li (Joseph), email: junxianli.93@gmail.com
"""
        
import csv
import re
import urllib.request
import urllib.parse
import itertools
import time
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import urllib
import os
from multiprocessing.dummy import Pool  # This is a thread-based Pool
from multiprocessing import cpu_count
import glob, os
global destination
global input_name
global output_name
global num_chunks

######################################################################################################################
"""
This is a multi-threads crawler for parsing the information of Youtube videos and downloading them in .mp4 format
Format of input file should be in .csv with two columns entitled by 'title' and 'artist'
It returns the information of the most matching video using Youtube internal searching engine
Information includes ['url','title','ID','duration','intro']
Ensue you have installed the above packages
Ensure you have installed the youtube-dl
which can be installed by 'pip install youtube-dl' in Windows
"""

#Please edit the following
destination = 'C:\\Users\\ljx93\\Desktop\\Implementation\\Youtube Crawler\\'  #the folder where your input file is located
input_name = "Youtube Title Artist.csv"   #the file name of your input
output_name = "Youtube Information.csv"   #the file name of your output
num_chunks = 50                           #number of chunks for multi-threads computation

#download music destination
download = False  #or False if not download songs
destination_download = 'C:\\Users\\ljx93\\Desktop\\Implementation\\Youtube Crawler\\download\\'  #the destination that your songs are located



######################################################################################################
os.chdir(destination_download)
global file
file =list()

for f in glob.glob("*.m4a"):
    file.append(f.replace(".m4a",""))
##    
for f in glob.glob("*.mp4"):
    file.append(f.replace(".mp4",""))

def getUrl(link):
    url= pd.Series([])
    url[0] = "https://www.youtube.com/watch?v=" + getID(link)[0]
    return url

def getTitle(link):
    getTitle = link.find("a",class_="yt-uix-tile-link yt-ui-ellipsis yt-ui-ellipsis-2 yt-uix-sessionlink spf-link ")
    title = pd.Series([])
    title[0]  = getTitle.text.strip()
    return title
   
# scrapes the ID
def getID(link):
    pattern = re.compile("data-context-item-id=\"([^\.]{11})")
    ID_ = pd.Series([])
    ID_[0]  = pattern.findall(link.decode())[0]
    return ID_
    
# scrapes the duration
def getDuration(link):
    getDuration = link.find("span", "accessible-description") 
    duration = pd.Series([])
    duration[0]  = getDuration.text.strip().replace('- Duration: ','').replace('.','')
    return duration
    
# scrapes the introduction
def getIntro(link):
    getIntro = link.find("div", "yt-lockup-description yt-ui-ellipsis yt-ui-ellipsis-2") 
    intro = pd.Series([])
    if getIntro ==None:
        return intro
    else :
        intro[0]  = getIntro.text.strip()
        return intro
 
def crawler(urlRecord,download=download):    #title,artist        
    output=pd.DataFrame([])
    user_agent = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64)'
    values = {'name' : 'Joseph',
          'location' : 'US',
          'language' : 'Python' }
    headers = { 'User-Agent' : user_agent }
    data = urllib.parse.urlencode(values)
    data= data.encode('ascii')
    for visit_url in urlRecord:
        visit_url = visit_url[0]
       # print(visit_url)
        try: 
            req = urllib.request.Request(visit_url, data, headers)
            #r = requests.get(visit_url)
            with urllib.request.urlopen(req) as response:
                soup = BeautifulSoup(response,'lxml')
            detail = getDetail(soup)
            if download:
                downloader(detail)
            output = pd.concat([output,detail],axis=0)
                       
        except:                                  #avoid suspending by Youtube
            print("hold ",visit_url)    
            time.sleep(3.0)
            try:
                req = urllib.request.Request(visit_url, data, headers)
                #r = requests.get(visit_url)
                with urllib.request.urlopen(req) as response:
                    soup = BeautifulSoup(response,'lxml')
                    detail = getDetail(soup)
                    if download:
                        downloader(detail)
                    output = pd.concat([output,detail],axis=0)

            except:
                print("missed", visit_url)
                continue   #ignore

            

    return output
            
def downloader(detail):
    if detail[2][0] in file:
        time.sleep(1.0)
    else:
        os.system("youtube-dl -f 140 --prefer-ffmpeg --output " +detail[2][0] +".%(ext)s " +detail[0][0])
        time.sleep(1.0)

            
def getDetail(soup):
    parent_link = soup.find("ol","item-section")
    link = parent_link.find("li")
    minute =[]
    detail=pd.DataFrame([])
    while link != None:
        AD = link.find("span","yt-badge ad-badge-byline yt-badge-ad")   #locate AD
        live = link.find("span","yt-badge yt-badge-live")              #locate live
        spell = link.find("span","spell-correction-corrected")     #locate spell-correction
        if AD != None or live !=None or spell !=None:
            link = link.find_next_sibling("li")    #if it is AD or live or spell, continue
            continue
        else:                                      #else parsing url ID duration etc.
            url = getUrl(link)
            title=getTitle(link)
            ID_=getID(link)
            duration=getDuration(link)
            intro = getIntro(link)
            try: 
                minute = time.strptime(duration[0],"%M:%S")
                if minute[4] < 8:    #download the first video whose duration is less than 8 minutes
                   detail = pd.concat([url,title,ID_,duration,intro],axis=1)
                   break
                else:
                    link = link.find_next_sibling("li")        
                    continue
            except:
                link = link.find_next_sibling("li")   
                continue
                
    return detail

def makeURL(filename):
    visit_url = []
    data = pd.read_csv(filename) #filename: "C:\\Project\\crawl\\billboard5_noduplicate.csv"
    base = "https://www.youtube.com/results?sp=CAASAhAB&q="
#    print(base)
    search = data['title']+' by '+data['artist']
    query= search.str.replace(' ','+')
    for q in query:
        q= q.replace('#','%23')    #special character
        visit_url.append(base + str(q))
    visit_url = pd.DataFrame(visit_url)

    visit_url.to_csv(destination+"query.csv", sep=',', encoding='utf-8',header=False,index=False)
    print("query file output")


  
def keyfunc(row):
    # `row` is one row of the CSV file.
    # replace this with the name column.
    return row[0]    

    
def main():
    i=0
    output = pd.DataFrame([])
   
    #read search field from csv
    #destination = 'C:\\Users\\ljx93\\Desktop\\Implementation\\Youtube Crawler\\'
    filename = destination+input_name
    makeURL(filename)    
    pool = Pool(cpu_count() * 2)  # Creates a Pool with cpu_count * 2 threads.
    num_chunks = 50    #alter this if necessary
    filename = destination+"query.csv"
    print("running...")
    try:
        with open(filename) as f:
            reader = csv.reader(f)
            chunks = itertools.groupby(reader, keyfunc)
            while True:
                # make a list of num_chunks chunks
                groups = [list(chunk) for key, chunk in
                          itertools.islice(chunks, num_chunks)]
                if groups:
                    result = pool.map(crawler, groups)
                    output = pd.concat([output,pd.concat(list(result))],axis=0)
                    print("chunk ",i)
                    i+=1
                    if i % 20 == 0:
                        time.sleep(10.0)
                    else:
                        time.sleep(5.0)
                else:
                    break
    except IOError:
        print("IO error, please check your working directory and files names")
    else:
        f.close()
    column_names = ['url','title','ID','duration','intro']
    output.columns = column_names        
    output.to_csv(destination+output_name, sep=',', encoding='utf-8',header=True,index=True)  #alter this if necessary
    pool.close()
    pool.join()
 
   
        
if __name__ == "__main__":
    main()   
        
