# -*- coding: utf-8 -*-
"""
Created on Sun Oct 22 14:03:12 2017

@author: ljx93
"""

#Spotify Multi-threads


from spotipy.oauth2 import SpotifyClientCredentials
import spotipy.util as util
import numpy as np
import csv
import requests
import spotipy
import json
import time
import sys
import pandas as pd
import billboard
import os
from multiprocessing.dummy import Pool  # This is a thread-based Pool
from multiprocessing import cpu_count
import glob, os
global num_chunks
import itertools

global destination
global SPOTIPY_CLIENT_ID
global SPOTIPY_CLIENT_SECRET
global input_name
global info_name
global feature_name

################################################################################################
"""
This is a multi-threads crawler for parsing the information of Spotify music and their musical features
Format of input file should be in .csv with three columns entitled by 'year','title' and 'artist'
Column 'year' may be omitted
It returns the information of the music with the most highest popularity using Spotify internal searching engine
Information includes ['id','title','artist','popularity']
Features includes ['acousticness','danceability',
                              'duration_ms','energy','instrumentalness',
                              'key','liveness','loudness','mode','speechiness',
                              'tempo','time_signature','valence']   
Ensue you have installed the above packages
Ensure you have installed the Spotify API tools
please edit the following
"""

destination = 'C:\\My Folder\\Dropbox\\Implementation\\Spotify Crawler\\'  #destination where your input is located
input_name = 'Spotify Title Artist.csv'  #the file name of the input
info_name = 'Spotify Info.csv'           #the file name of the information output
feature_name = 'Spotify Features.csv'    #the file name of the features output

num_chunks = 50                           #number of chunks for multi-threads computation


#Spotify API owned by Junxian Li (Joseph), if it is invalid, please report to junxianli.93@gmail.com
SPOTIPY_CLIENT_ID="498f921d99be49e08e61fdc917e7e27b"
SPOTIPY_CLIENT_SECRET="dbcd61249b184e7280c861f81cf24ba6"
#connect to Spotify
auth = spotipy.oauth2.SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET, proxies=None)
sp = spotipy.Spotify(auth=auth.get_access_token())
#

############################################################################################################
def SpotifyQuery(filename):
    info = pd.read_csv(filename)
    columns = info.columns.values
    query = []
    if 'year' in columns:
        print('include year searching')
        for i, row in info.iterrows():
            q = row['title'] + ' year:' + str(row['year']-1) + '-'+ str(row['year'])
            query.append(q)
    else:
        for i, row in info.iterrows():
            q = row['title']
            query.append(q)       
    query = pd.DataFrame(query)    
    query.to_csv(destination+"query.csv", sep=',', encoding='utf-8',header=False,index=False)
    print("query file output")
    
   

def SpotifyInfo(query,sp=sp):
    for q in query:
        q = q[0]
    title = str.split(q," year")[0]
    sp_id = []
    sp_popularity = []
    sp_name = []
    sp_artist = []        
    loc = None  
    try:
        results = sp.search(q=q, limit=20)
    except:  #reconnect
        auth = spotipy.oauth2.SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET, proxies=None)
        sp = spotipy.Spotify(auth=auth.get_access_token())
        
    if len(results['tracks']['items']) != 0:
        for i, t in enumerate(results['tracks']['items']):
            sp_id.append(t['id'])
            sp_name.append(t['name'])
            sp_artist.append(t['album']['artists'][0]['name'])
            sp_popularity.append(t['popularity'])            
#            print(i,' ',t['name'],' ',t['album']['artists'][0]['name'],' ',t['popularity'])
#            spotifyName.append(t['name'])
#            spotifyID.append(t['id'])
            if np.max(sp_popularity) < 10:
                q = title
                print('second query ')
                print(q)
                sp_id = []
                sp_popularity = []
                sp_name = []
                sp_artist = []        
                loc = None
                results = sp.search(q=q, limit=20)
                if len(results['tracks']['items']) != 0:
                    for i, t in enumerate(results['tracks']['items']):
                        sp_id.append(t['id'])
                        sp_name.append(t['name'])
                        sp_artist.append(t['album']['artists'][0]['name'])
                        sp_popularity.append(t['popularity'])                   
        loc = np.argmax(sp_popularity)
        output = pd.DataFrame(np.array([sp_id[loc],sp_name[loc],sp_artist[loc],sp_popularity[loc]]).reshape(1,4))
    else:
        print('No information ',title,'\n')
        output = pd.DataFrame(np.array(['',title,'','']).reshape(1,4))
        
    return output

def SpotifyFeatures(SpotifyID,sp=sp):
    for ID_ in SpotifyID:
        ID_ = ID_[0]
    acousticness = []
    danceability = []
    duration_ms = []
    energy = []
    instrumentalness =[]
    key =[]
    liveness =[]
    loudness =[]
    mode =[]
    speechiness=[]
    tempo=[]
    time_signature=[]
    valence =[]
    try:
        results = sp.audio_features(ID_)
    except:
        auth = spotipy.oauth2.SpotifyClientCredentials(client_id=SPOTIPY_CLIENT_ID, client_secret=SPOTIPY_CLIENT_SECRET, proxies=None)
        sp = spotipy.Spotify(auth=auth.get_access_token())    

    if len(results) != 0 :
        for f in results:
            acousticness=f['acousticness']
            danceability=f['danceability']
            duration_ms=f['duration_ms']
            energy=f['energy']
            instrumentalness=f['instrumentalness']
            key=f['key']
            liveness=f['liveness']
            loudness=f['loudness']
            mode=f['mode']
            speechiness=f['speechiness']
            tempo=f['tempo']
            time_signature=f['time_signature']
            valence=f['valence']
    else:
        print('No information ',ID_)
        acousticness=''
        danceability=''
        duration_ms=''
        energy=''
        instrumentalness=''
        key=''
        liveness=''
        loudness=''
        mode=''
        speechiness=''
        tempo=''
        time_signature=''
        valence=''
    spotify_features = pd.DataFrame([])
    spotify_features = pd.DataFrame(np.array([ID_,acousticness,danceability,
                              duration_ms,energy,instrumentalness,
                              key,liveness,loudness,mode,speechiness,
                              tempo,time_signature,valence]).reshape(1,14))
    return spotify_features
    
    
def keyfunc(row):
    # `row` is one row of the CSV file.
    # replace this with the name column.
    return row[0]    

    
def SpotifyInfo_m():
    i=0
    output = pd.DataFrame([])   
    #read search field from csv
    #destination = 'C:\\Users\\ljx93\\Desktop\\Implementation\\Youtube Crawler\\'
    filename = destination + input_name
    SpotifyQuery(filename)    
    pool = Pool(cpu_count() * 2)  # Creates a Pool with cpu_count * 2 threads.
    #num_chunks = 50    #alter this if necessary
    filename = destination + 'query.csv'
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
                    result = pool.map(SpotifyInfo, groups)
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
    column_names = ['id','title','artist','popularity']
    #print(output)
    output.columns = column_names        
    output.to_csv(destination+ info_name, sep=',', encoding='utf-8',header=True,index=False)  #alter this if necessary
    pool.close()
    pool.join()

    
def SpotifyFeatures_m():
    i=0
    output = pd.DataFrame([])   
    pool = Pool(cpu_count() * 2)  # Creates a Pool with cpu_count * 2 threads.
    filename = destination + info_name
    print("running...parsing features")
    try:
        with open(filename) as f:
            reader = csv.reader(f)
            next(reader, None)
            chunks = itertools.groupby(reader, keyfunc)
            while True:
                # make a list of num_chunks chunks
                groups = [list(chunk) for key, chunk in
                          itertools.islice(chunks, num_chunks)]
                if groups:
                    result = pool.map(SpotifyFeatures, groups)
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
    column_names = ['ID_','acousticness','danceability',
                              'duration_ms','energy','instrumentalness',
                              'key','liveness','loudness','mode','speechiness',
                              'tempo','time_signature','valence']   
    
    #print(output)
    output.columns = column_names        
    output.to_csv(destination+ feature_name, sep=',', encoding='utf-8',header=True,index=False)  #alter this if necessary
    pool.close()
    pool.join()

    
def main():
    SpotifyInfo_m()
    SpotifyFeatures_m()    
        
if __name__ == "__main__":
    main()   
        
