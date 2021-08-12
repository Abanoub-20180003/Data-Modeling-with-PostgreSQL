#!/usr/bin/env python3

import psycopg2
import os
import glob2
import pandas as pd 
pd.set_option("display.max_columns",200)
from SQL_Queries import *


#Connect to Database
def connToDataBase(): 
    try:
        conn = psycopg2.connect(user = "postgres",
                                password = "root",
                                port = "5432",
                                database = "sparkify",
                                host = "127.0.0.1")
    except psycopg2.Error as e:
        print(e)
        
    #get cursor
    cur  = conn.cursor()
    
    #set session to autocommit 
    conn.set_session(autocommit = True)
    
    return conn , cur


#function to load files from directory 
def get_files(filePath):
    all_files = []
    for root , dirs , files in os.walk(filePath):
        files  = glob2.glob(os.path.join(root , '*.json'))
        for file  in files:
            all_files.append(os.path.abspath(file))

    return all_files
      
def load_Data_into_DataFrame(path):
    files = get_files(path)

    df = []
    for file in files:
        data_file  = pd.read_json(file , lines= True)
    
        df.append(data_file)
        
    df = pd.concat(df)
    df.head()
    return df

    
#process log_data
log_data_path = "/home/bebo/Data Engineering Course Udcity/Project 1 Data Modeling with PostgreSQL /data/log_data/"

def process_data_of_timeTable():
    logData_df  = load_Data_into_DataFrame(log_data_path)

    #Filter records by NextSong action
    logData_df = logData_df[ logData_df['page']  == 'NextSong']
    
    #Convert the ts timestamp column to datetime
    time  = pd.to_datetime(logData_df['ts'] , unit= 'ms')
    
    #Extract the timestamp, hour, day, week of year, month, year, and weekday from the ts
    time_data = [time, time.dt.hour , time.dt.day, time.dt.isocalendar().week , time.dt.month , time.dt.year, time.dt.weekday ]
    column_labels = ['start_time', 'hour' , 'day', 'week', 'month', 'year', 'weekday']
    
    #combining `column_labels` and `time_data` into a dictionary
    time_dict = dict(zip(column_labels , time_data))
    
    #Create a dataframe, `time_df,` containing the time data for this file
    time_df = pd.DataFrame(time_dict)
    
    return time_df

def insert_data_into_timeTabel(cur , conn):
    time_df = process_data_of_timeTable()
    
    # iterating over rows using iterrows() function 
    for index , row in time_df.iterrows():
    
        #insert into time table
        try:
            cur.execute(time_table_insert , list(row))
            conn.commit()
        except psycopg2.Error as e:
            print("Error while insert into time table")
            print(e)
            

def process_data_for_userTable():
    logData_df  = load_Data_into_DataFrame(log_data_path)
    user_data_df  = logData_df[['userId', 'firstName', 'lastName', 'gender', 'level']]
    user_data_df = user_data_df.drop_duplicates().dropna()
    
    return user_data_df

def insert_data_into_userTable(cur , conn):
    user_data_df =  process_data_for_userTable()
    for index , row in user_data_df.iterrows():
        try:
            cur.execute(user_table_insert , list(row))
            conn.commit()
        except psycopg2.Error as e:
            print("Error while insert into users table")
            print(e)

  

#process song data
song_data_path = "/home/bebo/Data Engineering Course Udcity/Project 1 Data Modeling with PostgreSQL /data/song_data/"   

def process_data_for_songsTable():
    song_data_df = load_Data_into_DataFrame(song_data_path)
    
    song_data_df = song_data_df[['song_id', 'title', 'artist_id','year', 'duration']]
    song_data_df.drop_duplicates().dropna()
    
    return song_data_df
    
def insert_data_into_songsTable(cur , conn):
    song_data_df = process_data_for_songsTable()
    song_data_df.head()
    for index , row in song_data_df.iterrows():
        try:
            cur.execute(song_table_insert , list(row))
            conn.commit()
        except psycopg2.Error as e:
            print("Error while insert into songs table")
            print(e)
    
  
    
def process_data_for_artistTable():
     song_data_df = load_Data_into_DataFrame(song_data_path)
     
     song_data_df = song_data_df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
     song_data_df.drop_duplicates().dropna()
     
     return song_data_df
 
def insert_data_into_artistTable(cur , conn):
    song_data_df  = process_data_for_artistTable()
    
    for index , row in song_data_df.iterrows():
        try:
            cur.execute(artist_table_insert , list(row))
            conn.commit()
        except psycopg2.Error as e:
            print("Error while insert into artists table")
            print(e)
    
    
#process songplays table 
def insert_data_for_songplaysTable(cur , conn):
   log_data_df =  load_Data_into_DataFrame(log_data_path)
   
   #Filter records by NextSong action
   log_data_df = log_data_df[ log_data_df['page']  == 'NextSong']
   log_data_df = log_data_df.drop_duplicates().dropna()
   
   for index , row in log_data_df.iterrows():
       cur.execute(song_select, (row.song, row.artist, row.length))
       result = cur.fetchone()
       
       if result:
           songid, artistid = result
       
       else :
           songid, artistid = None , None
    
       # insert songplay record
       songplay_data = (pd.to_datetime(row.ts, unit='ms'), row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
       cur.execute(songplays_table_insert, songplay_data)
       conn.commit()
           
       try:
           cur.execute("SELECT * FROM songplays;")
           conn.commit()
       except psycopg2.Error as e:
           print(e)
        
       rows = cur.fetchall()
       for i in rows :
            print(i)
    

def main():
     conn , cur  = connToDataBase()   
     insert_data_into_songsTable(cur , conn)
     insert_data_into_userTable(cur , conn)   
     insert_data_into_artistTable(cur , conn)
     insert_data_into_songsTable(cur , conn)
     insert_data_for_songplaysTable(cur , conn)
     
     conn.close()

if __name__ == "__main__":
    main() 
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
        
