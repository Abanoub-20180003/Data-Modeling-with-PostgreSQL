#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import psycopg2
from SQL_Queries import create_table_queries , drop_table_queries 


def connect_to_database(user_name,password,database):
    try:
        conn  = psycopg2.connect(user = user_name,
                                 password = password,
                                 port = "5432",
                                 host = "127.0.0.1",
                                 database = database)
    except psycopg2.Error as e:
        print(e)
        
    #get the cursor from connection
    try:
        cur  = conn.cursor() 
    except psycopg2.Error as e:
        print(e)
    #set session to auto commit the queries
    conn.set_session(autocommit=True)
    return cur , conn 
        
def create_database(cur , conn):
    """
    drop if exists sparkify database
    create sparkify database
    close connection to udcity database 
    reconnect to sparkify database
    """
    
    #drop database first if exists
    try:
        cur.execute("DROP DATABASE IF EXISTS sparkify;")
    except psycopg2.Error as e:
        print(e)
        
    #create sparkfiy database
    try:
        cur.execute("CREATE DATABASE sparkify WITH ENCODING 'utf8' TEMPLATE template0;")
    except psycopg2.Error as e:
        print(e)
    
    #reconnect to sparkify database
    conn.close()
    cur , conn  = connect_to_database("postgres", "root", "sparkify")
    return cur , conn
    

        
#create tables in the create tablies list
def create_tables(cur, conn ):
    for query in create_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print(e)
    conn.commit()    
#drop tables in the drop tablies list       
def drop_tables(cur , conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except psycopg2.Error as e:
            print(e)
    conn.commit()
  
#main function         
def main():
   #connect to udcity database 
   cur , conn =  connect_to_database("postgres", "root", "udcity")
   
   #create new database sparkify 
   cur , conn = create_database(cur , conn)
   
   #drop tables
   drop_tables(cur , conn)
   
   # #create tables
   create_tables(cur , conn)

   # #close connection
   conn.close
    
if __name__ == "__main__":
    main()
        
