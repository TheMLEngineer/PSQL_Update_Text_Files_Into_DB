#!/usr/bin/env python

'''

LinkedIN URL : https://www.linkedin.com/in/themlengineer/
Language : Python 3.x
Purpose : Proof of concept to show that all text files (.txt , .log and .conf etc) can be stored in database and restored (Restore can be easily done with pandas)

'''

# coding: utf-8


import os
from posixpath import dirname
import psycopg2 as pg2
import datetime

# Prints the output in screen and also stores it in log file as well
def print_and_log(file_object , string_object_to_write_into_file):
  file_object.write(datetime.datetime.now().strftime("%d/%m/%Y, %H:%M:%S") + '    INFO    ' +string_object_to_write_into_file)
  print(datetime.datetime.now().strftime("%d/%m/%Y, %H:%M:%S") + '    INFO    ' +string_object_to_write_into_file)

log_file = open('database_log.log' , 'a+')

def get_connection_object(database = 'TestDB' , user='postgres' , password='Admin@123' , host='localhost'):
    connection_object = None
    try:
        connection_object = pg2.connect(database = database , user= user , password= password , host='localhost')
    except:
        print_and_log(log_file , 'Error While Creating connection object , Check the auth paramaters')
    return connection_object

def get_cursor_object(connection_object):
    try:
        cursor_object = connection_object.cursor()
    except:
        print_and_log(log_file , 'Error While Creating Cursor Object')
    
    return cursor_object

def make_a_create_db_transaction(connection , query ):
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()
    except Exception as e :
        print_and_log(log_file , 'Error while making make_a_create_db_transaction DB Transaction , \n The exception message is : ' + str(e))
        with connection.cursor() as cursor:
            cursor.execute('ROLLBACK')

def make_a_select_db_transaction(connection , query ):
    # Rows will read all rows and return
    rows = None
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
    except Exception as e :
        print_and_log(log_file , 'Error while making make_a_select_db_transaction DB Transaction , \n The exception message is : ' + str(e))
        with connection.cursor() as cursor:
            cursor.execute('ROLLBACK')
    return rows


def make_a_insert_db_transaction(connection , query ):
    # Rows will read all rows and return
    rows = None
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            # Commit will change db to actual values that we are inserting
            connection.commit()
    except Exception as e :
        print_and_log(log_file , 'Error while making make_a_insert_db_transaction DB Transaction , \n The exception message is : ' + str(e))
        with connection.cursor() as cursor:
            cursor.execute('ROLLBACK')
    return rows

def check_if_a_table_already_exist(connection , table_name = 'sql_poc' , query = "select exists(select * from information_schema.tables where table_name="):

    #q = "select exists(select * from information_schema.tables where table_name="
    query = query + "'" + table_name + "')"
    # print(query)
    try:
        with connection.cursor() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
    except Exception as e :
        print_and_log(log_file , 'Error while making check_if_a_table_already_exist DB Transaction , \n The exception message is : ' + str(e))
        with connection.cursor() as cursor:
            cursor.execute('ROLLBACK')
    return rows[0][0]

def getListOfFiles(dirName='.'):
    # create a list of file and sub directories 
    # names in the given directory 
    listOfFile = os.listdir(dirName)
    allFiles = list()
    # Iterate over all the entries
    for entry in listOfFile:
        # Create full path
        fullPath = os.path.join(dirName, entry)
        # If entry is a directory then get the list of files in this directory 
        if os.path.isdir(fullPath):
            allFiles = allFiles + getListOfFiles(fullPath)
        else:
            allFiles.append(fullPath)
                
    return allFiles        

def get_absolute_path_of_files_in_directory(file_list):
    abs_path = []
    for file_ in file_list:
        abs_path.append(os.path.abspath(file_))
                
    return abs_path

def read_file_and_return_the_content(file_absolute_path):
    if file_absolute_path.endswith('jar') or file_absolute_path.endswith('jks') or file_absolute_path.endswith('licenseKeyStore') or  file_absolute_path.endswith('zip') or  file_absolute_path.endswith('filepart') :
        return ''
    else:
        with open(file_absolute_path , encoding="utf-8") as f:
            return f.read()

def retify_single_quote_psql(string1):
    return string1.replace("'" , "''")



connection = get_connection_object()
cursor = get_cursor_object(connection)
table_name='sql_poc'
directory_path = '.'

if check_if_a_table_already_exist(connection , table_name=table_name):
    print_and_log(log_file , f'The table named {table_name} already exist , So not creating the table again ...')
else:
    print_and_log(log_file , f'The table named {table_name} does not exist , So creating the table ...')
    make_a_create_db_transaction(connection , query=f"CREATE TABLE {table_name}(backup_id SERIAL PRIMARY KEY, latest_update_time_stamp TIMESTAMPTZ NOT NULL , absolute_file_path VARCHAR(1000) , file_content VARCHAR(20000))")

absolute_path_of_all_files = get_absolute_path_of_files_in_directory(getListOfFiles(dirName = directory_path))

excluding_file_extensions = ['jar']

def remove_extenstion(file_list , extension ='jar'):
    for file in file_list:
        if file.endswith(extension):
            file_list.remove(file)
    return file_list

abs_path = []

for file in absolute_path_of_all_files:
    if not file.endswith('jar') or not file.endswith('jks'):
        abs_path.append(file)


# print(abs_path)

'''
# Inserting wrapper file path and wrapper file content into table
for path in absolute_path_of_all_wrapper_files:
    make_a_insert_db_transaction(connection , query=  f"INSERT INTO {table_name}( latest_update_time_stamp , absolute_file_path , file_content ) VALUES (CURRENT_TIMESTAMP , '{path}' , '{retify_single_quote_psql(read_file_and_return_the_content(path))}')" )
'''

# Inserting all text file's path and all text file's content into table
for path in abs_path:
    print('\n\n' + path + '\n\n')
    make_a_insert_db_transaction(connection , query=  f"INSERT INTO {table_name}( latest_update_time_stamp , absolute_file_path , file_content ) VALUES (CURRENT_TIMESTAMP , '{path}' , '{retify_single_quote_psql(read_file_and_return_the_content(path))}')" )

