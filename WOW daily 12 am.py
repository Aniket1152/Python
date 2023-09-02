import re
import os
import csv
import pandasql as ps
import pandas as pd
import pygsheets as pg
import numpy as np
import psycopg2
import gspread
import datetime

from datetime import date,timedelta



# Setting the path

os.chdir('C:/Users/User/Downloads')
print(os.getcwd())

# Google sheet auth JSON file

def myAuthToken():
    gc = pg.authorize(service_file = ("DO_NOT_DELETE.json"))
    return gc

def next_available_row(worksheet):
    str_list = list(filter(None, worksheet.get_col(1)))
    return str(len(str_list)+1)


#Reading and copying data
gc = myAuthToken()

#Sm Ranking
sh_1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1ERpJbmyR6Mi-QOoX_4yQSdt4dgdCdhFV2VOlXuPYHyQ/edit#gid=292645799')
#Calls talkdesk
sh_2 = gc.open_by_url('https://docs.google.com/spreadsheets/d/16SCdnOs6dXK_kscH81lYu4gU-bdVj7ZNrlcb9frJTjQ/edit#gid=1978215400')
#UK GB
sh_3 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1Pl4uoLNuBpaWtmV_ZubU2xLPF9L6ykpkuoE9yAAJiFw/edit#gid=1946004478')
#l1-l0
sh_4 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1Isem8UZn34cNnU1lt4DFF7XOHA4BtW3aEhWcuVpP1bQ/edit#gid=1946004478')
#Calls UK
sh_5 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1k4dwviaMySQ3Y4b8q_bXAkj_oDqPkNdFZShryzw4mW8/edit#gid=1946004478')

# get the first sheet of the Spreadsheet
wk_1 = sh_1.worksheet('title','Calls_Raw')
wk_2 = sh_1.worksheet('title','Calls')
wk_3 = sh_2.worksheet('title','UK_TT_weekly')
wk_4 = sh_3.worksheet('title','Calls')
wk_5 = sh_4.worksheet('title','Calls')
wk_6 = sh_5.worksheet('title','Calls')

print("Completed Reading the Data!")

# Reading file from local

df=pd.read_csv('TT.csv')
df1=pd.read_csv('Agents Report.csv', skiprows = 1)


# Filtering the data basis condition

temp_df=df.loc[df['Call Type'].str.contains('outbound', flags=re.I, regex=True)]
temp_df=temp_df[["Interaction ID","Call Type","Start Time","End Time","Talkdesk Phone Number","Customer Phone Number","Talk Time","Agent Name"]]
df1=df1[["Agent Name","Email","Agent Active?"]]
df1=df1.sort_values(by=['Agent Name'])
df1=df1.sort_values(by=['Agent Active?'], ascending=False)
df1['User']= df1['Email'].str.split('@').str[0].astype(str) + "@"
df1 = df1.groupby(by=['Agent Name'], as_index=False).first()
df1.drop(columns=['Email'],inplace = True)
df1.rename(columns={'User':'Email'},inplace=True)

# Clean up data

temp_df=temp_df.dropna(how='any')
df1=df1.dropna(how='any')

# Combining the dataframes

temp_df=pd.merge(temp_df,df1,on='Agent Name',how='left')

# Rename agent name column

temp_df.rename(columns = {'Agent Name':'SM_Name'},inplace=True)
temp_df.rename(columns = {'Start Time':'start_dt'},inplace=True)
temp_df.rename(columns = {'End Time':'end_dt'},inplace=True)


# Adding addition columns

temp_df['Total_Attempt']= temp_df['Call Type'].apply(lambda x: 1 if x == 'outbound_missed' else 1)
temp_df['Connect']= temp_df['Call Type'].apply(lambda x: 1 if x == 'outbound' else '')
temp_df['duration']=temp_df['Talk Time'].str.split(':').apply(lambda x:  int(x[0])*60 + int(x[1]) + int(x[2])/60)
#temp_df['hour'] = pd.to_datetime(temp_df["End Time"],format='%H:%M:%S').dt.hour
#temp_df['minute'] = pd.to_datetime(temp_df["Talk Time"],format='%H:%M:%S').dt.minute
#temp_df['second'] = pd.to_datetime(temp_df["Talk Time"],format='%H:%M:%S').dt.second
temp_df['GT_1_min_calls']= temp_df['duration'].apply(lambda x: 1 if x >= 1 else '')
temp_df['BTW_0_to_10_Sec']= temp_df['duration'].apply(lambda x: 1 if x < 0.16 else '')

#print(temp_df.info())

# SQL query for aggregation of data

q="""select date(end_dt) as date,Email,SM_Name,
    sum(Total_Attempt) as Total_Attempt,
    sum(Connect) as Connect,
    sum(duration) as duration,
    sum(GT_1_min_calls) as GT_1_min_calls,
    sum(BTW_0_to_10_Sec) as BTW_0_to_10_Sec
    from temp_df group by date(end_dt),Email,SM_Name;"""

sql_output = ps.sqldf(q)
#print(sql_output)

# Transferring data to Google sheet

gc = myAuthToken()
sh_1 =gc.open_by_url('https://docs.google.com/spreadsheets/d/1ERpJbmyR6Mi-QOoX_4yQSdt4dgdCdhFV2VOlXuPYHyQ/edit#gid=292645799')
worksheet_raw_data = sh_1.worksheet('title','Calls_Raw')
worksheet_raw_data.clear()
print('sheet cleared')
worksheet_raw_data.update_value((1,1) , "Last_updated_at")
worksheet_raw_data.update_value((1,2) , datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f'))
worksheet_raw_data.set_dataframe(sql_output,(3,1),False,True)
print('Data Tranfered Success')

#Deleting headers
sql_output.columns = sql_output.iloc[0]
sql_output.reset_index()
sql_output.drop(0)

#SM Ranking Data Calls
Lastrow = next_available_row(wk_2)
print("The empty row value is",Lastrow)
wk_2.set_dataframe(sql_output,(int(Lastrow),1),False,extend = True)
print("Data has been successfully copied to SM Ranking Data Calls Tab!")

#UK_TT_Weekly - Calls Data Talkdesk
Lastrow1 = next_available_row(wk_3)
Lastrow1 = int(Lastrow1)+1
print("The empty row value is",Lastrow1)
wk_3.set_dataframe(sql_output,(int(Lastrow1),1),False,extend = True)
print("Data has been successfully copied to Calls Data Talkdesk - UK_TT_weekly tab!")

#UK/GB Sales input metrics - Calls tab
Lastrow2 = next_available_row(wk_4)
Lastrow2 = int(Lastrow2)
print("The empty row value is",Lastrow2)
wk_4.set_dataframe(sql_output,(int(Lastrow2),1),False,extend = True)
print("Data has been successfully copied to UK/GB Sales input metrics - Calls tab!")

#L0-L1 UK - Calls
Lastrow3 = next_available_row(wk_5)
Lastrow3 = int(Lastrow3)
print("The empty row value is",Lastrow3)
wk_5.set_dataframe(sql_output,(int(Lastrow3),1),False,extend = True)
print("Data has been successfully copied to L0-L1 UK - Calls tab!")

#Calls Data UK
Lastrow4 = next_available_row(wk_6)
Lastrow4 = int(Lastrow4)
print("The empty row value is",Lastrow4)
wk_6.set_dataframe(sql_output,(int(Lastrow4),1),False,extend = True)
print("Data has been successfully copied to Calls Data UK- Calls tab!")
