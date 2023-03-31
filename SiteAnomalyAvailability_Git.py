# -*- coding: utf-8 -*-
"""


@author: hadis.ab


"""

import glob
import os
from ftplib import FTP
import re
import time
from datetime import datetime
import pandas as pd
#import pysftp
import datetime
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import seaborn as sns
import copy
import keyring
import shutil
import matplotlib.gridspec as gridspec
import warnings
warnings.filterwarnings("ignore")



'''Phase1'''
main_directory=r'E:\Scripts\Anomaly_avail_sites\\' #you can select a root for this project which contains all input and outputs



#%% insert data


os.chdir(main_directory) 
excel_lists=os.listdir(main_directory)

stats= [i for i in excel_lists if i.startswith("BigData_Site_Hourly")]  
Desired_days=60
#Desired_hours=24*Desired_days

if len(stats)>Desired_days:
    os.remove(stats[0])
    del stats[0]

TotalDF=pd.DataFrame()
for i in stats:
    DF= pd.read_csv(i, header=1 ,thousands = ',')#,usecols=(['Time', 'SITE', 'Vendor', '2G_TCH_AVAILABILITY_IR(%)','3G Cell_Avail_Sys_IR(%)','4G_CELL_AVAIL_SYS_IR','4G_TDD_Cell_Avail_sys_IR(%)']))
    DF=DF.rename(columns={'4G_CELL_AVAIL_SYS_IR(#)':'4G_CELL_AVAIL_SYS_IR'})
    TotalDF = pd.concat([TotalDF,DF[['Time', 'SITE', 'Vendor', '2G_TCH_AVAILABILITY_IR(%)','3G Cell_Avail_Sys_IR(%)','4G_CELL_AVAIL_SYS_IR','4G_TDD_Cell_Avail_sys_IR(%)']]])

TotalDF['Time']=pd.to_datetime(TotalDF['Time'])
TotalDF['Hour']=TotalDF['Time'].dt.hour

Total_hours=TotalDF['Time'].unique()
#stats=pd.concat([stats, stats7hours])

stats=TotalDF.drop_duplicates(subset=['Time','SITE'], keep='last')

stats.dtypes
if stats['4G_CELL_AVAIL_SYS_IR'].dtype=='O': # This part has been added to ignore the cells with negative throughput
        print('y')                  # ERROR: You may see an error because of negative throughput, please find the location of error in excel and delet the negative throughput rows
        pattern=r'-.*'
        #pattern=r'^-\d?.*'
        #data_temp=data_temp.dropna(subset=['4G_Throughput_UE_DL_kbps_IR(Kbps)'])
        list_=stats['4G_CELL_AVAIL_SYS_IR']
        for i in list_:
            result=re.search(pattern,str(i))
            if result:
                print(result.group())
                stats['4G_CELL_AVAIL_SYS_IR']=stats['4G_CELL_AVAIL_SYS_IR'].replace(result.group(),0)
                #data_temp=data_temp[data_temp['4G_Throughput_UE_DL_kbps_IR(Kbps)']!=result.group()]
stats_copy=stats.copy()
if stats['3G Cell_Avail_Sys_IR(%)'].dtype=='O': # This part has been added to ignore the cells with negative throughput
        print('y')                  # ERROR: You may see an error because of negative throughput, please find the location of error in excel and delet the negative throughput rows
        pattern=r'-.*'
        #pattern=r'^-\d?.*'
        #data_temp=data_temp.dropna(subset=['4G_Throughput_UE_DL_kbps_IR(Kbps)'])
        list_=stats['3G Cell_Avail_Sys_IR(%)']
        for i in list_:
            result=re.search(pattern,str(i))
            if result:
                print(result.group())
                stats['3G Cell_Avail_Sys_IR(%)']=stats['3G Cell_Avail_Sys_IR(%)'].replace(result.group(),0)



#stats['rank']=stats.groupby('SITE')['Time'].rank("dense", ascending=False) -,678.52
#stats['4G_CELL_AVAIL_SYS_IR']=stats['4G_CELL_AVAIL_SYS_IR'].replace('-,770.73',0)
stats['4G_CELL_AVAIL_SYS_IR']=stats['4G_CELL_AVAIL_SYS_IR'].astype(float) #type changing
stats['3G Cell_Avail_Sys_IR(%)']=stats['3G Cell_Avail_Sys_IR(%)'].astype(float) #type changing

'''['Time', 'SITE', 'Vendor', '2G_TCH_AVAILABILITY_IR(%)',
    '3G Cell_Avail_Sys_IR(%)', '4G_CELL_AVAIL_SYS_IR',
      '4G_TDD_Cell_Avail_sys_IR(%)', 'Hour', 'rank']'''


tuples = list( zip( *[   stats['Time'],stats['SITE'], stats['Vendor'],  stats['Hour'] ]  ) )
    
stats.index=pd.MultiIndex.from_tuples(tuples, names=['Time','SITE','Vendor','Hour'])
del stats['Time'],stats['SITE'],stats['Vendor'],stats['Hour']
stats= stats.stack().reset_index()
stats=stats.rename(columns={'level_4':'KPI',0:'Value'})


'''['Time', 'SITE', 'Vendor', 'Hour', 'KPI', 'Value']'''
stats['rank']=stats.groupby(['SITE','KPI'])['Time'].rank("dense", ascending=False)
stats['Value'][stats['Value']<0]=0



stats['Target']=98 #Target

stats['compare']=stats['Value']-stats['Target']
stats['status']=1
stats['status'][stats['compare']<0]=0 #kharab
stats['concat']=stats['SITE']+stats['KPI']

last_hour=stats[stats['rank']==1] #b=total_hours_problematic[total_hours_problematic['SITE']=='N4521X']

tech_numbers=last_hour[['Time','SITE']].groupby('SITE').count()
tech_numbers=tech_numbers.reset_index()
tech_numbers=tech_numbers.rename(columns={'Time':'Total_Tech_#'})

total_hours_problematic=stats[stats['status']==0]
 

last_hour_problematic=last_hour[last_hour['status']==0]

last_hour_problematic_tech_numbers=last_hour_problematic[['Time','SITE']].groupby('SITE').count()
last_hour_problematic_tech_numbers=last_hour_problematic_tech_numbers.reset_index()
last_hour_problematic_tech_numbers=last_hour_problematic_tech_numbers.rename(columns={'Time':'Problematic_Tech_#'})


last_hour_problematic=pd.merge(last_hour_problematic,tech_numbers, on='SITE', how='left' )
last_hour_problematic=pd.merge(last_hour_problematic,last_hour_problematic_tech_numbers, on='SITE', how='left' )




stats_time=stats['Time'].drop_duplicates()


last_hour_problematic['Age']=1 
#for site in Problematic_sites:
r=1    
for con in last_hour_problematic['concat'] : #con=last_hour_problematic['concat'][0]
    #print(con)
    r=r+1
    print(r)
    temp_df=total_hours_problematic[total_hours_problematic['concat']==con]
    cnt=2
    while cnt in list(temp_df['rank']):
        last_hour_problematic['Age'][last_hour_problematic['concat']==con]=last_hour_problematic['Age'][last_hour_problematic['concat']==con]+1
        cnt=cnt+1
 


r=1     
for con in last_hour_problematic['concat']:
   r=r+1
   print(r)
   fluct=1
   while(fluct==1 ):     
                 
       n=last_hour_problematic['Age'][last_hour_problematic['concat']==con].values[0]
       fluct=0
       temp_df=total_hours_problematic[total_hours_problematic['concat']==con]
       #stats_time=stats[stats['concat']==con]['Time'].drop_duplicates()

       if (n+2 in list(temp_df['rank'])) & (n<len(stats_time)-1) : #we need at least 3 hours recoverying to close the aging
           last_hour_problematic['Age'][last_hour_problematic['concat']==con]=last_hour_problematic['Age'][last_hour_problematic['concat']==con]+2

           fluct=1

       elif (n+3 in list(temp_df['rank'])) & (n<len(stats_time)-2) :
           last_hour_problematic['Age'][last_hour_problematic['concat']==con]=last_hour_problematic['Age'][last_hour_problematic['concat']==con]+3

           fluct=1

       if fluct==1:
           cnt=last_hour_problematic['Age'][last_hour_problematic['concat']==con].values[0]+1
           while cnt in list(temp_df['rank']):
               last_hour_problematic['Age'][last_hour_problematic['concat']==con]=last_hour_problematic['Age']+1
               cnt=cnt+1

          
#%% resukt gathering

Output=last_hour_problematic


Output=Output.drop_duplicates()

Output['Tech']=Output['KPI'].str[:2]
Output['Tech'][Output['KPI']=='4G_TDD_Cell_Avail_sys_IR(%)']='TDD'
#exclude the cases with last stats on 
today = datetime.date.today()
last_day = today - datetime.timedelta(days=2)
Output=Output[Output['Time'].dt.date>last_day]


Down_Avail_target=0
Output['Category']='Low Availability'
Output['Category'][Output['Value']<=Down_Avail_target]='Down Site'



#-----------adding Delay per day
Tracker=pd.read_excel(r'Tracker.xlsx') #['SITE', 'PROVINCE', 'Vendor', 'Region', 'KPI', 'Delay']

Output=pd.merge(Output, Tracker, on=['SITE', 'KPI'], how='left')
Output['Delay']=Output['Delay'].replace(np.nan,0)
Output['Delay']=Output['Delay']+1

Output[['SITE','KPI','Delay']].to_excel(r'Tracker.xlsx', index=False)
#---------------

Delay_Day_Threshold=3
Output['Delay']=np.floor(Output['Delay']/2)

Output=Output[['Time', 'SITE', 'PROVINCE', 'Vendor', 'Region', 'KPI', 'Value',
       'Total_Tech_#', 'Problematic_Tech_#', 'Age','Category', 'Tech', 
        'Site Payload Loss Score','Delay']]
Output=Output.drop_duplicates()




Output_=copy.copy(Output)

Output_['Age'][Output_['Age']>=len(stats_time)-10]='At least '+Output_['Age'].astype(str) #'At least {0}'.format(Output_['Age'].value.astype(str))


Output_=Output_.sort_values(by='Time', ascending=False) 




#%%exports
now = datetime.datetime.now().hour
if now < 14:
    now='10 AM'
else:
    now='04 PM'

with pd.ExcelWriter('Availability_degradations_{0}_{1}.xlsx'.format(today,now)) as writer:
    Output_.to_excel(writer, sheet_name='Raw Data', index=False)
    Output_[Output_['Category']=='Down Site'][['Time', 'SITE', 'PROVINCE', 'Vendor', 'Region', 'KPI', 'Value',
       'Total_Tech_#', 'Problematic_Tech_#', 'Age','Category', 'Tech','Delay','USO']].to_excel(writer, sheet_name='Down Sites', index=False)

    Output_[Output_['Category']=='Low Availability'][['Time', 'SITE', 'PROVINCE', 'Vendor', 'Region', 'KPI', 'Value',
       'Total_Tech_#', 'Problematic_Tech_#', 'Age','Category', 'Tech','Delay','USO']].to_excel(writer, sheet_name='Low Available Sites', index=False)
    
    worksheet = writer.sheets['Raw Data']  # pull worksheet object
    worksheet.active
    worksheet.set_column('A:A',18)  # set column width
    #worksheet.set_column('B:B',20)  # set column width
    worksheet.set_column('C:C',18)  # set column width
    worksheet.set_column('D:D',9)  # set column width
    worksheet.set_column('E:E',9)  # set column width
    worksheet.set_column('F:F',22)  # set column width
    worksheet.set_column('G:G',9)  # set column width
    worksheet.set_column('H:H',11)  # set column width
    worksheet.set_column('I:I',18)  # set column width
    worksheet.set_column('J:J',8)
    worksheet.set_column('K:K',16)
    worksheet.set_column('M:M',23)
   
    worksheet = writer.sheets['Down Sites']  # pull worksheet object
    worksheet.active
    worksheet.set_column('A:A',18)  # set column width
    #worksheet.set_column('B:B',20)  # set column width
    worksheet.set_column('C:C',18)  # set column width
    worksheet.set_column('D:D',9)  # set column width
    worksheet.set_column('E:E',9)  # set column width
    worksheet.set_column('F:F',22)  # set column width
    worksheet.set_column('G:G',9)  # set column width
    worksheet.set_column('H:H',11)  # set column width
    worksheet.set_column('I:I',18)  # set column width
    worksheet.set_column('J:J',8) 
    worksheet.set_column('K:K',16)

           
    worksheet = writer.sheets['Low Available Sites']  # pull worksheet object
    worksheet.active
    worksheet.set_column('A:A',18)  # set column width
    #worksheet.set_column('B:B',20)  # set column width
    worksheet.set_column('C:C',18)  # set column width
    worksheet.set_column('D:D',9)  # set column width
    worksheet.set_column('E:E',9)  # set column width
    worksheet.set_column('F:F',22)  # set column width
    worksheet.set_column('G:G',9)  # set column width
    worksheet.set_column('H:H',11)  # set column width
    worksheet.set_column('I:I',18)  # set column width
    worksheet.set_column('J:J',8) 
    worksheet.set_column('K:K',16)
    
  
   
    
writer.save()


