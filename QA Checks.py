# -*- coding: utf-8 -*-
"""
Created on Thu Mar 26 18:22:29 2020

@author: Arun
"""

   
    # ================= 2nd Method ======================
    
    
    
import pandas as pd

QA_df=pd.DataFrame(columns=['Root','Root_CR','Root_MD','Dir','File','File_CR','File_MD'])
QA_df.astype('object').dtypes

import os
from os.path import join, getsize, getctime, getmtime
from datetime import datetime
root1=r'C:\\Users\\Arun\\Desktop\\QA Folder'
for i in os.walk('C:\\Users\\Arun\\Desktop\\QA Folder'):
    thisdict = {
  "Root": i[0],
  "Root_CR": datetime.fromtimestamp(getctime(i[0])).strftime('%Y-%m-%d %H:%M:%S'),
  "Root_MD": datetime.fromtimestamp(getmtime(i[0])).strftime('%Y-%m-%d %H:%M:%S'),
  "Dir": i[1],
  "File": i[2]
  }
    QA_temp=pd.DataFrame([thisdict])
    QA_df=QA_df.append(QA_temp,ignore_index=True)
    print(thisdict)
    
# Transforming above dataframe 


    
df = QA_df
s = df.apply(lambda x: pd.Series(x['File']), axis=1).stack().reset_index(level=1, drop=True)
s.name = 'File'
df2 = df.drop('File', axis=1).join(s)
df2    


# datetime.fromtimestamp(getctime(i[0])).strftime('%Y-%m-%d %H:%M:%S')
for i,j in df2.iterrows():
    df2['File Path']=df2['Root']+'\\'+df2['File']
for i,j in df2.iterrows():
    if pd.notnull(j['File Path']):
        print(j['File Path'])
        # print(df2['File Path']==j['File Path'])
        # df2.loc[(df2['File Path']==j['File Path']),'File_CR']='Yes'
        df2.loc[(df2['File Path']==j['File Path']),'File_CR']=datetime.fromtimestamp(getctime(j['File Path'])).strftime('%Y-%m-%d %H:%M:%S')
        df2.loc[(df2['File Path']==j['File Path']),'File_MD']=datetime.fromtimestamp(getmtime(j['File Path'])).strftime('%Y-%m-%d %H:%M:%S')
    


#Creating list of campaigns that has been deployed/ to be checked 
        
import datetime        
deployed={"ID":[1981,1983],
          "Dep_date":[datetime.date(2020, 3, 30), datetime.date(2020, 3, 30)]    
    
    }

import pandas as pd 
dep=pd.DataFrame(deployed,index=None)


# Creating Cartesian Product to see available QA 

dep['key']=1

df2['key']=1


cart=pd.merge(df2,dep,on='key').drop('key',axis=1) #dropping extra key column




# cart['Root'].str.find(str(cart['ID'][3]))

# pattern = '|'.join(str(cart['ID']))





temp=[]
for x,y in cart.iterrows():
    temp.append(cart['Root'][x].find(str(cart['ID'][x])))
    
temp=pd.DataFrame(pd.Series(temp),index=None,columns=['Value'])



# Merge two Dataframes on index of both the dataframes
mergedDf = cart.merge(temp, left_index=True, right_index=True)
    

#Transforming complaint and non complaint campaigns 






complaint=mergedDf[(mergedDf['Value']>1) & pd.notnull(mergedDf['File Path'])]

complaint=complaint.reset_index(drop=True)


list(complaint)

from datetime import datetime 

comp_list=[]
file_crt=[]
for i,x in complaint.iterrows():
    z=datetime.strptime(datetime.strftime(complaint['Dep_date'][i], '%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')
    y=datetime.strptime(complaint['File_CR'][i], '%Y-%m-%d %H:%M:%S')
    comp_list.append(z)
    file_crt.append(y)
    
    
    
complaint['Deployment_DT']=comp_list
complaint['File_CR_Dt']=file_crt

complaint['Gap']=complaint['File_CR_Dt']-complaint['Deployment_DT']


QA_offtime = complaint[complaint['Gap'].dt.total_seconds() > 30]
QA_offtime.drop(['Value'],axis=1,inplace=True)





QA_offtime=complaint[complaint['Gap'].days>0] or ((complaint['File_CR_Dt']-complaint['Deployment_DT']).seconds)/3600>0)]
                     
                     
    








'''


x=datetime.strptime(datetime.strftime(complaint['Dep_date'][2], '%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')\
    -datetime.strptime(complaint['File_CR'][2], '%Y-%m-%d %H:%M:%S')








type(complaint['File_CR'][2])

print(date_as_datetime)



print(datetime.strftime(complaint['Dep_date'][2], '%Y-%m-%d %H:%M:%S'))
print(type(datetime.strftime(complaint['Dep_date'][2], '%Y-%m-%d %H:%M:%S')))

print(datetime.strptime(complaint['File_CR'][1], '%Y-%m-%d %H:%M:%S'))
print(type(datetime.strptime(complaint['File_CR'][1], '%Y-%m-%d %H:%M:%S')))




from datetime import datetime 

x = datetime.datetime(2018, 6, 1).strftime('%Y-%m-%d %H:%M:%S')

print(type(datetime.strptime(datetime.strftime(complaint['Dep_date'][2], '%Y-%m-%d %H:%M:%S'),'%Y-%m-%d %H:%M:%S')))

print(y)
print(type(y))




date_object = datetime.date(complaint['File_CR'][2])
print(date_object)



datetime.date(complaint['File_CR'][2])>complaint['Dep_date'][2]



type(complaint['Dep_date'][2])

import datetime

datetime.date(2020, 11, 20)>datetime.date(2020, 11, 19)
    
    
    
    
    if cart['Root'][x].find(str(cart['ID'][x]))>-1:
        cart.iloc[cart[(cart['Root'][x].find(str(cart['ID'][x])))>-1].index,'Flag']='Yes'
        
        # KeyError: 'cannot use a single bool to index into setitem'
        
    
    cart.loc[0]
    # cart['Index']=cart.loc[cart['Root'].str.find(str(cart['ID'][x]))]
    
'''






























    
    
    
    to_append_root=pd.DataFrame(pd.Series(i[0],index=None),columns=['Root'])
    
    to_append_dirs=pd.DataFrame(pd.Series(i[1],index=None),columns=['Dir'])
    # to_append_files=pd.DataFrame(pd.Series(files,index=None),columns=['File'])
    QA_df=QA_df.append(to_append_root,ignore_index=True)
    QA_df=QA_df.append(to_append_dirs,ignore_index=True)
    # QA_df=QA_df.append(to_append_files,ignore_index=True)
    
    
    
    
'''
'C:\\Users\\Arun\\Desktop\\QA Folder\SP1981'

I want to split this string using '\\'

output should be tried str.split method

['C:\\Users\\Arun\\Desktop\\QA Folder','SP1981']


'''

    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    for name in dirs:
        print(name)
        print('Root folder', root, 'created on', datetime.fromtimestamp(getctime(root)).strftime('%Y-%m-%d %H:%M:%S'))
        print('Root dir', name, 'created on', (datetime.fromtimestamp(getctime(join(root,name))).strftime('%Y-%m-%d %H:%M:%S')))
        print(os.listdir(join(root,name)))
    for root, dirs, files in os.walk(join(root,name)):
        print(files)
