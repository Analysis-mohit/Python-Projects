#!/usr/bin/env python
# coding: utf-8

# In[83]:


import datetime as dt
from datetime import datetime, timedelta
from datetime import date
from dateutil.relativedelta import relativedelta
import numpy as np
import pandas as pd
import time as tm
from pymongo import MongoClient
import calendar
from tqdm import tqdm
import sqlalchemy
import warnings
import time
import gspread
import gspread_dataframe as gd
from oauth2client.service_account import ServiceAccountCredentials  
import requests
import json
import google.auth
from google.cloud import bigquery
from google.oauth2 import service_account
from functools import reduce
from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, create_engine, select, inspect, and_, or_

#from airflow.models import Variable
#usr = Variable.get ("user")
#pasw = Variable.get ("password")

from sqlalchemy import Table, Column, Integer, String, MetaData, ForeignKey, create_engine, select, inspect, and_, or_

usr='kumarmohit'
pasw='W1BbX99CjQYy'
galaxy=sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format(usr,pasw))
scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]

# credentials = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)

# gc = gspread.authorize(credentials)


# In[2]:


# df = pd.read_sql("""select distinct event_date from analytics.operator_app_view_events
# where date(event_date)>= current_date-6
# order by event_date desc""",galaxy)

# max_date = df['event_date'].max()

# max_date = pd.to_datetime(max_date)



# start_date = max_date+timedelta(days=1)


# start_date = pd.to_datetime(start_date)


# In[7]:


start_date = pd.to_datetime(dt.date.today()-timedelta(days=3))

end_date = pd.to_datetime(dt.date.today()-timedelta(days=1))


# In[8]:


start_date = start_date.strftime('%Y%m%d')
end_date = end_date.strftime('%Y%m%d')

# start_date = 20231029
# end_date = 20231104

print(start_date)
print(end_date)


# In[84]:


# In[9]:


# not consider event

notif = ('we_notification_dismiss','we_notification_receive','we_notification_foreground','we_notification_open',
    'notification_dismiss','notification_receive','notification_foreground','notification_open',
        'NotificationStatus',
        'user_notification_status','Notificav1_notification_to_apptionAction','mp_token_notification',
        'mp_load_notification_v1',
        'mp_load_notification','bottom_menu_notification','v1_notification_to_app','v1_notification_open',
        'v1_we_notification_open','v1_we_notification_receive',
    'v1_we_notification_foreground','v1_we_notification_dismiss',
         'v1_map_readjust','v1_zoom_in','v1_zoom_out','user_engagement',
         'mp_token_notification_v1','mp_change_freight_notification_v1','v1_success_notification',
         'NotificationAction','notification','v1_perf_native','v1_performance','v1_fcm_token_scheduler_call_success','v1_fcm_token_scheduler_call')


# In[10]:


from google.cloud import bigquery
#from google.cloud import *
from google.oauth2 import service_account


credentials = service_account.Credentials.from_service_account_file(
#Enter you big query json path
  filename="/Users/mohitkumar/Downloads/mohit_kumar_bq.json"
# filename="/home/ec2-user/airflow/dags/shivam_key_bq.json"
)
client = bigquery.Client(
credentials=credentials,
project=credentials.project_id,
)


# In[85]:


import redshift_connector
conn = redshift_connector.connect(
    host='redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com',
    port=5439,
    database='datalake',
    user='kumarmohit',
    password='W1BbX99CjQYy'
 )
read_sql = conn.cursor()


# In[86]:


from google.cloud import bigquery
#from google.cloud import *
from google.oauth2 import service_account


credentials = service_account.Credentials.from_service_account_file(
#Enter you big query json path
  filename="/Users/mohitkumar/Downloads/mohit_kumar_bq.json"
# filename="/home/ec2-user/airflow/dags/shivam_key_bq.json"
)
client = bigquery.Client(
credentials=credentials,
project=credentials.project_id,
)



query_string = '''select event_date,app_info_id,target_product,event_platform, app_version,event_name,
                event_action,event_category,screen_name,
                sum(total_counts) as total_rows,
                sum(case when event_name = lower(event_name) then total_counts else 0  end) as event_name_lower,
                sum(case when event_action is not null then total_counts else 0  end) as event_action_fill,
                sum(case when event_action = lower(event_action) then total_counts else 0  end) as event_action_lower,
                sum(case when event_action in ('click','view','scroll','engagae','engage','engagement') then total_counts else 0  end) as event_action_format,
                sum(case when event_category is not null then total_counts else 0  end) as event_category_fill,
                sum(case when event_category = lower(event_category) then total_counts else 0  end) as event_category_lower,
                sum(case when screen_name is not null then total_counts else 0  end) as screen_name_fill,
                sum(case when screen_name = lower(screen_name) then total_counts else 0  end) as screen_name_lower,
                sum(case when entity is not null then total_counts else 0  end) as entity_fill,
                sum(case when entity = lower(entity) then total_counts else 0  end) as entity_lower,
                sum(case when REGEXP_CONTAINS(entity, r'^\w+:[^:]*$') then total_counts else 0  end) as entity_key,
                sum(case when miscellaneous is not null then total_counts else 0  end) as miscellaneous_fill,
                sum(case when miscellaneous = lower(miscellaneous) then total_counts else 0  end) as miscellaneous_lower,
                sum(case when REGEXP_CONTAINS(miscellaneous, r'^\w+:[^:]+(::\w+:[^:]+)*$') then total_counts else 0  end) as miscellaneous_key,
                sum(case when user_code is not null then total_counts else 0  end) as user_code_fill,
                sum(case when user_code = 'LOGOUT' then total_counts else 0  end) as user_code_logout,
                sum(case when demand_id is not null then total_counts else 0  end) as demand_id_fill,
                sum(case when SAFE_CAST(demand_id AS INT64) IS NOT NULL then total_counts else 0  end) as demand_id_int,
                sum(case when device_id is not null then total_counts else 0  end) as device_id_fill,
                sum(case when op_id is not null then total_counts else 0  end) as op_id_fill,
                sum(case when event_platform is not null then total_counts else 0  end) as event_platform_fill,
                sum(case when event_platform = lower(event_platform) then total_counts else 0  end) as event_platform_lower,
                sum(case when target_product is not null then total_counts else 0  end) as target_product_fill,
                sum(case when target_product = lower(target_product) then total_counts else 0  end) as target_product_lower
                from
                (
                SELECT event_date, event_name,
                (select value.string_value FROM UNNEST(event_params)
                WHERE key = 'event_action') as event_action,
                (select value.string_value FROM UNNEST(event_params)
                WHERE key = 'event_category') as event_category,
                (select value.string_value FROM UNNEST(event_params)
                WHERE key = 'screen_name') as screen_name,
                (select value.string_value FROM UNNEST(event_params)
                WHERE key = 'entity') as entity,
                (select value.string_value FROM UNNEST(event_params)
                WHERE key = 'miscellaneous') as miscellaneous,
                (select value.string_value FROM UNNEST(user_properties)
                WHERE key = 'user_code') as user_code,
                (select params.value.string_value from UNNEST(event_params) as params WHERE  params.key = 'demand_id' ) as demand_id,
                case when 
                (select params.value.string_value from UNNEST(user_properties) as params WHERE  params.key = 'device_id' ) is null
                then (select params.value.string_value from UNNEST(user_properties) as params WHERE  params.key = 'device_Id' )
                else (select params.value.string_value from UNNEST(user_properties) as params WHERE  params.key = 'device_id' ) end as device_id,
                (select params.value.string_value from UNNEST(user_properties) as params WHERE  params.key = 'op_id' ) as op_id,
                (select params.value.string_value from UNNEST(event_params) as params WHERE  params.key = 'event_platform' ) as event_platform,
                (select params.value.string_value from UNNEST(event_params) as params WHERE  params.key = 'target_product' ) as target_product,
                app_info.id as app_info_id,
                app_info.version as app_version,count(*) as total_counts
                from `wheelseye-178710.analytics_161329258.events_*`
                where app_info.id in 
                ('com.wheelseye.consigner')
                and _TABLE_SUFFIX BETWEEN ('{}') AND ('{}')
                and event_name like 'v1_%'
                and event_name not in {}
                group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
                )
                group by 1,2,3,4,5,6,7,8,9
                '''.format(start_date,end_date, notif)

query_job = client.query(query_string)


final_data = query_job.result().to_dataframe()

final_data


# final_data = query_job.result().to_dataframe()

# final_data = final_data.replace('<NA>',0)

# final_data['app_info_id'] = np.where((final_data['app_info_id']=='com.wheelseyeuser'),'android','ios')

# final_data = final_data[final_data['app_version']!='1.0.4']

# final_data.to_pickle('Downloads/final_data_new.pkl')


# In[ ]:





# In[87]:


final_data = final_data.replace('<NA>',0)
final_data


# In[88]:


final_data = final_data[final_data['app_version']!='1.1.0 debug']


# In[89]:


final_data[final_data['event_platform']!='android']['app_version'].unique()


# In[90]:


# In[42]:


android_app = final_data[final_data['event_platform']=='android'].groupby(['app_version'])['total_rows'].sum().reset_index()
android_app


# In[91]:


# In[43]:


android_app


# In[92]:


android_app = android_app.drop(index=0)


# In[93]:


android_app['app_version'] = android_app['app_version']


# In[94]:


android_app


# In[95]:


# In[45]:


android_app


# In[46]:


app_latest = android_app.iloc[7:8]

app_latest


# In[96]:


# In[47]:


android_app = android_app[~android_app['app_version'].isin(app_latest['app_version'])].sort_values(by='total_rows',ascending=False).reset_index(drop=True)


# In[97]:


android_app


# In[98]:


# In[48]:


last_app = android_app.iloc[0:9]

last_app


# In[99]:


# In[49]:


app_included = pd.concat([app_latest,last_app],ignore_index=True)


# In[100]:


# In[50]:


app_included


# In[101]:


# In[51]:


app_included = app_included.sort_values(by='app_version', ascending=False)


# In[52]:


# In[102]:


app_included['row_number'] = range(0, len(app_included))


# In[103]:


# In[53]:


app_included['row_number'] = 'app_latest_' + app_included['row_number'].astype(str)


# In[54]:


# In[104]:


bt_app = pd.read_sql("""select app_version, min(date(event_date)) as release_date from consigner_app_click_events
where app_version in {}
group by 1""".format(tuple(app_included['app_version'])),conn)


# In[105]:


# In[27]:

bt_app_cr = pd.read_sql("""select app_version, min(date(event_date)) as release_date from consigner_app_events_current
where app_version in {}
group by 1""".format(tuple(app_included['app_version'])),conn)


# In[106]:


# In[57]:


bt_app = pd.concat([bt_app,bt_app_cr],ignore_index=True)
bt_app


# In[107]:


# In[29]:


bt_app['release_date'] = pd.to_datetime(bt_app['release_date'])


# In[108]:


# In[60]:


bt_app = bt_app.sort_values(by='release_date').drop_duplicates(subset ='app_version',keep='first').reset_index(drop=True)


# In[109]:


# In[102]:


bt_app['app_version_1'] = bt_app['app_version'].astype(str) + " (Release date- "+bt_app['release_date'].astype(str)+")"


# In[110]:


# In[103]:


app_included = pd.merge(app_included,bt_app,on='app_version',how='left')


# In[111]:


app_included = app_included.sort_values(by='release_date', ascending=False)


# In[112]:


app_included


# In[113]:


app_included


# In[114]:


# In[62]:

android_data = final_data[final_data['event_platform']=='android'].reset_index(drop=True)
android_data = pd.merge(android_data,app_included[['app_version','row_number']],how='left')


# In[115]:


android_data.loc[~android_data['event_action'].isin(['click','api','view','engagement','engage','scroll','close','Click']),'event_action']='a'


# In[116]:


# In[63]:


android_data['combined_event'] = android_data['event_name'].astype(str)  + android_data['event_action'].astype(str) + android_data['event_category'].astype(str) + android_data['screen_name'].astype(str)



# In[64]:


android_data['combined_event'].nunique()


# In[117]:


# In[65]:


columns_to_sum = ['total_rows', 'event_name_lower', 'event_action_fill',
       'event_action_lower', 'event_action_format', 'event_category_fill',
       'event_category_lower', 'screen_name_fill', 'screen_name_lower',
       'entity_fill', 'entity_lower', 'entity_key', 'miscellaneous_fill',
       'miscellaneous_lower', 'miscellaneous_key', 'user_code_fill',
       'user_code_logout', 'demand_id_fill', 'demand_id_int',
       'device_id_fill', 'event_platform_fill',
       'event_platform_lower', 'target_product_fill', 'target_product_lower']


# In[118]:


overall = android_data.groupby(['combined_event'])[columns_to_sum].sum().reset_index()


# In[119]:


overall['flag'] = 1

columns_to_divide = [ 'event_name_lower', 'event_action_fill',
       'event_action_lower', 'event_action_format', 'event_category_fill',
       'event_category_lower', 'screen_name_fill', 'screen_name_lower',
       'entity_fill', 'entity_lower', 'entity_key', 'miscellaneous_fill',
       'miscellaneous_lower', 'miscellaneous_key', 'user_code_fill',
       'user_code_logout', 'demand_id_fill', 'demand_id_int',
       'device_id_fill', 'event_platform_fill',
       'event_platform_lower', 'target_product_fill', 'target_product_lower']


# In[66]:


for col in columns_to_divide:
    new_column_name = col + '_pct'
    overall[new_column_name] = ((overall['total_rows'] - overall[col]) / overall['total_rows']*100).apply(lambda x: round(x,1))


# In[67]:


a = overall.groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'total events'})
a1 = overall[~(overall['event_name_lower_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_name in lower case'})
a2 = overall[~(overall['event_action_fill_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_action fill'})
a2a = overall[~(overall['event_action_lower_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_action in lower case'})
a3 = overall[~(overall['event_action_format_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_action in defined keys'})
a4 = overall[~(overall['event_category_fill_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_category fill'})
a5 = overall[~(overall['event_category_lower_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_category in lower case'})
a6 = overall[~(overall['screen_name_fill_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'screen_name fill'})
a7 = overall[~(overall['screen_name_lower_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'screen_name in lower case'})
a8 = overall[~(overall['user_code_fill_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'user_code fill'})
a9 = overall[~(overall['user_code_logout_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'user_code LOGOUT'})
# a10 = overall[~(overall['op_id_fill_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'op_id fill'})
a11 = overall[~(overall['demand_id_fill_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'demand_id fill'})
a12 = overall[~(overall['demand_id_int_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'demand_id in integer'})
a13 = overall[~(overall['device_id_fill_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'device_id fill'})
a14 = overall[~(overall['event_platform_fill_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_platform fill'})
a15 = overall[~(overall['event_platform_lower_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_platform in lower case'})
a16 = overall[~(overall['target_product_fill_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'target_product fill'})
a17 = overall[~(overall['target_product_lower_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'target_product in lower case'})
a18 = overall[~(overall['entity_fill_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'entity fill'})
a19 = overall[~(overall['entity_lower_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'entity in lower case'})
a20 = overall[~(overall['entity_key_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'entity in right format (key_value)'})
a21 = overall[~(overall['miscellaneous_fill_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'miscellaneous fill'})
a22 = overall[~(overall['miscellaneous_lower_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'miscellaneous in lower case'})
a23 = overall[~(overall['miscellaneous_key_pct']>=1)].groupby(['flag'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'miscellaneous in right format (key_value)'})


raw = [a,a1,a2,a2a,a3,a4,a5,a6,a7,a8,a9,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23]

overall_funnel = reduce(lambda x,y: pd.merge(x,y,on='flag', how = 'outer'),raw).fillna(0)


# In[120]:


# In[68]:


overall_app_id = android_data[android_data['row_number'].notna()].groupby(['combined_event','row_number'])[columns_to_sum].sum().reset_index().fillna(0)


# In[121]:


# In[69]:


overall_app_id


# In[122]:


# In[71]:


columns_to_divide = [ 'event_name_lower', 'event_action_fill',
       'event_action_lower', 'event_action_format', 'event_category_fill',
       'event_category_lower', 'screen_name_fill', 'screen_name_lower',
       'entity_fill', 'entity_lower', 'entity_key', 'miscellaneous_fill',
       'miscellaneous_lower', 'miscellaneous_key', 'user_code_fill',
       'user_code_logout', 'demand_id_fill', 'demand_id_int',
       'device_id_fill', 'event_platform_fill',
       'event_platform_lower', 'target_product_fill', 'target_product_lower']


# In[73]:


for col in columns_to_divide:
    new_column_name = col + '_pct'
    overall_app_id[new_column_name] = ((overall_app_id['total_rows'] - overall_app_id[col]) / overall_app_id['total_rows']*100).apply(lambda x: round(x,1))

a = overall_app_id.groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'total events'})
a1 = overall_app_id[~(overall_app_id['event_name_lower_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_name in lower case'})
a2 = overall_app_id[~(overall_app_id['event_action_fill_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_action fill'})
a2a = overall_app_id[~(overall_app_id['event_action_lower_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_action in lower case'})
a3 = overall_app_id[~(overall_app_id['event_action_format_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_action in defined keys'})
a4 = overall_app_id[~(overall_app_id['event_category_fill_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_category fill'})
a5 = overall_app_id[~(overall_app_id['event_category_lower_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_category in lower case'})
a6 = overall_app_id[~(overall_app_id['screen_name_fill_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'screen_name fill'})
a7 = overall_app_id[~(overall_app_id['screen_name_lower_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'screen_name in lower case'})
a8 = overall_app_id[~(overall_app_id['user_code_fill_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'user_code fill'})
a9 = overall_app_id[~(overall_app_id['user_code_logout_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'user_code LOGOUT'})
# a10 = overall_app_id[~(overall_app_id['op_id_fill_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'op_id fill'})
a11 = overall_app_id[~(overall_app_id['demand_id_fill_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'demand_id fill'})
a12 = overall_app_id[~(overall_app_id['demand_id_int_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'demand_id in integer'})
a13 = overall_app_id[~(overall_app_id['device_id_fill_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'device_id fill'})
a14 = overall_app_id[~(overall_app_id['event_platform_fill_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_platform fill'})
a15 = overall_app_id[~(overall_app_id['event_platform_lower_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'event_platform in lower case'})
a16 = overall_app_id[~(overall_app_id['target_product_fill_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'target_product fill'})
a17 = overall_app_id[~(overall_app_id['target_product_lower_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'target_product in lower case'})
a18 = overall_app_id[~(overall_app_id['entity_fill_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'entity fill'})
a19 = overall_app_id[~(overall_app_id['entity_lower_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'entity in lower case'})
a20 = overall_app_id[~(overall_app_id['entity_key_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'entity in right format (key_value)'})
a21 = overall_app_id[~(overall_app_id['miscellaneous_fill_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'miscellaneous fill'})
a22 = overall_app_id[~(overall_app_id['miscellaneous_lower_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'miscellaneous in lower case'})
a23 = overall_app_id[~(overall_app_id['miscellaneous_key_pct']>=1)].groupby(['row_number'])['combined_event'].nunique().reset_index().rename(columns={'combined_event':'miscellaneous in right format (key_value)'})


raw = [a,a1,a2,a2a,a3,a4,a5,a6,a7,a8,a9,a11,a12,a13,a14,a15,a16,a17,a18,a19,a20,a21,a22,a23]

app_id_funnel = reduce(lambda x,y: pd.merge(x,y,on='row_number', how = 'outer'),raw).fillna(0)

app_id_funnel.rename(columns={'row_number':'flag'},inplace=True)

final_df = pd.concat([overall_funnel,app_id_funnel],ignore_index=True)


# In[74]:


final_df


# In[123]:


# In[75]:


columns_to_divide = ['event_name in lower case', 'event_action fill', 'event_category fill',
       'screen_name fill', 'user_code fill', 'user_code LOGOUT', 'demand_id fill',
        'device_id fill', 'event_platform fill', 'target_product fill', 'entity fill', 'miscellaneous fill']

for col in columns_to_divide:
    new_column_name = col + '_pct'
    final_df[new_column_name] = (final_df[col] / final_df['total events']*100).apply(lambda x: round(x,1)).astype(str)+'%'



# In[76]:


app_included = app_included[['row_number','app_version','app_version_1']].rename(columns={'row_number':'flag'})


# In[77]:


# In[124]:


app_included


# In[125]:


app_included['release_date'] = pd.to_datetime(app_included['app_version_1'].str.extract(r'Release date- (\d{4}-\d{2}-\d{2})')[0])

# Sort DataFrame by release date in descending order
app_included = app_included.sort_values(by='release_date', ascending=False)

# Reset index
app_included.reset_index(drop=True, inplace=True)

# Reassign 'flag' column based on the sorted order
app_included['flag'] = ['app_latest_' + str(i) for i in range(len(app_included))]

print(app_included[['flag', 'app_version', 'app_version_1']])


# In[126]:


app_included


# In[ ]:





# In[ ]:





# In[127]:


# In[78]:


final_df['event_action in lower case_pct'] = (final_df['event_action in lower case']/final_df['event_action fill']*100).apply(lambda x: round(x,1)).astype(str)+'%'
final_df['event_action in defined keys_pct'] = (final_df['event_action in defined keys']/final_df['event_action fill']*100).apply(lambda x: round(x,1)).astype(str)+'%'
final_df['event_category in lower case_pct'] = (final_df['event_category in lower case']/final_df['event_category fill']*100).apply(lambda x: round(x,1)).astype(str)+'%'
final_df['screen_name in lower case_pct'] = (final_df['screen_name in lower case']/final_df['screen_name fill']*100).apply(lambda x: round(x,1)).astype(str)+'%'
final_df['demand_id in integer_pct'] = (final_df['demand_id in integer']/final_df['demand_id fill']*100).apply(lambda x: round(x,1)).astype(str)+'%'
final_df['event_platform in lower case_pct'] = (final_df['event_platform in lower case']/final_df['event_platform fill']*100).apply(lambda x: round(x,1)).astype(str)+'%'
final_df['target_product in lower case_pct'] = (final_df['target_product in lower case']/final_df['target_product fill']*100).apply(lambda x: round(x,1)).astype(str)+'%'
final_df['entity in lower case_pct'] = (final_df['entity in lower case']/final_df['entity fill']*100).apply(lambda x: round(x,1)).astype(str)+'%'
final_df['entity in right format (key_value)_pct'] = (final_df['entity in right format (key_value)']/final_df['entity fill']*100).apply(lambda x: round(x,1)).astype(str)+'%'
final_df['miscellaneous in lower case_pct'] = (final_df['miscellaneous in lower case']/final_df['miscellaneous fill']*100).apply(lambda x: round(x,1)).astype(str)+'%'
final_df['miscellaneous in right format (key_value)_pct'] = (final_df['miscellaneous in right format (key_value)']/final_df['miscellaneous fill']*100).apply(lambda x: round(x,1)).astype(str)+'%'


# In[128]:


# In[79]:


final_summary = final_df[['flag', 'total events', 'event_name in lower case_pct', 'event_action fill_pct', 'event_action in lower case_pct', 'event_action in defined keys_pct',
    'event_category fill_pct', 'event_category in lower case_pct',
    'screen_name fill_pct', 'screen_name in lower case_pct',
    'user_code fill_pct', 'user_code LOGOUT_pct', 
    'demand_id fill_pct','demand_id in integer_pct',
    'device_id fill_pct', 'event_platform fill_pct', 'event_platform in lower case_pct',
    'target_product fill_pct', 'target_product in lower case_pct',
    'entity fill_pct', 'entity in right format (key_value)_pct',
    'miscellaneous fill_pct', 'miscellaneous in right format (key_value)_pct']]


# In[129]:


# In[80]:


final_summary = pd.merge(final_summary,app_included,on='flag',how='left')


# In[130]:


# In[117]:


final_summary.loc[final_summary['flag']==1,'app_version_1']='overall'


# In[ ]:





# In[131]:


# In[81]:


final_summary = final_summary[['app_version_1', 'total events', 'event_name in lower case_pct','event_action fill_pct',
       'event_action in lower case_pct', 'event_action in defined keys_pct',
       'event_category fill_pct', 'event_category in lower case_pct',
       'screen_name fill_pct', 'screen_name in lower case_pct',
       'user_code fill_pct', 'user_code LOGOUT_pct',
       'demand_id fill_pct', 'demand_id in integer_pct',
       'device_id fill_pct', 'event_platform fill_pct',
       'event_platform in lower case_pct', 'target_product fill_pct',
       'target_product in lower case_pct', 'entity fill_pct',
       'entity in right format (key_value)_pct', 'miscellaneous fill_pct',
       'miscellaneous in right format (key_value)_pct']]


# In[119]:


final_summary.rename(columns={'app_version_1':'app_version','total events':'total_events',
                              'demand_id fill_pct':'demand_id fill_pct_optional'
                               ,'entity fill_pct':'entity fill_pct_optional'},inplace=True)

final_summary = final_summary.T.reset_index()

new_header = final_summary.iloc[0]
final_summary = final_summary[1:]
final_summary.columns = new_header

final_summary['key_name'] = final_summary['app_version'].str.split(' ').str[0]

final_summary['metrics'] = final_summary['app_version'].str.split(' ', n=1).str[1]

final_summary = final_summary.drop(['app_version'],axis=1)

final_summary = final_summary.set_index(['key_name','metrics']).reset_index()

final_summary = final_summary.fillna('--')


# In[82]:


final_summary


# In[60]:


### Issue list attachment



# In[61]:


columns_to_sum = ['total_rows', 'event_name_lower', 'event_action_fill',
       'event_action_lower', 'event_action_format', 'event_category_fill',
       'event_category_lower', 'screen_name_fill', 'screen_name_lower',
       'entity_fill', 'entity_lower', 'entity_key', 'miscellaneous_fill',
       'miscellaneous_lower', 'miscellaneous_key', 'user_code_fill',
       'user_code_logout', 'demand_id_fill', 'demand_id_int','event_platform_fill',
       'event_platform_lower', 'target_product_fill', 'target_product_lower']


# In[62]:


issue_data = android_data.groupby(['target_product','event_platform','row_number','event_name','event_action','event_category','screen_name'])[columns_to_sum].sum().reset_index()


# In[63]:


columns_to_divide = [ 'event_name_lower', 'event_action_fill',
       'event_action_lower', 'event_action_format', 'event_category_fill',
       'event_category_lower', 'screen_name_fill', 'screen_name_lower',
       'entity_fill', 'entity_lower', 'entity_key', 'miscellaneous_fill',
       'miscellaneous_lower', 'miscellaneous_key', 'user_code_fill',
       'user_code_logout', 'demand_id_fill', 'demand_id_int',
       'event_platform_fill',
       'event_platform_lower', 'target_product_fill', 'target_product_lower']

for col in columns_to_divide:
    new_column_name = col + '_pct'
    issue_data[new_column_name] = (issue_data[col] / issue_data['total_rows']*100).apply(lambda x: round(x,1)).astype(int)


# In[64]:


if 'issue_type' not in issue_data.columns:
    issue_data['issue_type'] = ''


# In[65]:


issue_data.loc[issue_data['event_name_lower_pct'] < 99, 'issue_type'] += 'event name not in lower case,'
issue_data.loc[(issue_data['event_action_fill_pct'] < 99) | (issue_data['event_action_lower_pct'] < 99) | (issue_data['event_action_format_pct'] < 99), 'issue_type'] += 'issue in event_action,'
issue_data.loc[(issue_data['event_category_fill_pct'] >= 99)&(issue_data['event_category_lower_pct'] < 99), 'issue_type'] += 'event category not in lower case,'
issue_data.loc[issue_data['screen_name_fill_pct'] < 99, 'issue_type'] += 'screen name missing,'
issue_data.loc[(issue_data['screen_name_fill_pct'] >= 99)&(issue_data['screen_name_lower_pct'] < 99), 'issue_type'] += 'screen name not in lower case,'
# issue_data.loc[issue_data['entity_lower'] < issue_data['entity_fill'], 'issue_type'] += 'entity not in lower case,'
issue_data.loc[(issue_data['entity_fill_pct']>=90)&(issue_data['entity_key_pct'] < 95), 'issue_type'] += 'entity key value is not in right format,'
# issue_data.loc[issue_data['miscellaneous_lower'] < issue_data['miscellaneous_fill'], 'issue_type'] += 'miscellaneous key value is not in lower case,'
issue_data.loc[(issue_data['miscellaneous_fill_pct'] >= 95) & (issue_data['miscellaneous_key_pct'] < 95), 'issue_type'] += 'miscellaneous key value is not in right format,'
issue_data.loc[issue_data['user_code_fill_pct'] < 80, 'issue_type'] += 'user_code missing,'
issue_data.loc[(issue_data['demand_id_fill_pct'] >= 95) & (issue_data['demand_id_int_pct'] < 95), 'issue_type'] += 'demand id not in numeric,'
# issue_data.loc[issue_data['device_id_fill_pct'] < 98, 'issue_type'] += 'device id not found,'
issue_data.loc[issue_data['event_platform_fill_pct'] < 99, 'issue_type'] += 'event_platform missing,'
issue_data.loc[issue_data['event_platform_lower_pct'] < 99, 'issue_type'] += 'event_platform not in lower case,'
issue_data.loc[issue_data['target_product_fill_pct'] < 99, 'issue_type'] += 'target product missing,'
issue_data.loc[issue_data['target_product_lower_pct'] < 99, 'issue_type'] += 'target product not in lower case,'


# In[66]:


issue_data1 = issue_data[issue_data['issue_type'] != ''].sort_values(by='row_number').drop_duplicates(subset=['event_name', 'event_action', 'event_category', 'screen_name'], keep='first').reset_index(drop=True)


# In[67]:


issue_data1 = issue_data1[['target_product','row_number','event_name','event_action','event_category','screen_name','issue_type']]


# In[68]:


issue_data1.rename(columns={'row_number':'flag'},inplace=True)


# In[69]:


issue_data1 = pd.merge(issue_data1,app_included,on='flag',how='left')


# In[70]:


issue_data1 = issue_data1[['target_product','app_version','event_name','event_action','event_category','screen_name','issue_type']]


# In[71]:


issue_data1['issue_type'] = issue_data1['issue_type'].str.rstrip(',')


# In[72]:


if len(issue_data1)>1:
    
    issue_data1[['app_version','event_name','event_action','event_category','screen_name','target_product','issue_type']].drop_duplicates().to_csv('/Users/mohitkumar/Downloads/overall_event_issue_list.csv',index=False)
    
else:
    blank_data = pd.DataFrame(columns=['app_version', 'event_name', 'event_action', 'event_category', 'screen_name', 'target_product', 'issue_type'])
    
    blank_data.to_csv('/Users/mohitkumar/Downloads/overall_event_issue_list.csv', index=False)


# In[73]:


def build_table(data, color, text_align, font_size, width):
    table_html = '<table style="text-align: ' + text_align + '; font-size: ' + font_size + '; width: ' + width + ';" class="dataframe ' + color + '">'
    
    # Add the table headers
    table_html += '<tr>'
    for col in data.columns:
        table_html += '<th>' + str(col) + '</th>'
    table_html += '</tr>'
    
    for index, row in data.iterrows():
        table_html += '<tr>'
        for col_index, col in enumerate(data.columns):
            value = row[col]
            # Check for rows that need highlighting (index 1, 14, 15, and 9)
            if index in [1, 2, 3, 4, 6, 7, 8, 9, 10, 11, 12, 14, 16, 18, 19, 21] and '%' in str(value) and float(value.strip('%')) < 99:
                value = '<span class="highlight">' + str(value) + '</span>'
            if index in [19, 20, 21] and col_index == 0 and value in ['operator_code', 'device_id']:
                value = '<span class="highlight-yellow">' + str(value) + '</span>'
            ## optional row index number & column index number
            if index in [5, 13, 15, 17] and col_index == 1 and 'optional' in str(value).lower():
                value = '<span class="highlight-magenta">' + str(value) + '</span>'
            if col_index < 2:  # Make the first two columns bold
                value = '<b>' + str(value) + '</b>'
            table_html += '<td>' + str(value) + '</td>'
        table_html += '</tr>'
    
    table_html += '</table>'
    return table_html


# In[74]:


html_table1 = build_table(final_summary, 'blue_dark', text_align='center', font_size='11px', width='auto')


# In[75]:


css_style = '''
<style>
.dataframe.blue_dark {
    border-collapse: collapse;
    width: 100%;
}

.dataframe.blue_dark td, .dataframe.blue_dark th {
    border: 1px solid #ddd;
    padding: 8px;
    text-align: center;
}

.dataframe.blue_dark th {
    background-color: #f2f2f2;
}

/* Define a class to highlight values in red */
.highlight {
    background-color: red;
    color: white; /* To make text visible on the red background */
}

/* Define a class to highlight values in yellow */
.highlight-yellow {
    background-color: yellow;
}

/* Define a class to highlight values in light blue */
.highlight-light-blue {
    background-color: lightblue;
    color: white; /* To make text visible on the light blue background */
}

/* Define alternating row colors */
.dataframe.blue_dark tr:nth-child(even) {
    background-color: white;
}

.dataframe.blue_dark tr:nth-child(odd) {
    background-color: lightgrey;
}
</style>
'''


# In[77]:


## Final Overall Summary

final_content = f'''
<!DOCTYPE html>
<html>
<head>
    {css_style}
</head>
<body>
    <h3> Overall Summary </h3>
    {html_table1}
    <br>
    Note:
    <ol>
        <li> Only v1 events are considered except notifications. </li>
        <li> The summary below is based on unique event combinations (event_name, event_action, event_category, and screen_name).</li>
        <li> This summary relates the number of events with the correct value or format for that key matrix to the total number of events except user properties (highlighted with yellow colour).</li>
        <li> "total_events" - unique event combinations of event_name, event_action, event_category, and screen_name. </li>
        <li> "in lower case_pct" - % of event keys are in lower case. </li>
        <li> "fill_pct" - % of events have value found. Like - operator code passing in 99% of events. </li>
        <li> "in defined keys_pct" - % of events have defined keys (like event action defined keys - click, view, scroll & engage)e. </li>
        <li> "LOGOUT_pct" - % of events have found operator_code "LOGOUT" value </li>
        <li> "in integer_pct" - % of events have vehicle_id in interger format. </li>
        <li> "in right format (key_value)_pct" - % of events have keys value in right key-value pair format, example- key1:value1::key2:value2 . </li>
        <li> "user properties metrics" are divided by the total events data. </li>
        <li> The value which is in red color must be greater than 99%.</li>
        <li> Other target product summary for events where the target product is blank or different from (generic, gps, fastag, marketplace, expense, buy_sell, document & vehicle score).</li>
    </ol>
    
    
    <br><br>
    <h4> Regards </h4>
    <h4> Mohit Kumar </h4>
</body>
</html>
'''


# In[78]:


from IPython.display import HTML

# Display the HTML content
HTML(final_content)


# In[80]:


import imaplib
import email
from email.message import EmailMessage
from smtplib import SMTPException
import smtplib 
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders

SENDER_ADDRESS = "mohit.kumar@wheelseye.com"
SENDER_PASSWORD = "uncygtqiplrtoare"
# RECEIVER_ADDRESS = ['mohit.kumar@wheelseye.com']
RECEIVER_ADDRESS = ['mohit.kumar@wheelseye.com','jeevan.gupta@wheelseye.com','ajit.jangra@wheelseye.com','arjun.kotwal@wheelseye.com',
                    'ankit.surekha@wheelseye.com','rahul.gupta@wheelseye.com','tarun.tanmay@wheelseye.com','animesh.gautam@wheelseye.com'
                    ,'shankar.majhi@wheelseye.com','rohan.pardeshi@wheelseye.com','vaibhav.b@wheelseye.com','sahil.bansal@wheelseye.com']
msg = EmailMessage()
msg['Subject'] = "Consigner Android App Events QA (from_date - {} to {})".format(str(pd.Timestamp(start_date).date()),str(pd.Timestamp(end_date).date()))
msg['From'] = SENDER_ADDRESS
msg['To'] = ', '.join(RECEIVER_ADDRESS)

msg.add_alternative(final_content, subtype='html')

csv_filename = '/Users/mohitkumar/Downloads/overall_event_issue_list.csv'
attachment = open(csv_filename, 'rb')
part = MIMEBase('application', 'octet-stream')
part.set_payload(attachment.read())
encoders.encode_base64(part)
part.add_header('Content-Disposition', 'attachment; filename="overall_event_issue_list.csv"')
msg.attach(part)

with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
    smtp.login(SENDER_ADDRESS, SENDER_PASSWORD)
    smtp.send_message(msg)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:


# In[83]:


## Defined build table

# In[86]:


def build_table(data, color, text_align, font_size, width):
    table_html = '<table style="text-align: ' + text_align + '; font-size: ' + font_size + '; width: ' + width + ';" class="dataframe ' + color + '">'
    
    # Add the table headers
    table_html += '<tr>'
    for col in data.columns:
        table_html += '<th>' + str(col) + '</th>'
    table_html += '</tr>'
    
    for index, row in data.iterrows():
        table_html += '<tr>'
        for col_index, col in enumerate(data.columns):
            value = row[col]
            # Check for rows that need highlighting (index 1, 14, 15, and 9)
            if index in [1, 2, 3, 4, 6, 7, 8, 9, 12, 13, 14, 15, 16, 17, 19, 21] and '%' in str(value) and float(value.strip('%')) < 99:
                value = '<span class="highlight">' + str(value) + '</span>'
            if col_index < 2:  # Make the first two columns bold
                value = '<b>' + str(value) + '</b>'
            table_html += '<td>' + str(value) + '</td>'
        table_html += '</tr>'
    
    table_html += '</table>'
    return table_html


# # In[87]:

# convert to html table


html_table1 = build_table(final_summary, 'blue_dark', text_align='center', font_size='11px', width='auto')


# In[84]:


css_style = '''
<style>
.dataframe.blue_dark {
    border-collapse: collapse;
    width: 100%;
}

.dataframe.blue_dark td, .dataframe.blue_dark th {
    border: 1px solid #ddd;
    padding: 8px;
    text-align: center;
}

.dataframe.blue_dark th {
    background-color: #f2f2f2;
}

/* Define a class to highlight values in red */
.highlight {
    background-color: red;
    color: white; /* To make text visible on the red background */
}

/* Define alternating row colors */
.dataframe.blue_dark tr:nth-child(even) {
    background-color: white;
}

.dataframe.blue_dark tr:nth-child(odd) {
    background-color: lightgrey;
}

</style>
# '''


# In[85]:


# In[89]:


## Final Overall Summary

final_content = f'''
<!DOCTYPE html>
<html>
<head>
    {css_style}
</head>
<body>
    <h3> Overall Summary </h3>
    {html_table1}
    <br>
    Note:
    <ol>
        <li> Only v1 events are considered except notifications. </li>
        <li> The summary below is based on unique event combinations (event_name, event_action, event_category, and screen_name).</li>
        <li> This summary relates the number of events with the correct value or format for that key matrix to the total number of events.</li>
        <li> "total_events" - unique event combinations of event_name, event_action, event_category, and screen_name. </li>
        <li> "in lower case_pct" - % of event keys are in lower case. </li>
        <li> "fill_pct" - % of events have value found. Like - user code passing in 99% of events. </li>
        <li> "in defined keys_pct" - % of events have defined keys (like event action defined keys - click, view, scroll & engage)e. </li>
        <li> "LOGOUT_pct" - % of events have found user_code "LOGOUT" value </li>
        <li> "in integer_pct" - % of events have demand_id in interger format. </li>
        <li> "in right format (key_value)_pct" - % of events have keys value in right key-value pair format, example- key1:value1::key2:value2 . </li>
        <li> The value which is in red color must be greater than 99%.</li>    
    </ol>
    
    
    <br><br>
    <h4> Regards </h4>
    <h4> Mohit Kumar </h4>
</body>
</html>
'''


# In[90]:


from IPython.display import HTML

# Display the HTML content
HTML(final_content)


# In[86]:


# Mail Sent

# In[103]:


# In[ ]:


import imaplib
import email
from email.message import EmailMessage
from smtplib import SMTPException
import smtplib 

SENDER_ADDRESS = "mohit.kumar@wheelseye.com"
SENDER_PASSWORD = "wbvdlfuvnwavzptv"

RECEIVER_ADDRESS = ['jeevan.gupta@wheelseye.com','ajit.jangra@wheelseye.com','rohan.agarwal@wheelseye.com',
                    'shivam.k@wheelseye.com','arjun.kotwal@wheelseye.com','ankit.surekha@wheelseye.com',
                    'rahul.gupta@wheelseye.com','tarun.tanmay@wheelseye.com ','animesh.gautam@wheelseye.com ',
                    'shankar.majhi@wheelseye.com','rohan.pardeshi@wheelseye.com ','vaibhav.b@wheelseye.com '] 

msg = EmailMessage()
msg['Subject'] = "Consigner android App Events QA (Week - {} to {})".format(str(pd.Timestamp(start_date).date()),str(pd.Timestamp(end_date).date()))
msg['From'] = SENDER_ADDRESS
msg['To'] = ', '.join(RECEIVER_ADDRESS)

msg.add_alternative(final_content, subtype='html')

with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
    smtp.login(SENDER_ADDRESS, SENDER_PASSWORD)
    smtp.send_message(msg)


# In[ ]:

