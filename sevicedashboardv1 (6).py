#!/usr/bin/env python
# coding: utf-8

# In[1]:


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
from oauth2client.service_account import ServiceAccountCredentials
usr='kumarmohit'
pasw='W1BbX99CjQYy'
galaxy=sqlalchemy.create_engine("postgresql+psycopg2://{}:{}@redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com:5439/datalake".format(usr,pasw))
scope = ["https://spreadsheets.google.com/feeds","https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name("mohit_bq.json", scope)
gc = gspread.authorize(credentials)




# In[2]:


import redshift_connector
conn = redshift_connector.connect(
    host='redshift-cluster-2.ct9kqx1dcuaa.ap-south-1.redshift.amazonaws.com',
    port=5439,
    database='datalake',
    user='kumarmohit',
    password='W1BbX99CjQYy'
 )
read_sql = conn.cursor()


# In[3]:


raw_data = pd.read_sql("""




with s as (select id,(case when json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'overHeight') != '' then json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'overHeight')::float else null end) as over_height,
(case when json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'overWeight') != '' then json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'overWeight') else null end) as over_weight,
(case when json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'expressDeliveryTat') != '' then json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'expressDeliveryTat')::float else null end) as express_delivery,
(case when json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'openDala') != '' then json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'openDala') else null end) as open_dala,
(case when json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'extraRope') != '' then json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'extraRope') else null end) as ex_rope,
(case when json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'extraWidth') != '' then json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'extraWidth') else null end) as ex_width,
(case when json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'extraPerson') != '' then json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'extraPerson') else null end) as ex_person,
(case when json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'helperRequired') != '' then json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'helperRequired') else null end) as helper_req,
(case when json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'dieselVehicle') != '' then json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'dieselVehicle') else null end) as diesel_veh,
(case when json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'extraTripalRequired') != '' then json_extract_path_text(json_extract_path_text(d.metadata,'specialRequestBody'),'extraTripalRequired') else null end) as extra_tirpal,
case when v.demand_id = d.id then 1 else 0 end as via_points_ind,
case when (over_height > 0 or over_weight = 'true' or express_delivery > 0 or open_dala = 'true' or ex_rope = 'true' or ex_width = 'true' or ex_person = 'true' or helper_req = 'true' or diesel_veh = 'true' or extra_tirpal = 'true' or via_points_ind = 1 ) then 1 else 0 end as sr_demand
from wfms_demands d 
left join (select demand_id, count(demand_id) as add_count from wfms_address_v2 where created >= '2023-10-01' group by 1 having add_count > 2) v on v.demand_id = d.id
where created>='2023-10-01')



(select distinct dr.demand_id,(timestamp 'epoch' + ((tnd.time + 19800) * interval '1 second')) as unloading_done_time,
                            rating,new_raw.remarks,reasons,gtl_delay,sys_trnst_dly,dr.code,sr_demand,express_delivery
            from wfms_consignments dr 
            left join wfms_consigner_demand_feedback new_raw on new_raw.demand_id = dr.demand_id
            left join wfms_demands wd on wd.id = dr.demand_id
            left join s on s.id = dr.demand_id
            left join wfms_consigners cn on cn.code = wd.consigner_user_code 
            left join (select consignment_code,state,time from wfms_consignment_state_info where state IN ('TRIP_END') AND deleted = 'false') tnd on tnd.consignment_code = dr.code
            left join (select id,json_extract_path_text(metadata,'ptl') as PTL from wfms_demands) as d on d.id = dr.demand_id  

            left join (select distinct demand_id,
                              datediff(min,elt,loading_reach_time) as g_delay,
                              case when g_delay<=1 then 'ontime' 
                                   when g_delay<=120 then 'delay_2hr'
                                   when g_delay<=240 then 'delay_2hr_to_4hr' else 'delay_4hours+' end as gtl_delay 
                                   
                      from (
                             select distinct demand_id,
                                   (timestamp 'epoch' + ((tnd.time + 19800) * interval '1 second')) as unloading_done_time,
                                   (timestamp 'epoch' + ((lod.time + 19800) * interval '1 second')) as loading_reach_time,
                                   
                                    (timestamp 'epoch' + ((json_extract_path_text(d.metadata,'appLoadingTime')::float + 19800) * interval '1 second')) as elt 
                                    from wfms_consignments wc 
                             left join wfms_demands d on d.id = wc.demand_id 
                             left join (select consignment_code,state,time from wfms_consignment_state_info where state IN ('TRIP_END') AND deleted = 'false') tnd on tnd.consignment_code = wc.code
                             left join (select consignment_code,state,time from wfms_consignment_state_info where state IN ('AT_LOADING') AND deleted = 'false') lod on lod.consignment_code = wc.code
                             where unloading_done_time>='2024-01-01')
                             group by 1,2)a on a.demand_id = dr.demand_id
            left join (select distinct demand_id,
                              (trip_jrny - cxnr_tat) as cxnr_trnst ,
                              (trip_jrny - fo_tat) as fo_trnst,
                              case when cxnr_trnst<=0 then 'ontime' 
                                   when cxnr_trnst<=24 then 'dely_undr_24' 
                                   when cxnr_trnst<=48 then 'dely_undr_48'
                                    when cxnr_trnst>48 then 'dely_abv_48' end as sys_trnst_dly
                      from (
                             select distinct demand_id,
                                    (timestamp 'epoch' + ((trn.time + 19800) * interval '1 second')) as trip_start_time,
                                    (timestamp 'epoch' + ((un.time + 19800) * interval '1 second')) as unloading_reach_time,
                                    (timestamp 'epoch' + ((te.time + 19800) * interval '1 second')) as unloading_done_time,
                                    datediff(hour,
                                    trip_start_time,
                                    unloading_reach_time) as trip_jrny,
                                    case when json_extract_path_text(d.metadata,'estimatedRouteDistance')>0 
                                    then floor(cast(json_extract_path_text(d.metadata,'estimatedRouteDistance') as float)) 
                                    else null end as trip_distance,
                                    case when json_extract_path_text(d.metadata,'shipperTATinHrs')!='' then json_extract_path_text(metadata,'shipperTATinHrs')::float 
                                    else (trip_distance/300)*24 end as cxnr_tat,
                                    case when json_extract_path_text(d.metadata,'operatorTATinHrs')!='' then json_extract_path_text(metadata,'operatorTATinHrs')::float 
                                    else  (trip_distance/400)*24 end as fo_tat
                                    from wfms_consignments dr 
                                    left join wfms_demands d on d.id = dr.demand_id 
                                    left join (select consignment_code,state,time from wfms_consignment_state_info where state IN ('IN_TRANSIT') AND deleted = 'false') trn on trn.consignment_code = dr.code
                                    left join (select consignment_code,state,time from wfms_consignment_state_info where state IN ('TRIP_END') AND deleted = 'false') te on te.consignment_code = dr.code
                                    left join (select consignment_code,state,time from wfms_consignment_state_info where state IN ('AT_UNLOADING') AND deleted = 'false') un on un.consignment_code = dr.code
                                    where unloading_done_time>='2024-01-01'
                                    group by 1,trip_jrny,cxnr_tat,fo_tat,trip_start_time,unloading_reach_time,d.metadata,unloading_done_time)
                                    
                                    group by 1,trip_jrny , cxnr_tat,fo_tat)b on b.demand_id = dr.demand_id 
            where unloading_done_time>='2024-01-01'and d.PTL != 'true' and cn.name not ilike '%%testing%%'
            order by unloading_done_time desc)""",conn)


# In[4]:


raw_data['week_date'] = raw_data['unloading_done_time'].dt.strftime('%A')  # Day of the week
raw_data['month_year'] = raw_data['unloading_done_time'].dt.strftime('%b\'%y')  # Month name
raw_data['week_start'] = raw_data['unloading_done_time'] - pd.to_timedelta(raw_data['unloading_done_time'].dt.weekday, unit='D')
raw_data['week_end'] = raw_data['week_start'] + pd.to_timedelta(6, unit='D')

# Format the week_start and week_end dates
raw_data['week_date'] = raw_data['week_start'].dt.strftime('%d-%b-%y') + ' - ' + raw_data['week_end'].dt.strftime('%d-%b-%y')
raw_data = raw_data.drop(columns=['week_start', 'week_end'])
raw_data


# In[5]:


tickets = pd.read_sql("""with aa as(select consigner_user_code, id, trip_start_time, rating_cat

    from wfms_demands wd

    left join(select * from(select demand_id,rating,case when rating<3 then 'BAD' when rating=3 then 'MODERATE' else 'GOOD' end as rating_cat,
            row_number() over(partition by demand_id order by updated desc)as frn
            from wfms_consigner_demand_feedback
        where deleted!='true')
    where frn=1)as wf on wf.demand_id=wd.id

    left join(select code, name, type as cx_type, sales_manager from wfms_consigners where deleted!='true')as wc on wc.code=wd.consigner_user_code

    left join(select id as tid, demand_id, code as trip_id, state as trip_status from wfms_consignments where state!='CANCELLED' and deleted!='true')as wcc on wcc.demand_id=wd.id
    left join(select consignment_code,(timestamp 'epoch' + (time)*interval '1 second' + interval '5h,30m') as trip_start_time
        from wfms_consignment_state_info 
    where state='TRIP_END' AND deleted!='true')as ee on ee.consignment_code=wcc.trip_id

    left join(select * from(select demand_id, remarks as ptl,
            row_number() over(partition by demand_id order by created asc)as rn
            from analytics_wfms_demand_comments
        where remarks ilike '%#wheelseye_ptl%' or remarks ilike '%wheelseye ptl%')
    where rn=1)as dc on dc.demand_id=wd.id

where status='FULFILLED' and ptl is null and json_extract_path_text(metadata,'ptl',true)!='true' and cx_type in('SME','SME_HP') and trip_start_time is not null and
    name not ilike '%alternat%' and name not ilike '%testing%' and consigner_user_code not in ('WE2558989','WE3171753','WE2866674','WE2726471','WE2402271')
),

bb as (select ticket_code, created + interval '5:30' as Ticket_Created_time, tt_dem_id, status, ticket_feedback,
    case when status='RESOLVED' then datediff('minute',Ticket_Created_time,resolve_time) else null end as resolve_dif,
    case when status!='RESOLVED' then datediff('minute',Ticket_Created_time,getdate()) else null end as opened_from,
    datediff(min,Ticket_Created_time,expected_res_time) as tat,
    case when resolve_dif<=TAT then 1 when resolve_dif is not null then 0 else null end as TAT_resolve_cat,
    
    case when status = 'ACTIVE' and opened_from>tat  then 'open_abv_tat'  end as tat_chk,
    case when status = 'ACTIVE' and opened_from>=4320 then 'opn_abv_72hrs' end as op_cat

    from tms_ticket tt

    left join(select * from(select ticket_id,timestamp 'epoch' + (date_value)*interval '1 second' + interval '5h30min' as resolve_time,
            row_number() over(partition by ticket_id)as srrn
            from tms_custom_field_value
        where deleted!='true' and custom_field_id in(36,35))
    where srrn=1)as tres on tres.ticket_id=tt.id
    
    left join (select ticket_id,date_value,(timestamp 'epoch' + ((date_value + 19800) * interval '1 second')) as expected_res_time from tms_custom_field_value
     where custom_field_id IN ('32','31')) tat on tat.ticket_id = tt.id

    left join(select * from(select ticket_id,string_value as sub_reason,
            row_number() over(partition by ticket_id)as srrn
            from tms_custom_field_value
        where deleted!='true' and custom_field_id in('26','29'))
    where srrn=1)as tsr on tsr.ticket_id=tt.id

    left join(select * from(select ticket_id,number_value as tt_dem_id,
            row_number() over(partition by ticket_id)as trn
            from tms_custom_field_value 
        where deleted!='true' and custom_field_id in(8,24))
    where trn=1)as trtt on trtt.ticket_id=tt.id

    left join(select * from(select ticket_id,string_value as ticket_feedback,
            row_number() over(partition by ticket_id order by updated desc)as srrn
            from tms_custom_field_value
        where deleted!='true' and custom_field_id in(37,38))
    where srrn=1)as tf on tf.ticket_id=tt.id

where deleted!='true' and source in('APP','App','ODIN') and subject!='Unfulfilled Demand - Operator Backout')


select * from bb
left join aa on aa.id=bb.tt_dem_id
where Ticket_Created_time>='2024-01-01' and aa.id is not null
order by Ticket_Created_time desc""",conn)


# In[6]:


tickets = tickets.rename(columns={'id': 'demand_id'})
tickets_v1 = tickets.drop(columns={'consigner_user_code','trip_start_time','rating_cat','tt_dem_id','ticket_created_time'
                                   ,'status','tat','resolve_dif','tat_chk','op_cat'})


# In[7]:


tickets


# In[8]:


gps_data = pd.read_sql("""SELECT
    code,
    vehicle_number,
    operator_code,
    trnst_time,
    trip_journey,
    gtl_to_loading_time,
    loading_to_transit_time,
    transit_to_unloading_time,
    (gtl_to_loading_time +  transit_to_unloading_time) AS total_offline_time,
    total_offline_time*1.0/nullif(trip_journey,0) as "%_offline_time",
    device_name,device_family,hardware_type,sim_network,transit_journey
FROM (
    SELECT 
        code,
        vehicle_number,
        operator_code,
        trnst_time,
        trip_journey,
        sim_network,
        device_name,device_family,hardware_type,
        transit_journey,
        SUM(gtl_to_loading_time) AS gtl_to_loading_time,
        SUM(loading_to_transit_time) AS loading_to_transit_time,
        SUM(transit_to_unloading_time) AS transit_to_unloading_time
    FROM (
        SELECT 
            c.code,
            c.vehicle_number,
            c.operator_code, 
            vh.id,
            dv.action,
            dv.from_time,
            dv.to_time
            ,device_name,device_family,hardware_type,sim_network,
            (timestamp 'epoch' + ((dv.from_time + 19800) * interval '1 second')) AS no_info_start_time,
            (timestamp 'epoch' + ((dv.to_time + 19800) * interval '1 second')) AS no_info_end_time,
            (timestamp 'epoch' + ((gtl.time + 19800) * interval '1 second')) AS gtl_time,
            (timestamp 'epoch' + ((atl.time + 19800) * interval '1 second')) AS loading_time,
            (timestamp 'epoch' + ((itr.time + 19800) * interval '1 second')) AS trnst_time,
            (timestamp 'epoch' + ((ul.time + 19800) * interval '1 second')) AS unloading_time,
            datediff(hour,gtl_time,unloading_time) as trip_journey,
            datediff(hour,trnst_time,unloading_time) as transit_journey,
            CASE WHEN no_info_start_time BETWEEN gtl_time AND loading_time AND no_info_end_time BETWEEN gtl_time AND loading_time THEN datediff(hour, no_info_start_time, no_info_end_time)
            ELSE 0 END AS gtl_to_loading_time,
            CASE WHEN no_info_start_time BETWEEN loading_time AND trnst_time AND no_info_end_time BETWEEN loading_time AND trnst_time THEN datediff(hour, no_info_start_time, no_info_end_time)
                ELSE 0 
            END AS loading_to_transit_time,
            CASE WHEN no_info_start_time BETWEEN trnst_time AND unloading_time AND no_info_end_time BETWEEN trnst_time AND unloading_time THEN datediff(hour, no_info_start_time, no_info_end_time)
                ELSE 0 
            END AS transit_to_unloading_time
        FROM 
            wfms_consignments c 
            left join wfms_demands wd on wd.id = c.demand_id 
            left join wfms_consigners cn on cn.code = wd.consigner_user_code
            LEFT JOIN (SELECT id, vehicle_number FROM ocms_vehicles) vh ON vh.vehicle_number = c.vehicle_number
            LEFT JOIN (SELECT vehicle_id,action,from_time,to_time FROM argus_device_history ) dv ON dv.vehicle_id = vh.id  
            LEFT join (SELECT vehicle_number,device_name,device_family,hardware_type,sim_network,vehicle_state from vehicle_details) vd on vd.vehicle_number = c.vehicle_number
            LEFT JOIN (SELECT consignment_code, time FROM wfms_consignment_state_info WHERE state = 'GOING_TO_LOAD' AND deleted = 'false') gtl ON gtl.consignment_code = c.code 
            LEFT JOIN (SELECT consignment_code, time FROM wfms_consignment_state_info WHERE state = 'AT_LOADING' AND deleted = 'false') atl ON atl.consignment_code = c.code 
            LEFT JOIN (SELECT consignment_code, time FROM wfms_consignment_state_info WHERE state = 'IN_TRANSIT' AND deleted = 'false') itr ON itr.consignment_code = c.code 
            LEFT JOIN (SELECT consignment_code, time FROM wfms_consignment_state_info WHERE state = 'AT_UNLOADING' AND deleted = 'false') ul ON ul.consignment_code = c.code 
        where vehicle_state = 'LIVE'
        
        
        ORDER BY 
            from_time ASC
    ) AS subquery
    GROUP BY 
        code,
        vehicle_number,
        trnst_time,
        trip_journey,device_name,device_family,hardware_type,sim_network,operator_code,transit_journey
ORDER BY  
trnst_time DESC )""",conn)


# -- SELECT vehicle_id,device_id,device_name from ocms_vehicle_devices d 
# -- SELECT * from vehicle_details


# In[9]:


gps_data = gps_data.drop(columns={'vehicle_number','operator_code','trnst_time',
                                  'gtl_to_loading_time','loading_to_transit_time',
                                    'transit_to_unloading_time','%_offline_time','device_name','hardware_type','device_family',
                                     'hardware_type'})


# In[10]:


raw_data


# In[11]:


new_raw1 = pd.merge(raw_data,tickets_v1,on='demand_id',how='left')

new_raw1

new_raw = pd.merge(new_raw1,gps_data,on='code',how='left')


# In[12]:


new_raw1


# In[13]:


dmg = pd.read_sql("""with t as (with a as (select demand_id,remarks from (select demand_id,remarks,row_number() over(partition by demand_id order by created desc) as ranker from trucking.wfms_demand_comments
          where remarks ilike '%wheelseye_ptl%') where ranker=1)

select distinct trip_end_time,wc.code as wc_code,json_extract_path_text(d.metadata,'ptl') as PTL,
case when a.remarks is not null then 'true' else 'false' end as PTL_m,wcc.type as consigner_type,wv.body_type as vehicle_demand_body_type,
d.status as demand_status
from wfms_consignments wc
left join a on a.demand_id= wc.demand_id
left join trucking.wfms_demands d on d.id=wc.demand_id
left join wfms_vehicle_types wv on wv.id = d.vehicle_type_id
left join wfms_consigners wcc on wcc.code = d.consigner_user_code
left join (select *,(timestamp 'epoch' + (time)*interval '1 second' + interval '5h,30m') as trip_end_time 
from (select consignment_code as code,row_number() over (partition by consignment_code order by updated desc) as rnk,time
                from wfms_consignment_state_info where state = 'TRIP_END' and deleted = 'false')where rnk = 1) t on t.code = wc.code
where demand_status = 'FULFILLED' 
and wc.code is not null and trip_end_time >= '04-01-2024'
and PTL!='true'
order by trip_end_time desc
),

aa as(select demand_id, trip_id, created +interval '5h30min' as damage_creation, 
    type, reason, claim_status, consigner_assessed_amount, assessed_damage_amount, final_closure_amount,
    json_extract_path_text(metadata,'items',0,'name')as items,
    json_extract_path_text(metadata,'items',1,'unit')as unit,
    json_extract_path_text(metadata,'items',2,'quantity')as quantity,
    json_extract_path_text(metadata,'proofDocCodes',true)as proofDocCodes

    from wfms_consignment_damage wcd
    
    left join(select distinct demand_id,code as trip_id from wfms_consignments where deleted!='true')as wcs on wcs.trip_id=wcd.consignment_code

where deleted!='true'),

bb as(select ticket_code, created + interval '5h30min' as ticket_creation, tt_dem_id, subject, status, source, 
    case when id is not null then 'https://odin.wheelseye.com/portal/ticketing/details?tid=' else null end as Link1_,
    Link1_ + Ticket_code as Ticket_link

    from tms_ticket tt

    left join(select * from(select ticket_id,number_value as tt_dem_id,
            row_number() over(partition by ticket_id)as trn
            from tms_custom_field_value 
        where deleted!='true' and custom_field_id in(8,24))
    where trn=1)as trtt on trtt.ticket_id=tt.id

where deleted!='true' and source in('APP','App','ODIN') and ((subject ilike '%Damage%' and subject ilike '%Shortage%') or assigned_to='damage@wheelseye.com')
),

cc as(select tt_dem_id as dupli_dem, count(distinct ticket_code)as total_tickets
    from bb
group by 1),



dmg as (select code,name,sales_manager,demand_id,trip_id,ticket_month,
ticket_month,claim_status,type,reason,consigner_assessed_amount,assessed_damage_amount,final_closure_amount,items,unit,quantity,proofDocCodes,total_tickets,multiple_ticket_cat,
max(case when source = 'APP' then ticket_code end) as app_ticket,
max(case when source = 'APP' then Ticket_link end) as app_ticket_link,
max(case when source = 'ODIN' then Ticket_link end) as odin_ticket_link,
max(case when source = 'ODIN' then ticket_code end) as ODIN_ticket,
min(ticket_creation) as ticket_creation_time

from 


(select code, name, sales_manager, demand_id, trip_id, ticket_code, ticket_creation, extract('month' from ticket_creation)as ticket_month, subject, status, source, Ticket_link, 
    type, reason, claim_status, consigner_assessed_amount, assessed_damage_amount, final_closure_amount, 
    items, unit, quantity, proofDocCodes, total_tickets,
    case when total_tickets>1 then 1 else 0 end as multiple_ticket_cat

    from bb
    left join aa on tt_dem_id=demand_id
    left join cc on tt_dem_id=dupli_dem
    left join(select consigner_user_code, id
    
        from wfms_demands) wd on aa.demand_id = wd.id
        
    left join(select code, name, sales_manager from wfms_consigners where deleted!='true')as wc on wc.code=wd.consigner_user_code
where code is not null and name not ilike '%testing%'
order by ticket_creation desc)


group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
order by ticket_creation_time desc,multiple_ticket_cat desc)

select 
date_trunc('day',trip_end_time) as daily_trip,
count(distinct(wc_code)) as total_trips,
count(distinct(case when total_tickets>=1 then wc_code end)) as Reported,
count(distinct(case when total_tickets>=1 and type is null then wc_code end)) not_identfied,
count(distinct(case when total_tickets>=1 and type is not null then wc_code end)) identfied,
count(distinct(case when total_tickets>=1 and type ='SHORT' then wc_code end)) shortage,
count(distinct(case when total_tickets>=1 and type = 'NO_DAMAGE' then wc_code end)) no_dmg,
identfied - no_dmg as actual_damage,
Reported*1.0/nullif(total_trips,0) as "%reported_dmg",
identfied*1.0/nullif(total_trips,0) as "%identfied_dmg",
not_identfied*1.0/nullif(total_trips,0) as "%pending_identification",
no_dmg*1.0/nullif(total_trips,0) as "%no_dmg_found",
actual_damage*1.0/nullif(total_trips,0) as "%actual_dmg"

from t 
left join dmg on dmg.trip_id = t.wc_code
group by 1 
order by 1 desc""",conn)


# In[ ]:





# In[14]:


runtime=dt.datetime.now()

ws1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1467923172#gid=1467923172').worksheet('dmg_raw')
gd.set_with_dataframe(ws1,dmg,row=2,col=1, resize=False,include_index=False)
ws1.update_acell('A1',str(runtime))

print(f"Done: {dt.datetime.now()}")


# In[15]:


runtime=dt.datetime.now()

ws1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1467923172#gid=1467923172').worksheet('tickets')
gd.set_with_dataframe(ws1,tickets,row=2,col=1, resize=False,include_index=False)
ws1.update_acell('A1',str(runtime))

print(f"Done: {dt.datetime.now()}")


# In[16]:


cancel_raw = pd.read_sql("""select c.demand_id,c.code,(c.created + interval '5h30m') as placement_time,c.state as consignment_state,wcsi.remarks as cancel_remarks,d.status,
case when cancel_remarks ilike '%CANCELLED_FROM_SHIPPER_DEMAND_EXPIRY%' then 'Consigner'
when cancel_remarks ilike '%CONSIGNER_ASKED_TO_CANCEL_CONSIGNMENT_CANCELLATION%' then 'Consigner'
when cancel_remarks ilike '%VEH_BREAKDOWN_CANCELLED_BY_OPERATOR%' then 'Operator'
when cancel_remarks ilike '%OP_DENIED_FREIGHT_CANCELLED_BY_OPERATOR%' then 'Operator'
when cancel_remarks ilike '%KYC_ISSUE_CONSIGNMENT_CANCELLATION%' then 'Kyc'
when cancel_remarks ilike '%VEHICLE_GETTING_DELAYED_CANCELLED_BY_CONSIGNER_V2%' then 'Operator'
when cancel_remarks ilike '%LOADING_POSTPONED_CANCELLED_BY_CONSIGNER_V2%' then 'Consigner'
when cancel_remarks ilike '%LOADING_POSTPONED_CONSIGNMENT_CANCELLATION%' then 'Consigner'
when cancel_remarks ilike '%POSTPONED_BY_SHIPPER_DEMAND_EXPIRY%' then 'Consigner'
when cancel_remarks ilike '%DRIVER_DENIED_CANCELLED_BY_CONSIGNER_V2%' then 'Operator'
when cancel_remarks ilike '%ADDRESS_CHANGE_CONSIGNMENT_CANCELLATION%' then 'Consigner'
when cancel_remarks ilike '%FOUND_VEHICLE_AT_BETTER_PRICE_CANCELLED_BY_CONSIGNER_V2%' then 'Consigner'
when cancel_remarks ilike '%VEHICLE_NOT_EMPTY_CANCELLED_BY_OPERATOR%' then 'Operator'
when cancel_remarks ilike '%DRIVER_DENIED_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%TECH_ISSUE_CONSIGNMENT_CANCELLATION%' then 'Tech'
when cancel_remarks ilike '%BLACKLISTED_OPERATOR_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%DELAYED_ARRIVAL_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%TONNAGE_HEIGHT_CHANGE_CONSIGNMENT_CANCELLATION%' then 'Consigner'
when cancel_remarks ilike '%OTHER_DEMAND_EXPIRY%' then 'Other'
when cancel_remarks ilike '%NO_RESPONSE_FROM_CUSTOMER_CONSIGNMENT_CANCELLATION%' then 'Consigner'
when cancel_remarks ilike '%ALTERNATIVE_LOAD_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%DRIVER_DENIED_CANCELLED_BY_OPERATOR%' then 'Operator'
when cancel_remarks ilike '%REPEATED_INDENT_DEMAND_EXPIRY%' then 'Other'
when cancel_remarks ilike '%PLACED_BY_MISTAKE_CONSIGNMENT_CANCELLATION%' then 'Other'
when cancel_remarks ilike '%GPS_NOT_AVAILABLE_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%GPS_NOT_AVAILABLE_CANCELLED_BY_OPERATOR%' then 'Operator'
when cancel_remarks ilike '%MATERIAL_TYPE_DIFFERENT_CONSIGNMENT_CANCELLATION%' then 'Consigner'
when cancel_remarks ilike '%VEHICLE_AT_UNLOADING_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%OP_DENIED_FREIGHT_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%VEH_BREAKDOWN_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%OVERLOAD_OVERHEIGHT_ISSUE_DEMAND_EXPIRY%' then 'Consigner'
when cancel_remarks ilike '%CONSIGNEE_BOOKED_VEHICLE_DEMAND_EXPIRY%' then 'Consigner'
when cancel_remarks ilike '%VEHICLE_NOT_EMPTY_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%RFQ_DEMAND_EXPIRY%' then 'Other'
when cancel_remarks ilike '%CREDIT_CYCLE_ISSUE_DEMAND_EXPIRY%' then 'Consigner'
when cancel_remarks ilike '%CANCELLED_AFTER_FULFILMENT_TRANSPORTER_DEMAND_EXPIRY%' then 'Consigner'
when cancel_remarks ilike '%OTHERS_CONSIGNMENT_CANCELLATION%' then 'Other'
when cancel_remarks ilike '%CANCELLED_AFTER_FULFILMENT_DEMAND_DEMAND_EXPIRY%' then 'Consigner'
when cancel_remarks ilike '%LESS_PLACEMENT_TIME_DEMAND_EXPIRY%' then 'Other'
when cancel_remarks ilike '%LACK_OF_MATERIAL_WITH_TRANSPORTER_DEMAND_EXPIRY%' then 'Consigner'
when cancel_remarks ilike '%BOOKED_ANOTHER_VECHICLE_DEMAND_EXPIRY%' then 'Consigner'
when cancel_remarks ilike '%MATERIAL_NOT_READY_DEMAND_EXPIRY%' then 'Consigner'
when cancel_remarks ilike '%ALTERNATIVE_LOAD_CANCELLED_BY_OPERATOR%' then 'Operator'
when cancel_remarks ilike '%DELAYED_COMMUNICATION_DEMAND_EXPIRY%' then 'Other'
when cancel_remarks ilike '%VIA_POINT_ADDED_CONSIGNMENT_CANCELLATION%' then 'Consigner'
when cancel_remarks ilike '%DATA_ENTRY_MISTAKE_DEMAND_EXPIRY%' then 'Other'
when cancel_remarks ilike '%OPERATOR_BACKOUT_DEMAND_EXPIRY%' then 'Operator'
when cancel_remarks ilike '%UNFIT_VEHICLE_DEMAND_EXPIRY%' then 'Operator'
when cancel_remarks ilike '%PRICE_FOR_KNOWN_LANES_NOT_FOUND_DEMAND_EXPIRY%' then 'Other'
when cancel_remarks ilike '%ADDRESS_MISMATCH_DEMAND_EXPIRY%' then 'Consigner'
when cancel_remarks ilike '%VEHICLE_NOT_FOUND_DEMAND_EXPIRY%' then 'Other'
when cancel_remarks ilike '%VEHICLE_SIZE_TYPE_MISMATCH_DEMAND_EXPIRY%' then 'Operator'
when cancel_remarks ilike '%CANCELLED_AFTER_FULFILMENT_CONSIGNEE_DEMAND_EXPIRY%' then 'Consigner'
when cancel_remarks ilike '%NO_ENTRY_SLOT_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%LOADING_POSTPONED_CANCELLED_BY_CONSIGNER_WITHOUT_CONSIGNMENT%' then 'Consigner'
-----
--ADDED NEW REASONS BELOW-----
-----
when cancel_remarks ilike '%%%%DEMAND_MODIFIED_CONSIGNOR_APP%%%%' then 'Consigner'
when cancel_remarks ilike '%%%%FOUND_VEHICLE_AT_BETTER_PRICE_CANCELLED_BY_CONSIGNER_WITHOUT_CONSIGNMENT%%%%' then 'Consigner'
when cancel_remarks ilike '%%%%INCORRECT_UNLOADING_LOCATION_DEMAND_EXPIRY%%%%' then 'Consigner'
when cancel_remarks ilike '%%%%BREAKDOWN_WITH_VERIFICATION_CONSIGNMENT_CANCELLATION%%%%' then 'Operator'
when cancel_remarks ilike '%VEHICLE_GETTING_DELAYED_CANCELLED_BY_CONSIGNER_AT_GTL%' then 'Operator'
when cancel_remarks ilike '%OPERATOR_DENIED_OR_NOT_RESPONDING_CANCELLED_BY_CONSIGNER_AT_GTL%' then 'Operator'
when cancel_remarks ilike '%LOADING_POSTPONED_CANCELLED_BY_CONSIGNER_AT_GTL%' then 'Consigner'
when cancel_remarks ilike '%BOOKED_ANOTHER_VEHICLE_CANCELLED_BY_CONSIGNER_AT_GTL%' then 'Consigner'
when cancel_remarks ilike '%LOADING_POSTPONED_CANCELLED_BY_CONSIGNER_AT_LOADING%' then 'Consigner'
when cancel_remarks ilike '%FOUND_VEHICLE_AT_BETTER_PRICE_CANCELLED_BY_CONSIGNER_AT_GTL%' then 'Consigner'
when cancel_remarks ilike '%FOUND_VEHICLE_AT_BETTER_PRICE_CANCELLED_BY_CONSIGNER_AT_LOADING%' then 'Consigner'
when cancel_remarks ilike '%BOOKING_REQUIREMENT_CHANGE_CANCELLED_BY_CONSIGNER_AT_GTL%' then 'Consigner'
when cancel_remarks ilike '%DELAYED_COMMUNICATION_DEMAND_DEMAND_EXPIRY%' then 'Other'


when cancel_remarks ilike '%UNFIT_VEHICLE_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%SR_NA_OVERHEIGHT_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%SR_NA_OVERWEIGHT_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%SR_NA_EXP_DELIVERY_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%SR_NA_DIESEL_VEH_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%SR_NA_OPEN_DALA_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%VEH_MISMATCH_SIZE_DIFFERENT_CONSIGNMENT_CANCELLATION%' then 'Operator'
when cancel_remarks ilike '%VEH_MISMATCH_BODY_DIFFERENT_CONSIGNMENT_CANCELLATION%' then 'Operator'

when cancel_remarks ilike '%BOOKING_REQUIREMENT_CHANGE_CANCELLED_BY_CONSIGNER_AT_LOADING%' then 'Consigner'
when cancel_remarks ilike '%BOOKED_ANOTHER_VEHICLE_CANCELLED_BY_CONSIGNER_AT_LOADING%' then 'Consigner'
when cancel_remarks ilike '%BOOKING_REQUIREMENT_CHANGE_CANCELLED_BY_CONSIGNER_AT_LOADING%' then 'Consigner'
when cancel_remarks ilike '%WRONG_VEHICLE_REACHED_CANCELLED_BY_CONSIGNER_AT_LOADING%' then 'Consigner'
else null end as placed_Cancel_reason,wcsi.ranker,
case when consignment_state = 'CANCELLED' then row_number()over(partition by c.demand_id,c.state order by placement_time desc) end as ord,dt.status as token_status,
(timestamp 'epoch' + ((wcsi.t + 19800) * interval '1 second')) as cancel_time,
datediff(min,placement_time,cancel_time) as cancelled_in,
wcsi.updated_by as cancelled_by,
vt.tyre_count,
c.operator_code,
d.consigner_user_code
from wfms_consignments c 
left join(select *,row_number()over(partition by consignment_code order by t desc) as ranker from (select consignment_code,state,remarks,time as t,updated_by  from wfms_consignment_state_info where state = 'CANCELLED' and deleted = 'false')) wcsi on wcsi.consignment_code = c.code 
left join wfms_demands d on d.id = c.demand_id
left join wfms_consigners cn on cn.code = d.consigner_user_code
left join wfms_vehicle_types vt on vt.id = d.vehicle_type_id
left join (select * from wfms_operator_demand_token where status NOT IN ('EXPIRED','INITIATED')) dt on dt.demand_id = c.demand_id and dt.operator_code = c.operator_code
where placement_time>='2024-01-01' and json_extract_path_text(d.metadata,'ptl')!='true' and cn.type IN ('SME','SME_HP') and cn.name not ilike '%testing%' and cn.name not ilike '%alternate%'
--and  placed_Cancel_reason = 'Operator' and cancelled_in>15 and token_status <> 'FORFEITED'
order by placement_time desc""",conn)


# In[17]:


cancel_raw['week_date'] = cancel_raw['placement_time'].dt.strftime('%A')  # Day of the week
cancel_raw['month_year'] = cancel_raw['placement_time'].dt.strftime('%b\'%y')  # Month name


# In[18]:


cancel_raw['week_start'] = cancel_raw['placement_time'] - pd.to_timedelta(cancel_raw['placement_time'].dt.weekday, unit='D')
cancel_raw['week_end'] = cancel_raw['week_start'] + pd.to_timedelta(6, unit='D')
# Format the week_start and week_end dates?
cancel_raw['week_date'] = cancel_raw['week_start'].dt.strftime('%d-%b-%y') + ' - ' + cancel_raw['week_end'].dt.strftime('%d-%b-%y')
cancel_raw = cancel_raw.drop(columns={'ranker','token_status','cancel_time','cancelled_in','cancelled_by','tyre_count','consigner_user_code','week_start','week_end'})
cancel_raw = cancel_raw.drop(columns={'cancel_remarks'})
cancel_raw
cancel_raw['date'] = cancel_raw['placement_time'].dt.date


# In[19]:


new_raw
new_raw['date'] = new_raw['unloading_done_time'].dt.date


# In[20]:


runtime=dt.datetime.now()

ws1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1467923172#gid=1467923172').worksheet('trip_end_raw')
gd.set_with_dataframe(ws1,new_raw,row=2,col=1, resize=False,include_index=False)
ws1.update_acell('A1',str(runtime))

print(f"Done: {dt.datetime.now()}")


# In[21]:


runtime=dt.datetime.now()

ws1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1467923172#gid=1467923172').worksheet('can_raw')
gd.set_with_dataframe(ws1,cancel_raw,row=2,col=1, resize=False,include_index=False)
ws1.update_acell('A1',str(runtime))

print(f"Done: {dt.datetime.now()}")


# In[22]:


sub_raw = pd.read_sql("""select s_date,
count(distinct(user_code)) as sub_op,
sum(count(distinct(user_code))) over (order by s_date rows between unbounded preceding and current row) as cumulative_subscribed

from

(select a.user_code,a.created_at::date as s_date, date_trunc('week',a.created_at)::date as s_week, date_trunc('month',a.created_at)::date as s_month, b.latest_quote, datediff('days',b.latest_quote,getdate()),
c.latest_placement, datediff('days',c.latest_placement,getdate())
from trucking.apollo_subscription a

left join (select operator_code, max(created)::date as latest_quote from trucking.wfms_operator_demand_quotation
            where (case when json_extract_path_text(app_response_details,'quote') != '' then json_extract_path_text(app_response_details,'quote')::float else null end) > 0
            group by 1) b on b.operator_code = a.user_code
left join (select operator_code, max(created)::date as latest_placement from trucking.wfms_consignments
            where deleted = 'false' and state != 'CANCELLED'
            group by 1) c on c.operator_code = a.user_code

where a.deleted = 'false'
order by 1 asc)

group by 1
order by 1 desc""",conn)


# In[23]:


sub_raw['s_date'] = pd.to_datetime(sub_raw['s_date'])
sub_raw['week_date'] = sub_raw['s_date'].dt.strftime('%A')  # Day of the week
sub_raw['month_year'] = sub_raw['s_date'].dt.strftime('%b\'%y')  # Month name
sub_raw['week_start'] = sub_raw['s_date'] - pd.to_timedelta(sub_raw['s_date'].dt.weekday, unit='D')
sub_raw['week_end'] = sub_raw['week_start'] + pd.to_timedelta(6, unit='D')
# Format the week_start and week_end dates?
sub_raw['week_date'] = sub_raw['week_start'].dt.strftime('%d-%b-%y') + ' - ' + sub_raw['week_end'].dt.strftime('%d-%b-%y')
sub_raw = sub_raw.drop(columns={'sub_op','week_start','week_end'})
sub_raw


# In[24]:


runtime=dt.datetime.now()

ws1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1467923172#gid=1467923172').worksheet('sub_raw')
gd.set_with_dataframe(ws1,sub_raw,row=2,col=1, resize=False,include_index=False)
ws1.update_acell('A1',str(runtime))

print(f"Done: {dt.datetime.now()}")


# In[25]:


fo_tkts = pd.read_sql("""WITH tk AS (
    SELECT ticket_code, description, subject, status, a_created, hold_time, resolved_time, datediff(hour, a_created, resolved_time) AS resolved_dur, created_by, assigned_to, reporting_to, updated_by, consigner_type,
           extract(hour from a_created)::integer AS a_hour, 
           a_created::date AS a_date,
           extract(month from a_created)::integer AS a_month, 
           extract(week from a_created)::integer AS a_week, issue_identified, trip_id
    FROM (
        SELECT ticket_code, a.description, a.subject, status, (a.created + interval '5h30m') AS a_created, (a.updated + interval '5h30m') AS a_updated, b.string_value AS issue_identified,
               CAST(DATEDIFF(MINUTE, a_created, a_updated) AS DECIMAL(30,10))/60 AS hour_tat, a.created_by, a.assigned_to, reporting_to, a.updated_by,
               CASE WHEN status = 'RESOLVED' THEN 1 ELSE 0 END AS resolve,
               CASE WHEN status = 'HOLD' THEN 1 ELSE 0 END AS holding,
               CASE WHEN status = 'ACTIVE' THEN 1 ELSE 0 END AS actived,
               (resolve + holding + actived) AS total, t.string_value AS trip_id, dr.consigner_type, hold.updatd AS hold_time, resolved.updatd AS resolved_time
        FROM tms_ticket a
        LEFT JOIN Tms_custom_field_value b ON a.id = b.ticket_id
        LEFT JOIN (SELECT * FROM Tms_custom_field_value WHERE custom_field_id = 5) t ON t.ticket_id = a.id
        LEFT JOIN (SELECT *, (updated + interval '5h30m') AS updatd, ROW_NUMBER() OVER(PARTITION BY ticket_id ORDER BY updated ASC) AS ranker FROM tms_ticket_activity WHERE field = 'status' AND new_value = 'HOLD') hold ON hold.ticket_id = a.id AND hold.ranker=1
        LEFT JOIN (SELECT *, (updated + interval '5h30m') AS updatd FROM tms_ticket_activity WHERE field = 'status' AND new_value = 'RESOLVED') resolved ON resolved.ticket_id = a.id
        LEFT JOIN demand_report dr ON dr.consignment_code = t.string_value
        WHERE source = ('MATRIX') AND b.custom_field_id = 10
    ) 
    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,issue_identified,trip_id
),
trips_data AS (
    SELECT code, c.created + interval '5h30m' AS placemnt_time, extract(week from placemnt_time) AS week_placed, extract(month from placemnt_time) AS month_placed,
           listagg(issue_identified, '/') AS tickets_created,
           dr.consigner_type, listagg(ticket_code, '/') AS ticket_codes,
           LENGTH(tickets_created) - LENGTH(REPLACE(tickets_created, '/', '')) + 1 AS total_tickets
    FROM wfms_consignments c 
    LEFT JOIN tk ON tk.trip_id = c.code
    LEFT JOIN demand_report dr ON dr.consignment_code = c.code
    left join wfms_demands d on d.id = c.demand_id 
    WHERE placemnt_time >= '2023-02-01' AND c.deleted = 'false' and  json_extract_path_text(d.metadata,'ptl')!='true'
    GROUP BY 1,2,dr.consigner_type
)


(SELECT placemnt_time::date,
count(distinct(code)) AS trips,
       count(distinct(case when total_tickets IS NULL THEN code END)) AS no_tickets_created_trips,
       count(distinct(case when total_tickets >= 1 THEN code END)) AS "trip_atleast_1_tickets",
       sum(total_tickets) AS total_tickets_created,
       total_tickets_created * 1.0 / trips AS "Ratio_trips_tickets",
       no_tickets_created_trips * 100.0 / trips AS "%_no_trips_tickets",
       trip_atleast_1_tickets * 100.0 / trips AS "%_trips_tickets>=1"
FROM trips_data
WHERE placemnt_time>='2023-12-01'
group by 1 
order by 1 desc)""",conn)



# In[26]:


runtime=dt.datetime.now()

ws1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1467923172#gid=1467923172').worksheet('fo_tkts')
gd.set_with_dataframe(ws1,fo_tkts,row=2,col=1, resize=False,include_index=False)
ws1.update_acell('A1',str(runtime))

print(f"Done: {dt.datetime.now()}")


# In[27]:


avg_quotes = pd.read_sql("""select quote_date,count((demand_id)) as demands , 
count(distinct(operator_code)) as operators,
demands*1.0/nullif(operators,0) as quotes_per_op
from
(select operator_code, (created)::date as quote_date,demand_id from trucking.wfms_operator_demand_quotation
            where (case when json_extract_path_text(app_response_details,'quote') != '' then json_extract_path_text(app_response_details,'quote')::float else null end) > 0
            and quote_date>='2023-01-01'
            group by 1,2,3)
            group by 1 
            order by 1 desc""",conn)


# In[28]:


runtime=dt.datetime.now()

ws1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1467923172#gid=1467923172').worksheet('avg_quotes')
gd.set_with_dataframe(ws1,avg_quotes,row=2,col=1, resize=False,include_index=False)
ws1.update_acell('A1',str(runtime))

print(f"Done: {dt.datetime.now()}")


# In[29]:


acq_pod_dod = pd.read_sql("""select subs_date, 
sum(op_subscribed) over(order by subs_date rows between unbounded preceding and current row) as op_subscribed_,
sum(op_verified) over(order by subs_date rows between unbounded preceding and current row) as op_verified_,
sum(op_profile_completed) over(order by subs_date rows between unbounded preceding and current row) as op_profile_completed_,
sum(op_active_quote) over(order by subs_date rows between unbounded preceding and current row) as op_active_quote_,
sum(op_active_placement) over(order by subs_date rows between unbounded preceding and current row) as op_active_placement_
from
(select subs_date, 
count(user_code) as op_subscribed,
count(case when kyc_status = 'VERIFIED' then user_code else null end) as op_verified,
count(case when op_profile = 1 then user_code else null end) as op_profile_completed,
count(case when quote_active = 1 then user_code else null end) as op_active_quote,
count(case when plc_active = 1 then user_code else null end) as op_active_placement
from
(select a.user_code, date_trunc('days',a.created_at)::date as subs_date, date_trunc('week',a.created_at)::date as subs_week, date_trunc('month',a.created_at)::date as subs_month, d.kyc_status, 
case when datediff('days',b.latest_quote,getdate()) <= 30 then 1 else 0 end as quote_active,
case when datediff('days',c.latest_placement,getdate()) <= 30 then 1 else 0 end as plc_active,
case when e.q_count > 0 then 1 else 0 end as op_profile
from trucking.apollo_subscription a

left join (select operator_code, max(created)::date as latest_quote from trucking.wfms_operator_demand_quotation
            where (case when json_extract_path_text(app_response_details,'quote') != '' then json_extract_path_text(app_response_details,'quote')::float else null end) > 0
            group by 1) b on b.operator_code = a.user_code
left join (select operator_code, max(created)::date as latest_placement from trucking.wfms_consignments
            where deleted = 'false' and state != 'CANCELLED'
            group by 1) c on c.operator_code = a.user_code
left join (select user_code, kyc_status from trucking.apollo_operator_profile
            where deleted = 'false') d on d.user_code = a.user_code
left join (select user_code, count(case when rate > 0 then user_code else null end) as q_count from trucking.wfms_operator_odvt_rate_input
            where active = 'true'
            group by 1) e on e.user_code = a.user_code
where a.deleted = 'false')
group by 1) a
group by 1, a.op_subscribed, a.op_verified, a.op_profile_completed, a.op_active_quote, a.op_active_placement
order by 1 desc""",conn)


# In[30]:


runtime=dt.datetime.now()

ws1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1467923172#gid=1467923172').worksheet('acq_pod_dod')
gd.set_with_dataframe(ws1,acq_pod_dod,row=2,col=1, resize=False,include_index=False)
ws1.update_acell('A1',str(runtime))

print(f"Done: {dt.datetime.now()}")


# In[31]:


acq_pod_wow = pd.read_sql("""select subs_week, 
sum(op_subscribed) over(order by subs_week rows between unbounded preceding and current row) as op_subscribed_,
sum(op_verified) over(order by subs_week rows between unbounded preceding and current row) as op_verified_,
sum(op_profile_completed) over(order by subs_week rows between unbounded preceding and current row) as op_profile_completed_,
sum(op_active_quote) over(order by subs_week rows between unbounded preceding and current row) as op_active_quote_,
sum(op_active_placement) over(order by subs_week rows between unbounded preceding and current row) as op_active_placement_
from
(select subs_week, 
count(user_code) as op_subscribed,
count(case when kyc_status = 'VERIFIED' then user_code else null end) as op_verified,
count(case when op_profile = 1 then user_code else null end) as op_profile_completed,
count(case when quote_active = 1 then user_code else null end) as op_active_quote,
count(case when plc_active = 1 then user_code else null end) as op_active_placement
from
(select a.user_code, date_trunc('days',a.created_at)::date as subs_date, date_trunc('week',a.created_at)::date as subs_week, date_trunc('month',a.created_at)::date as subs_month, d.kyc_status, 
case when datediff('days',b.latest_quote,getdate()) <= 30 then 1 else 0 end as quote_active,
case when datediff('days',c.latest_placement,getdate()) <= 30 then 1 else 0 end as plc_active,
case when e.q_count > 0 then 1 else 0 end as op_profile
from trucking.apollo_subscription a

left join (select operator_code, max(created)::date as latest_quote from trucking.wfms_operator_demand_quotation
            where (case when json_extract_path_text(app_response_details,'quote') != '' then json_extract_path_text(app_response_details,'quote')::float else null end) > 0
            group by 1) b on b.operator_code = a.user_code
left join (select operator_code, max(created)::date as latest_placement from trucking.wfms_consignments
            where deleted = 'false' and state != 'CANCELLED'
            group by 1) c on c.operator_code = a.user_code
left join (select user_code, kyc_status from trucking.apollo_operator_profile
            where deleted = 'false') d on d.user_code = a.user_code
left join (select user_code, count(case when rate > 0 then user_code else null end) as q_count from trucking.wfms_operator_odvt_rate_input
            where active = 'true'
            group by 1) e on e.user_code = a.user_code
where a.deleted = 'false')
group by 1) a
group by 1, a.op_subscribed, a.op_verified, a.op_profile_completed, a.op_active_quote, a.op_active_placement
order by 1 desc""",conn)


# In[32]:


acq_pod_mom = pd.read_sql("""select subs_month, 
sum(op_subscribed) over(order by subs_month rows between unbounded preceding and current row) as op_subscribed_,
sum(op_verified) over(order by subs_month rows between unbounded preceding and current row) as op_verified_,
sum(op_profile_completed) over(order by subs_month rows between unbounded preceding and current row) as op_profile_completed_,
sum(op_active_quote) over(order by subs_month rows between unbounded preceding and current row) as op_active_quote_,
sum(op_active_placement) over(order by subs_month rows between unbounded preceding and current row) as op_active_placement_
from
(select subs_month, 
count(user_code) as op_subscribed,
count(case when kyc_status = 'VERIFIED' then user_code else null end) as op_verified,
count(case when op_profile = 1 then user_code else null end) as op_profile_completed,
count(case when quote_active = 1 then user_code else null end) as op_active_quote,
count(case when plc_active = 1 then user_code else null end) as op_active_placement
from
(select a.user_code, date_trunc('days',a.created_at)::date as subs_date, date_trunc('week',a.created_at)::date as subs_week, date_trunc('month',a.created_at)::date as subs_month, d.kyc_status, 
case when datediff('days',b.latest_quote,getdate()) <= 30 then 1 else 0 end as quote_active,
case when datediff('days',c.latest_placement,getdate()) <= 30 then 1 else 0 end as plc_active,
case when e.q_count > 0 then 1 else 0 end as op_profile
from trucking.apollo_subscription a

left join (select operator_code, max(created)::date as latest_quote from trucking.wfms_operator_demand_quotation
            where (case when json_extract_path_text(app_response_details,'quote') != '' then json_extract_path_text(app_response_details,'quote')::float else null end) > 0
            group by 1) b on b.operator_code = a.user_code
left join (select operator_code, max(created)::date as latest_placement from trucking.wfms_consignments
            where deleted = 'false' and state != 'CANCELLED'
            group by 1) c on c.operator_code = a.user_code
left join (select user_code, kyc_status from trucking.apollo_operator_profile
            where deleted = 'false') d on d.user_code = a.user_code
left join (select user_code, count(case when rate > 0 then user_code else null end) as q_count from trucking.wfms_operator_odvt_rate_input
            where active = 'true'
            group by 1) e on e.user_code = a.user_code
where a.deleted = 'false')
group by 1) a
group by 1, a.op_subscribed, a.op_verified, a.op_profile_completed, a.op_active_quote, a.op_active_placement
order by 1 desc""",conn)


# In[33]:


runtime=dt.datetime.now()

ws1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1467923172#gid=1467923172').worksheet('acq_pod_wow')
gd.set_with_dataframe(ws1,acq_pod_wow,row=2,col=1, resize=False,include_index=False)
ws1.update_acell('A1',str(runtime))

print(f"Done: {dt.datetime.now()}")


runtime=dt.datetime.now()

ws1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1467923172#gid=1467923172').worksheet('acq_pod_mom')
gd.set_with_dataframe(ws1,acq_pod_mom,row=2,col=1, resize=False,include_index=False)
ws1.update_acell('A1',str(runtime))

print(f"Done: {dt.datetime.now()}")


# In[34]:


cc_calls = pd.read_sql("""WITH cll AS (
    SELECT date_trunc('day', call_time) AS "day", count(distinct call_id) AS calls_count
    FROM  (
        SELECT call_time, a.call_id, c.disposition_code, c_type
        FROM cc_acd_raw a
        LEFT JOIN cc_disposition_raw b ON a.user_disposition_code = b.user_disposition_code
        LEFT JOIN cc_crm_raw c ON a.call_id = c.call_id
        WHERE product IN ('Marketplace') AND c_type = 'Customer' AND call_time::date >= '2023-07-13' and disposition_code IN ('MP_VEHICLE OVERLOAD / OVERHEIGHT ISSUE',
'MP_TRACKING ISSUE',
'MP_DEMAND CANCEL FROM SHIPPER',
'MP_TICKET UNRESOLVED',
'MP_TOKEN MONEY QUERY',
'MP_TRIP UPDATE ISSUE',
'MP_VEHICLE CANCELLATION CHARGES',
'MP_VEHICLE NO / DRIVER NO CHANGE',
'MP_VEHICLE REPLACEMENT STATUS',
'MP_EXTRA DISTANCE CHARGES',
'MP_ADVANCE QUERY',
'MP_TOKEN MONEY REFUND QUERY',
'MP_TRANSIT HAULTIING CHARGE',
'MP_VEHICLE BREAKDOWN',
'MP_LOADING TIME POSTPONED',
'MP_WAITING FOR LOADING / UNLOADING',
'MP_LOADING / UNLOADING CHARGE',
'MP_TICKET RESOLVED',
'MP_DAMAGE / SHORTAGE',
'MP_TRANSIT DELAY PENALTY',
'MP_WAITING FOR BILTY / POD',
'MP_LOADING/ UNLOADING/ POC ADDRESS QUERY',
'MP_MULTIPLE LOADING / UNLOADING POINT',
'MP_LOADING / UNLOADING DETENTION',
'MP_GTL DELAY PENALTY QUERY',
'MP_PAYMENT DONE UTR NOT GENERATED')
    ) AS subquery
    GROUP BY 1
)

-- MP_VEHICLE OVERLOAD / OVERHEIGHT ISSUE
-- MP_TRACKING ISSUE
-- MP_DEMAND CANCEL FROM SHIPPER
-- MP_TICKET UNRESOLVED
-- MP_TOKEN MONEY QUERY
-- MP_TRIP UPDATE ISSUE
-- MP_VEHICLE CANCELLATION CHARGES
-- MP_VEHICLE NO / DRIVER NO CHANGE
-- MP_VEHICLE REPLACEMENT STATUS
-- MP_EXTRA DISTANCE CHARGES
-- MP_ADVANCE QUERY
-- MP_TOKEN MONEY REFUND QUERY
-- MP_TRANSIT HAULTIING CHARGE
-- MP_VEHICLE BREAKDOWN
-- MP_LOADING TIME POSTPONED
-- MP_WAITING FOR LOADING / UNLOADING
-- MP_LOADING / UNLOADING CHARGE
-- MP_TICKET RESOLVED
-- MP_DAMAGE / SHORTAGE
-- MP_TRANSIT DELAY PENALTY
-- MP_WAITING FOR BILTY / POD
-- MP_LOADING/ UNLOADING/ POC ADDRESS QUERY
-- MP_MULTIPLE LOADING / UNLOADING POINT
-- MP_LOADING / UNLOADING DETENTION
-- MP_GTL DELAY PENALTY QUERY
-- MP_PAYMENT DONE UTR NOT GENERATED


(SELECT date_trunc('day', c.created + interval '5h30m') as day,
       count(code) AS trips,
       cll.calls_count,
       cll.calls_count * 1.0 / count(code) AS calls_trips_ratio
FROM wfms_consignments c
LEFT join wfms_demands d on d.id = c.demand_id 
LEFT JOIN cll ON cll.day = date_trunc('day', c.created + interval '5h30m')
WHERE json_extract_path_text(d.metadata,'ptl')!='true'
GROUP BY  1,3)
order by 1 desc""",conn)


# In[35]:


runtime=dt.datetime.now()

ws1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1467923172#gid=1467923172').worksheet('Call_fo')
gd.set_with_dataframe(ws1,cc_calls,row=2,col=1, resize=False,include_index=False)
ws1.update_acell('A1',str(runtime))

print(f"Done: {dt.datetime.now()}")


# In[36]:


advance_op_payment = pd.read_sql("""select date(date_trunc('day',unloading_done_time)) as month, 
      count(distinct(code)) as total_trips,
      count(case when time_dispose is not null then 1 end) as advance_paid,
      count(case when time_dispose = 'LessThan6Hr' then 1 end) as LessThan6Hr,
      round(LessThan6Hr*100.0/nullif(advance_paid,0),2)||'%%' as LessThan6Hr_prct,
      count(case when time_dispose = 'Btw6to12Hr' then 1 end) as Btw6to12Hr,
      round(Btw6to12Hr*100.0/nullif(advance_paid,0),2)||'%%' as Btw6to12Hr_prct,
      count(case when time_dispose = 'Btw12to24Hr' then 1 end) as Btw12to24Hr,
      round(Btw12to24Hr*100.0/nullif(advance_paid,0),2)||'%%' as Btw12to24Hr_prct,
      count(case when time_dispose = 'Morethan24Hr' then 1 end) as Morethan24Hr,
      round(Morethan24Hr*100.0/nullif(advance_paid,0),2)||'%%' as Morethan24Hr_prct

from (select distinct c.code,dr.demand_id,dr.unloading_done_time,
       x.raise_time,x.pay_time as adv_pay_time ,x.amount as adv_paid,
       
       DATEDIFF(minute, '1970-01-01 00:00:00',adv_pay_time)-DATEDIFF(minute, '1970-01-01 00:00:00',trip_start_time)::float  as adv_time_diff,
       case when adv_time_diff <360 then 'LessThan6Hr'
            when adv_time_diff between 360 and 720 then 'Btw6to12Hr'
            when adv_time_diff between 720 and 1440 then 'Btw12to24Hr'
            when adv_time_diff >1440 then 'Morethan24Hr' 
            when (trip_bal<200 or trip_bal is null) and adv_time_diff is null then 'LessThan6Hr'
            end as time_dispose, bl.trip_bal
            
from trucking.wfms_consignments c
left join analytics.demand_report dr on dr.demand_id = c.demand_id

left join (select * from (select consignment_id, amount,
                    created+interval'5h30m' as raise_time,
                    updated+interval'5h30m' as pay_time,
                   row_number()over(partition by consignment_id order by pay_time desc) as rnkr
            from wfms_operator_consignment_transaction
            where type = 'ADVANCE'
            and status = 'SUCCESS'
            and category = 'DEBIT'
            and deleted = 'false') where rnkr =1) x on split_part(c.code,'-',2) = x.consignment_id


left join (select dr.demand_id,dr.op_freight::float,
                   case when opadd.amount is null then 0.0 else opadd.amount::float end as opadd,
                   case when opsub.amount is null then 0.0 else opsub.amount::float end as opsub,
                   case when tds.tds_value = ''  then 0.0 else coalesce(tds.tds_value::float,0.0) end as tv,
                   op_freight*tv as tds,
                   oppaid.amount::float as oppaid,
                   op_freight + opadd - opsub - tds - oppaid as trip_bal
            
            from demand_report dr
            left join (Select wmt.consignment_id as demand_id, sum(coalesce(wmt.amount,0)) as amount
                        FROM trucking.wfms_consignment_expense as wmt
                        WHERE wmt.category = 'CREDIT' and wmt.party_type = 'OPERATOR' and wmt.deleted = 'false' 
                        GROUP BY wmt.consignment_id) opadd on opadd.demand_id = split_part(dr.consignment_code,'-',2)
            left join (Select wmt.consignment_id as demand_id, sum(coalesce(wmt.amount,0)) as amount
                        FROM trucking.wfms_consignment_expense as wmt
                        WHERE wmt.category = 'DEBIT' and wmt.party_type = 'OPERATOR' and wmt.deleted = 'false' 
                        GROUP BY wmt.consignment_id) opsub on opsub.demand_id = split_part(dr.consignment_code,'-',2)
            left join (select * from (select dr.demand_id,dr.tds_value as tds_value,dr.tds_amount as tds_amount,
                       row_number()over(partition by demand_id order by tds_amount) as rnk
                                        FROM demand_report as dr 
                                        GROUP by dr.demand_id,tds_value,tds_amount) where rnk=1) tds on tds.demand_id = dr.demand_id
            left join (select dr.demand_id,sum(coalesce(tn.amount,0)) as amount
                        from analytics.demand_report dr
                        left join analytics.tp_op_new_data4 tn on tn.code = dr.consignment_code
                        where tn.otherparty='OPERATOR' and tn.status in ('SUCCESS','DONE') group by 1) oppaid on oppaid.demand_id = dr.demand_id
            where dr.vehicle_state = 'TRIP_END'
            group by 1,2,3,4,5,6,7,8) bl on bl.demand_id = dr.demand_id

left join (select * from(select demand_id,remarks,created,
                    rank()over(partition by demand_id order by created desc) as rnkr
            from wfms_demand_comments
            where remarks = 'DEMAND CANCELLED') where rnkr =1) y on y.demand_id = dr.demand_id

left join (select demand_id,remarks from (select demand_id,remarks,row_number() over(partition by demand_id order by created desc) as ranker from trucking.wfms_demand_comments
          where remarks ilike '%%wheelseye_ptl%%') where ranker=1) b on b.demand_id = dr.demand_id
          
LEFT JOIN (select id,json_extract_path_text(metadata,'ptl') as ptl from wfms_demands) wd on wd.id = dr.demand_id

left join wfms_consigners z on z.code = dr.consigner_code

where dr.unloading_done_time >= '2023-07-01'
and c.deleted = 'false' and c.state IN ('TRIP_END')
and z.name not ilike '%%testing%%'
and y.remarks is null and b.remarks is null and ptl != 'true' )
group by 1
order by 1 desc""",conn)


# In[37]:


runtime=dt.datetime.now()

ws1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1467923172#gid=1467923172').worksheet('advance_op_payment')
gd.set_with_dataframe(ws1,advance_op_payment,row=2,col=1, resize=False,include_index=False)
ws1.update_acell('A1',str(runtime))

print(f"Done: {dt.datetime.now()}")


# In[38]:


bal_op_payment = pd.read_sql("""select date(date_trunc('day',unloading_done_time)) as month, 
      count(distinct(code)) as total_trips,
      count(case when time_dispose is not null then 1 end) as balance_paid,
      count(case when time_dispose = 'LessThan6Hr' then 1 end) as LessThan6Hr,
      round(LessThan6Hr*100.0/nullif(balance_paid,0),2)||'%%' as LessThan6Hr_prct,
      count(case when time_dispose = 'Btw6to12Hr' then 1 end) as Btw6to12Hr,
      round(Btw6to12Hr*100.0/nullif(balance_paid,0),2)||'%%' as Btw6to12Hr_prct,
      count(case when time_dispose = 'Btw12to24Hr' then 1 end) as Btw12to24Hr,
      round(Btw12to24Hr*100.0/nullif(balance_paid,0),2)||'%%' as Btw12to24Hr_prct,
      count(case when time_dispose = 'Morethan24Hr' then 1 end) as Morethan24Hr,
      round(Morethan24Hr*100.0/nullif(balance_paid,0),2)||'%%' as Morethan24Hr_prct,
      count(case when pod_status = 'VERIFIED' then 1 end) as verified

from (select distinct c.code,dr.demand_id,dr.unloading_done_time,
       x.raise_time,x.pay_time as bal_pay_time ,x.amount as bal_paid,
       x2.pod_status,x2.pod_validity,x2.pod_collection_entry_time,
       case when unloading_done_time >= pod_collection_entry_time then unloading_done_time
            when pod_collection_entry_time >= unloading_done_time then pod_collection_entry_time end as finl_time,
       DATEDIFF(minute, '1970-01-01 00:00:00',bal_pay_time)-DATEDIFF(minute, '1970-01-01 00:00:00',finl_time)::float  as bal_time_diff,
       case when bal_time_diff <360 then 'LessThan6Hr'
            when bal_time_diff between 360 and 720 then 'Btw6to12Hr'
            when bal_time_diff between 720 and 1440 then 'Btw12to24Hr'
            when bal_time_diff >1440 then 'Morethan24Hr' 
            when (trip_bal<200 or trip_bal is null) and bal_pay_time is null then 'LessThan6Hr'
            end as time_dispose, bl.trip_bal
            
from trucking.wfms_consignments c
left join analytics.demand_report dr on dr.demand_id = c.demand_id

left join (select * from (select code, amount,
                    created+interval'5h30m' as raise_time,
                    actpaidtime+interval'5h30m' as pay_time,
                   row_number()over(partition by code order by pay_time desc) as rnkr
            from tp_op_new_data4
            where opsremarks = 'BALANCE'
            and status = 'SUCCESS'
        ) where rnkr =1) x on c.code = x.code

left join (select code,demand_id,
                   json_extract_path_text(offline_metadata,'podStatus') as pod_status,
                   json_extract_path_text(offline_metadata,'podValidity') as pod_validity,
                   case when json_extract_path_text(offline_metadata,'podCollectedDate')>0 then timestamp 'epoch' 
                   +((json_extract_path_text(offline_metadata,'podCollectedDate')::float) + 19800) * interval '1 second' else null 
                   end as pod_collection_entry_time
            from wfms_consignments
            where state = 'TRIP_END'
            and deleted = 'false') x2 on x2.code = c.code

left join (select dr.demand_id,dr.op_freight::float,
                   case when opadd.amount is null then 0.0 else opadd.amount::float end as opadd,
                   case when opsub.amount is null then 0.0 else opsub.amount::float end as opsub,
                   case when tds.tds_value = ''  then 0.0 else coalesce(tds.tds_value::float,0.0) end as tv,
                   op_freight*tv as tds,
                   oppaid.amount::float as oppaid,
                   op_freight + opadd - opsub - tds - oppaid as trip_bal
            
            from demand_report dr
            left join (Select wmt.consignment_id as demand_id, sum(coalesce(wmt.amount,0)) as amount
                        FROM trucking.wfms_consignment_expense as wmt
                        WHERE wmt.category = 'CREDIT' and wmt.party_type = 'OPERATOR' and wmt.deleted = 'false' 
                        GROUP BY wmt.consignment_id) opadd on opadd.demand_id = split_part(dr.consignment_code,'-',2)
            left join (Select wmt.consignment_id as demand_id, sum(coalesce(wmt.amount,0)) as amount
                        FROM trucking.wfms_consignment_expense as wmt
                        WHERE wmt.category = 'DEBIT' and wmt.party_type = 'OPERATOR' and wmt.deleted = 'false' 
                        GROUP BY wmt.consignment_id) opsub on opsub.demand_id = split_part(dr.consignment_code,'-',2)
            left join (select * from (select dr.demand_id,dr.tds_value as tds_value,dr.tds_amount as tds_amount,
                       row_number()over(partition by demand_id order by tds_amount) as rnk
                                        FROM demand_report as dr 
                                        GROUP by dr.demand_id,tds_value,tds_amount) where rnk=1) tds on tds.demand_id = dr.demand_id
            left join (select dr.demand_id,sum(coalesce(tn.amount,0)) as amount
                        from analytics.demand_report dr
                        left join analytics.tp_op_new_data4 tn on tn.code = dr.consignment_code
                        where tn.otherparty='OPERATOR' and tn.status in ('SUCCESS','DONE') group by 1) oppaid on oppaid.demand_id = dr.demand_id
            where dr.vehicle_state = 'TRIP_END'
            group by 1,2,3,4,5,6,7,8) bl on bl.demand_id = dr.demand_id

left join (select * from(select demand_id,remarks,created,
                    rank()over(partition by demand_id order by created desc) as rnkr
            from wfms_demand_comments
            where remarks = 'DEMAND CANCELLED') where rnkr =1) y on y.demand_id = dr.demand_id

left join (select demand_id,remarks from (select demand_id,remarks,row_number() over(partition by demand_id order by created desc) as ranker from trucking.wfms_demand_comments
          where remarks ilike '%%wheelseye_ptl%%') where ranker=1) b on b.demand_id = dr.demand_id
left join (select id,json_extract_path_text(metadata,'ptl') as ptl from wfms_demands) wd on wd.id = dr.demand_id

left join wfms_consigners z on z.code = dr.consigner_code

where dr.unloading_done_time >= '2023-07-01'
and c.deleted = 'false' and c.state IN ('TRIP_END')
and z.name not ilike '%%testing%%'
and y.remarks is null and b.remarks is null and ptl != 'true' and pod_status = 'VERIFIED' AND pod_Validity = 'OK')
group by 1
order by 1 desc""",conn)
bal_op_payment


# In[39]:


runtime=dt.datetime.now()

ws1 = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1467923172#gid=1467923172').worksheet('bal_op_payment')
gd.set_with_dataframe(ws1,bal_op_payment,row=2,col=1, resize=False,include_index=False)
ws1.update_acell('A1',str(runtime))

print(f"Done: {dt.datetime.now()}")


# In[40]:


op_report = gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1898574702#gid=1898574702').worksheet('Dashboard_FO')


# In[41]:


data = op_report.get_all_values()
op_report = pd.DataFrame(data)
op_report.columns


# In[42]:


op_report


# In[43]:


acq_df = op_report.iloc[0:11]

acq_df.columns=['', '', '', '', '', '', '', '', '',
                                     '','','', '', '', '','','','']
acq_df


# In[44]:


pay_df = op_report.iloc[11:16]

pay_df.columns=['', '', '', '', '', '', '', '', '',
                                     '','','', '', '', '','','','']
pay_df


# In[45]:


foexp_df = op_report.iloc[16:]

foexp_df.columns=['', '', '', '', '', '', '', '', '',
                                     '','','', '', '', '','','','']
foexp_df


# In[46]:


# Reset index for each DataFrame if needed
acq_df.reset_index(drop=True, inplace=True)
pay_df.reset_index(drop=True, inplace=True)
foexp_df.reset_index(drop=True, inplace=True)


# In[47]:


import pandas as pd
from pretty_html_table import build_table  # Import build_table function from pretty_html_table
from datetime import datetime

# Assuming combined_df is your DataFrame

# Convert DataFrame to HTML table
acq_df_table = build_table(acq_df, 'blue_dark', padding='5px 20px 5px 5px', text_align='left',
                                even_color='black', even_bg_color='white', border_bottom_color='blue_light',
                                font_size='10px', index=False)

pay_df_table = build_table(pay_df, 'blue_dark', padding='5px 20px 5px 5px', text_align='left',
                                even_color='black', even_bg_color='white', border_bottom_color='blue_light',
                                font_size='10px', index=False)

foexp_df_table = build_table(foexp_df, 'blue_dark', padding='5px 20px 5px 5px', text_align='left',
                                even_color='black', even_bg_color='white', border_bottom_color='blue_light',
                                font_size='10px', index=False)


# In[48]:


import calendar

from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.message import EmailMessage
from email import encoders
import smtplib
import json
import datetime

 
import pandas as pd
from pretty_html_table import build_table


# In[49]:


from IPython.display import HTML

# Display the HTML content
HTML(foexp_df_table)


# In[50]:


import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

def send_email(sender_email, receiver_emails, subject, body, smtp_username, smtp_password):
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = ', '.join(receiver_emails)
    message['Subject'] = subject

    message.attach(MIMEText(body, 'html'))

    smtp_server = 'smtp.gmail.com'
    smtp_port = 587

    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(sender_email, receiver_emails, message.as_string())

    server.quit()

# Define your variables
sender_email = 'mohit.kumar@wheelseye.com'
#receiver_emails = ['mohit.kumar@wheelseye.com']


 receiver_emails = ['mohit.kumar@wheelseye.com','guneet.singh@wheelseye.com','nitin.kukna@wheelseye.com',
                    'arjun.kotwal@wheelseye.com','saurabh.bharti@wheelseye.com','himani.jadaun@wheelseye.com',
                     'harish.laddha@wheelseye.com','manish.somani@wheelseye.com','anshul.mimani@wheelseye.com',
                      'saikat.mukhopadhyay@wheelseye.com','mohit.aggarwal@wheelseye.com','sahil.bansal@wheelseye.com',
                       'nishant.kumar@wheelseye.com','sachin.jangra@wheelseye.com','ashish.dudeja@wheelseye.com',
                       'jatin.bhojwani@wheelseye.com','vaibhav.b@wheelseye.com','randheer.singh@wheelseye.com']
subject = 'Operator Service & Experience Reports - {}'.format(datetime.now().strftime('%Y-%m-%d'))

# Define your HTML body
# (Assuming cx_report_tab is defined elsewhere)
body = '''
Hi,
Please find the Operator Experience Metrics report attached.
<br><br>
''' + acq_df_table + '''
<br><br>
''' + pay_df_table + '''
<br><br>
''' + foexp_df_table + '''


<b>Regards,<b><br>
<b>Mohit Kumar<b>
'''


# souyqpeumvnuwxnk

smtp_username = 'mohit.kumar@wheelseye.com'
smtp_password = 'uncygtqiplrtoare'  # Use the generated app password

# Call the function to send the email
send_email(sender_email, receiver_emails, subject, body, smtp_username, smtp_password)


# In[51]:


cx_report= gc.open_by_url('https://docs.google.com/spreadsheets/d/1KLVxGoeUJcyjx-v2mO2bh6iVqqhGd5C4ud8iXiOSZHk/edit?gid=1898574702#gid=1898574702').worksheet('Dashboard_CX')


# In[52]:


data = cx_report.get_all_values()
cx_report = pd.DataFrame(data)


# In[ ]:


cx_report


# In[ ]:


ratings_df = cx_report.iloc[0:14]

ratings_df.columns=['', '', '', '', '', '', '', '', '',
                                     '','','', '', '', '','','','']
ratings_df


# In[ ]:


escalation_df = cx_report.iloc[14:23]
escalation_df.columns=['', '', '', '', '', '', '', '', '',
                                     '','','', '', '', '','','','']
escalation_df


# In[ ]:


sla_df = cx_report.iloc[23:]
sla_df.columns=['', '', '', '', '', '', '', '', '',
                                     '','','', '', '', '','','','']
sla_df


# In[ ]:


# Reset index for each DataFrame if needed
ratings_df.reset_index(drop=True, inplace=True)
escalation_df.reset_index(drop=True, inplace=True)
sla_df.reset_index(drop=True, inplace=True)


# In[ ]:


import pandas as pd
from pretty_html_table import build_table  # Import build_table function from pretty_html_table
from datetime import datetime

# Assuming combined_df is your DataFrame

# Convert DataFrame to HTML table
ratings_df_table = build_table(ratings_df, 'blue_dark', padding='5px 20px 5px 5px', text_align='left',
                                even_color='black', even_bg_color='white', border_bottom_color='blue_light',
                                font_size='10px', index=False)

escalation_df_table = build_table(escalation_df, 'blue_dark', padding='5px 20px 5px 5px', text_align='left',
                                even_color='black', even_bg_color='white', border_bottom_color='blue_light',
                                font_size='10px', index=False)

sla_df_table = build_table(sla_df, 'blue_dark', padding='5px 20px 5px 5px', text_align='left',
                                even_color='black', even_bg_color='white', border_bottom_color='blue_light',
                                font_size='10px', index=False)


# In[ ]:


from IPython.display import HTML

# Display the HTML content
HTML(sla_df_table)


# In[ ]:


import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime

def send_email(sender_email, receiver_emails, subject, body, smtp_username, smtp_password):
    message = MIMEMultipart()
    message['From'] = sender_email
    message['To'] = ', '.join(receiver_emails)
    message['Subject'] = subject

    message.attach(MIMEText(body, 'html'))

    smtp_server = 'smtp.gmail.com'
    smtp_port = 587

    server = smtplib.SMTP(smtp_server, smtp_port)
    server.starttls()
    server.login(smtp_username, smtp_password)
    server.sendmail(sender_email, receiver_emails, message.as_string())

    server.quit()

# Define your variables
sender_email = 'mohit.kumar@wheelseye.com'
#receiver_emails = ['mohit.kumar@wheelseye.com']

 receiver_emails = ['mohit.kumar@wheelseye.com','guneet.singh@wheelseye.com','nitin.kukna@wheelseye.com',
                   'arjun.kotwal@wheelseye.com','saurabh.bharti@wheelseye.com','himani.jadaun@wheelseye.com',
                     'harish.laddha@wheelseye.com','manish.somani@wheelseye.com','anshul.mimani@wheelseye.com',
                      'saikat.mukhopadhyay@wheelseye.com','mohit.aggarwal@wheelseye.com','sahil.bansal@wheelseye.com',
                       'nishant.kumar@wheelseye.com','animesh.gautam@wheelseye.com','yash.chandan@wheelseye.com','randheer.singh@wheelseye.com']
subject = 'Shipper Service & Experience Reports - {}'.format(datetime.now().strftime('%Y-%m-%d'))

# Define your HTML body
# (Assuming cx_report_tab is defined elsewhere)
body = '''
Hi,
Please find the Shipper Experience Metrics report attached

''' + ratings_df_table + '''
<br><br>
''' + escalation_df_table + '''
<br><br>
''' + sla_df_table + '''

<b>Regards,<b><br>
<b>Mohit Kumar<b>
'''

smtp_username = 'mohit.kumar@wheelseye.com'
smtp_password = 'uncygtqiplrtoare'  # Please replace with your actual password

# Call the function to send the email
send_email(sender_email, receiver_emails, subject, body, smtp_username, smtp_password)


# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:





# In[ ]:




