#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time : 2021/1/6 16:01 
# @Author : magician 
# @File : get_lost_uli.py 
# @Software: PyCharm


from pyspark.shell import sqlContext
sqlContext.sql("set hive.exec.dynamic.partition=true")
sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
sqlContext.sql("set hive.exec.compress.output=true")
sqlContext.sql("set parquet.compression=snappy")


import datetime
def get_week_of_month(year, month, day):
    """
    获取指定的某天是某个月的第几周
    周一为一周的开始
    实现思路：就是计算当天在本年的第y周，本月一1号在本年的第x周，然后求差即可。
    因为查阅python的系统库可以得知：

    """

    begin = int(datetime.date(year, month, 1).strftime("%W"))
    end = int(datetime.date(year, month, day).strftime("%W"))

    return end - begin + 1

def choose_mode(number_week):

    if number_week==1:
        #每月第一周
        hours=[0,1,8,9,16,17]
        for hour in hours:
            insert_sql="insert overwrite table ulidatabase.lost_uli_union partition (c_load_time_part) select c_cdrtype,c_spcode,c_ascode,c_imsi,c_u_jmflag,c_usernum,c_imei,c_lac,c_ci,c_bsid,c_uli,c_areacode,c_homecode,c_lng,c_lat,c_uli_addr,c_timestamp,c_begintime,c_alert_time,c_connect_time,c_endtime,c_event_flag,c_cftype,c_disconcause,c_r_jmflag,c_relatenum,c_rspcode,c_rhomecode,c_cfnum,c_dtmf,c_smscstamp,c_msgparts,c_msgpartnum,c_content,c_old_lac,c_ue_category,c_load_time_part,hour from ( select *, row_number() over (partition by c_uli order by c_timestamp) as topn from original.t_cdr_k where c_load_time_part=%s and hour =%s and c_uli is not null) a where topn=1"%(time,hour)
            print(insert_sql)
            sqlContext.sql("%s"%insert_sql)
    elif number_week==2:
        #每月第2周
        hours=[2,3,10,11,18,19]
        for hour in hours:
            insert_sql="insert overwrite table ulidatabase.lost_uli_union partition (c_load_time_part) select c_cdrtype,c_spcode,c_ascode,c_imsi,c_u_jmflag,c_usernum,c_imei,c_lac,c_ci,c_bsid,c_uli,c_areacode,c_homecode,c_lng,c_lat,c_uli_addr,c_timestamp,c_begintime,c_alert_time,c_connect_time,c_endtime,c_event_flag,c_cftype,c_disconcause,c_r_jmflag,c_relatenum,c_rspcode,c_rhomecode,c_cfnum,c_dtmf,c_smscstamp,c_msgparts,c_msgpartnum,c_content,c_old_lac,c_ue_category,c_load_time_part,hour from ( select *, row_number() over (partition by c_uli order by c_timestamp) as topn from original.t_cdr_k where c_load_time_part=%s and hour =%s and c_uli is not null) a where topn=1"%(time,hour)
            print(insert_sql)
            sqlContext.sql("%s"%insert_sql)
    elif number_week==3:
        #每月第3周
        hours=[4,5,12,13,20,21]
        for hour in hours:
            insert_sql="insert overwrite table ulidatabase.lost_uli_union partition (c_load_time_part) select c_cdrtype,c_spcode,c_ascode,c_imsi,c_u_jmflag,c_usernum,c_imei,c_lac,c_ci,c_bsid,c_uli,c_areacode,c_homecode,c_lng,c_lat,c_uli_addr,c_timestamp,c_begintime,c_alert_time,c_connect_time,c_endtime,c_event_flag,c_cftype,c_disconcause,c_r_jmflag,c_relatenum,c_rspcode,c_rhomecode,c_cfnum,c_dtmf,c_smscstamp,c_msgparts,c_msgpartnum,c_content,c_old_lac,c_ue_category,c_load_time_part,hour from ( select *, row_number() over (partition by c_uli order by c_timestamp) as topn from original.t_cdr_k where c_load_time_part=%s and hour =%s and c_uli is not null) a where topn=1"%(time,hour)
            print(insert_sql)
            sqlContext.sql("%s"%insert_sql)
    elif number_week==4:
        #每月第4周
        hours=[6,7,14,15,22,23]
        for hour in hours:
            insert_sql="insert overwrite table ulidatabase.lost_uli_union partition (c_load_time_part) select c_cdrtype,c_spcode,c_ascode,c_imsi,c_u_jmflag,c_usernum,c_imei,c_lac,c_ci,c_bsid,c_uli,c_areacode,c_homecode,c_lng,c_lat,c_uli_addr,c_timestamp,c_begintime,c_alert_time,c_connect_time,c_endtime,c_event_flag,c_cftype,c_disconcause,c_r_jmflag,c_relatenum,c_rspcode,c_rhomecode,c_cfnum,c_dtmf,c_smscstamp,c_msgparts,c_msgpartnum,c_content,c_old_lac,c_ue_category,c_load_time_part,hour from ( select *, row_number() over (partition by c_uli order by c_timestamp) as topn from original.t_cdr_k where c_load_time_part=%s and hour =%s and c_uli is not null) a where topn=1"%(time,hour)
            print(insert_sql)
            sqlContext.sql("%s"%insert_sql)
    elif number_week==5:
        #每月第5周
        hours=[0,1,8,9,16,17]
        for hour in hours:
            insert_sql="insert overwrite table ulidatabase.lost_uli_union partition (c_load_time_part) select c_cdrtype,c_spcode,c_ascode,c_imsi,c_u_jmflag,c_usernum,c_imei,c_lac,c_ci,c_bsid,c_uli,c_areacode,c_homecode,c_lng,c_lat,c_uli_addr,c_timestamp,c_begintime,c_alert_time,c_connect_time,c_endtime,c_event_flag,c_cftype,c_disconcause,c_r_jmflag,c_relatenum,c_rspcode,c_rhomecode,c_cfnum,c_dtmf,c_smscstamp,c_msgparts,c_msgpartnum,c_content,c_old_lac,c_ue_category,c_load_time_part,hour from ( select *, row_number() over (partition by c_uli order by c_timestamp) as topn from original.t_cdr_k where c_load_time_part=%s and hour =%s and c_uli is not null) a where topn=1"%(time,hour)
            print(insert_sql)
            sqlContext.sql("%s"%insert_sql)

time=(datetime.date.today()+datetime.timedelta(days=-1)).strftime('%Y%m%d')
year=int(time[:4])
month=int(time[4:6])
day=int(time[6:])

print("本月第%s周"%(get_week_of_month(year,month,day)))
# print(get_week_of_month(2021,3,31))
number_week=get_week_of_month(year,month,day)
# number_week=get_week_of_month(2021,2,28)

choose_mode(number_week)