#!/usr/bin/env python 
# -*- coding: utf-8 -*- 
# @Time : 2020/10/19 14:40 
# @Author : magician 
# @File : get_distance.py 
# @Software: PyCharm
import sys
from math import radians, cos, sin, asin, sqrt


def get_distance(lng1,lat1,lng2,lat2):
    """
    计算两点距离
    :param lng1:
    :param lat1:
    :param lng2:
    :param lat2:
    :return:两点距离
    """
    lng1, lat1, lng2, lat2 = map(radians, [float(lng1), float(lat1), float(lng2), float(lat2)]) # 经纬度转换成弧度
    dlon=lng2-lng1
    dlat=lat2-lat1
    a=sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    distance=int(2*asin(sqrt(a))*6378.137*1000) # 地球平均半径，6378.137km。

    return distance


try:
    for line in sys.stdin:
        arr = line.strip().split("\t")

        radLng1 = arr[1]
        radLat1 = arr[2]
        radlng2 = arr[4]
        radLat2 = arr[5]
        distance = get_distance(radLng1, radLat1, radlng2, radLat2)
        print("\t".join(arr)+"\t"+distance)

except:
    #In case of an exception, write the stack trace to stdout so that we
    #can see it in Hive, in the results of the UDF call.
    print (sys.exc_info())
