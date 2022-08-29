#!/usr/bin/python
# -*- coding: UTF-8 -*-

import os,sys,string
from multiprocessing import Process
import pandas as pd
from functools import reduce
import hashlib
from pathlib import Path

DefaultPath='/opt/spark/work-dir/result/ccf'
MD5='md5'
Dcolumns=['uuid','ip','hostname','requests','name','city','job','phonenum']

def getParquetFiles(dirpath):
    result=[]
    if os.path.exists(dirpath):
        for filepath,dirnames,filenames in os.walk(dirpath):
            for filename in filenames:
                if filename.startswith("part") and filename.endswith(".parquet"):
                    result.append(dirpath+"/"+filename)
        return result
    else:
        print("File path is not existed")
        sys.exit()

def mergeParquets(dirpath):
    data_dir = Path(dirpath)
    finaldf = pd.concat(
        pd.read_parquet(parquet_file)
        for parquet_file in data_dir.glob('*.parquet')
    )
    md5str=sortDFAndMD5(finaldf)
    if md5str==MD5:
        print('true')
    else:
        print('false')

def sortParquetFile(dirpath):
    #df=pd.read_parquet(filepath)
    files=getParquetFiles(dirpath)
    result=[]
    for item in files:
        temp=pd.read_parquet(item)
        result.append(temp)
    finaldf=reduce(lambda left,right: pd.merge(left,right,on=Dcolumns,how='outer'),result)
    md5str=sortDFAndMD5(finaldf)
    print(md5str)
def sortDFAndMD5(df):
    res=df.sort_values(by='uuid')
    #res.to_csv("result",index=False)
    md5str=hashlib.sha256(res.to_csv(index=False).encode()).hexdigest()
    return md5str
if __name__ == "__main__":
     Action="merge" 
     if len(sys.argv)>3:
        DefaultPath=sys.argv[1] 
        Action=sys.argv[2] 
        MD5=sys.argv[3]
     else:
        print('usage: python CCFCheck.py /opt/path merge md5str')
     if Action.lower()=='merge':
        mergeParquets(DefaultPath)
     else:
        sortParquetFile(DefaultPath)
