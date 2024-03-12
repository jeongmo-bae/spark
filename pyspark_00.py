import os
import shutil
import sys
import json
import time
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
from io import BytesIO,StringIO
import jaydebeapi as jp
import warnings
warnings.filterwarnings('ignore')

os.system('pip3 install hdfs')
from hdfs.config import Config
config = Config()
config.add_section(config.global_section)
section = 'cdp.alias'
config.add_section(section)
config.set(section, 'url', 'http://~~~~~~~~~~~~/;http://~~~~~~~~~~~~')
config.set(section, 'user', '~~~~~~~~~~~~')
client = config.get_client('cdp')

os.chdir("/project_data/data_asset")
from useful.db_conn import * 

#pyspark
os.system('pip install pyspark==3.1.2 hdfs==2.7.0 koalas==1.8.1 python-dotenv==0.20.0 memory_profiler==0.60.0 dython==0.6.6 pyathena==2.9.6 pyathenajdbc==3.0.1')
from useful.libs import *  
db = DB()
sp = SparkDB('hive_table')
sp.spark.conf.set('spark.sql.parquet.writeLegacyFormat', 'TRUE')
db = DB()
conn = db.hconn
curr = conn.cursor()



sparkDataFrame = sp.spark.read.parquet(f'hdfs://datalake/user/~~~~~~~~~~~~/UploadData/ACRM/TSMFCRM70_ALL/20230315_crm70_all.parquet')
sparkDataFrame.count()


sparkDataFrame.rdd.getNumPartitions()
#print(sparkDataFrame.rdd.getNumPartitions())

sparkDataFrame.columns

new_column_name = [
    'acmplcmpgnid',
    'sendpshistidnfr',
    'sendymd',
    'msgcretncd',
    'custidnfr',
    'custmgtno',
    'custnm',
    'sendbrncd',
    'sendempid',
    'notimsgmdiadstcd',
    'notimsgmdiadtalsdstcd',
    'rcverinfoctnt',
    'notisendstusdstcd',
    'sendmsgtitlctnt',
    'sendmsgctnt',
    'sendsvcdsticctnt',
    'sendsvcno',
    'bzwkrceptidnfiid',
    'syslastuno',
    'flbcmdiactnt',
    'flbcyn',
    'newstarbnkgpushlagclsfidstcd',
    'newstarbnkgpushmedmclsfidstcd',
    'nttksendstusdstcd',
    'nttksendrsultdstcd',
    'nttktmpltid',
    'frndtksendstusdstcd',
    'frndtksendrsultdstcd'
]
for i ,col_name in enumerate(sparkDataFrame.columns) :
    sparkDataFrame = sparkDataFrame.withColumnRenamed(col_name,new_column_name[i])

sparkDataFrame.printSchema()

sparkDataFrame = sparkDataFrame.withColumn(
    'p_yyyymmdd'
    , F.col('sendymd')
)
sparkDataFrame = sparkDataFrame.drop("custnm")
sparkDataFrame

sparkDataFrame.write.option('header', True).partitionBy('p_yyyymmdd').format('parquet').mode('overwrite').save(f'hdfs://datalake/user/2360851/UploadTable/tsmfcrm70_all')

client.list(f'/user/~~~~~~~~~~~~/UploadTable/tsmfcrm70_all/')

len(client.list(f"/user/~~~~~~~~~~~~/UploadTable/tsmfcrm70_all/{client.list(f'/user/~~~~~~~~~~~~/UploadTable/tsmfcrm70_all/')[1]}" ))

data_urls = client.list(base_url)[1:]
data_urls


base_url = '/user/~~~~~~~~~~~~/UploadTable/tsmfcrm70_all'
data_urls = client.list(base_url)[1:]

for p_yyyymmdd in data_urls:
    time.sleep(0.1)
    k1, v1 = p_yyyymmdd.split('=')[0], p_yyyymmdd.split('=')[1]

    curr.execute(
        f"""
            LOAD DATA INPATH '/user/~~~~~~~~~~~~/UploadTable/tsmfcrm70_all/{p_yyyymmdd}' 
            OVERWRITE INTO TABLE project_dma.tsmfcrm70 
            PARTITION({k1}='{v1}')
        """
    )