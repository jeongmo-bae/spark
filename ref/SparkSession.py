import os
import sys
import warnings
warnings.filterwarnings('ignore')

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

import databricks.koalas as ks


class SparkToHDFS:
    '''
    sc = spark.spark.sparkContext
    uri = sc._gateway.jvm.java.net.URI
    path = sc._gateway.jvm.org.apache.hadoop.fs.Path
    fs = sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
    ff = fs.get(uri('hdfs://~~~~~~~~~~~~'), sc._jsc.hadoopConfiguration())

    for f in ff.listStatus(path('hdfs://~~~~~~~~~~~~')):
        print(f.getPath())

    ff.exists(path('/~~~~~~~~~~~~'))
    '''

    def __init__(self, sc, base_url='hdfs://~~~~~~~~~~~~'):
        self.base_url = base_url
        self.sc = sc
        self.URI = self.sc._gateway.jvm.java.net.URI
        self.Path = self.sc._gateway.jvm.org.apache.hadoop.fs.Path

        self.FileSystem = self.sc._gateway.jvm.org.apache.hadoop.fs.FileSystem
        self.FileSystem = self.FileSystem.get(self.URI(self.base_url), sc._jsc.hadoopConfiguration())

    def print_file_lists(self, url=None):
        if url is None:
            url = self.base_url

        try:
            for f in self.FileSystem.listStatus(self.Path(url)):
                print(f)
        except Exception:
            return 'FAIL'

    def write_parquet_to_hdfs(self, sdf, parquet_name, mode='overwrite'):
        # self.clear_cache()
        url = self.base_url

        try:
            sdf.write.mode(mode).format('parquet').save(f'{url}/{parquet_name}')
            return 'SUCCESS'
        except Exception:
            return 'FAIL'

    def delete_parquet_from_hdfs(self, parquet_name, url=None):
        if url is None:
            url = self.base_url

        file_url = f'{url}/{parquet_name}'

        try:
            self.FileSystem.delete(self.Path(file_url))
        except Exception:
            return 'FAIL'


class SparkDB(DbInfo):

    def __init__(self, spark_app_name='~~~~~', bng_yn=False):

        super().__init__()
        self.spark_app_name = spark_app_name
        self.bng_yn = bng_yn

        # sybase
        self.sybase_url = 'jdbc:sybase:Tds:~~~~~~~~~~~~'
        self.sybase_properties = {
            'user': self._ID1
            , 'password': self._PW1
            , 'driver': 'com.sybase.jdbc4.jdbc.SybDriver'
        }

        # oracle
        self.oracle_url = 'jdbc:oracle:thin:@~~~~~~~~~~~~'
        self.oracle_properties = {
            'user': self._ID2
            , 'password': self._PW2
            , 'driver': 'oracle.jdbc.driver.OracleDriver'
        }

        # impala
        self.impala_url = 'jdbc:impala://~~~~~~~~~~~~/default;AuthMech=3;UseNativeQuery=0;SSL=1;sslTrustStore=/user-home/_global_/dbdrivers/jssecacerts'
        self.impala_properties = {
            'user': self._ID3
            , 'password': self._PW3
            , 'driver': 'com.cloudera.impala.jdbc41.Driver'
        }

        # spark
        self.get_spark()

    def get_spark(self):
        ### DB jar 위치
        jar_dir = '//user-home//_global_//dbdrivers'
        jars = ['jconn4.jar', 'ojdbc6.jar', 'ImpalaJDBC41.jar']  # sybase, oracle, impala
        classpaths = ''

        for jar in jars:
            classpaths += f'{jar_dir}//{jar}:'

        ### 경로 (운영 서버)
        cluster = 'datalake'
        THRIFT_URL = '~~~~~~~~~~~~'
        # THRIFT_URL = '~~~~~~~~~~~~'
        PORT = '9083'
        URL_1 = '~~~~~~~~~~~~'
        URL_2 = '~~~~~~~~~~~~'

        HADOOP_USER = self._BNGID if self.bng_yn else self._ID3
        os.environ['HADOOP_USER_NAME'] = HADOOP_USER

        ### 리소스 설정
        driver_cores = 4          # 스파크 드라이버 코어 수
        driver_memory = '32G'     # 스파크 드라이버 메모리
        max_result_size = '3G'    # 스파크 드라이버 최대결과 크기
        executor_cores = 3        # 스파크 익스큐터 코어 수
        executor_memory = '32G'    # 스파크 익스큐터 메모리
        executor_instances = 3    # 스파크 익스큐터 인스턴스

        spark_settings = [
            # Hive 메타스토어 연결
            ('hive.metastore.uris', f'thrift://{THRIFT_URL}:{PORT}')
            # Pyarrow 연결
            , ("spark.sql.execution.arrow.enabled ", "true")
            # Metastore PARQUET FileFormat을 해당 테이블에 기본 Serde로 읽도록 설정
            , ("spark.sql.hive.convertMetastoreParquet", "false")
            # crossjoin 사용을 위한 설정
            , ("spark.sql.crossJoin.enabled", "true")
            # spark ui 미사용 설정
            , ("spark.ui.enabled", "true")
            # insert overwrite 시 동적 파티션 설정
            , ("hive.exec.dynamic.partition", "true")
            , ("hive.exec.dynamic.partition.mode", "nonstrict")
            # insert overwrite 시 형변환 관련 설정
            , ('spark.sql.storeAssignmentPolicy', 'legacy')
            # HDFS DFS 사용을 위한 추가
            , ("hive.input.dir.recursive", 'true')
            , ("hive.mapred.supports.subdirectories", 'true')
            , ("hive.supports.subdirectories", 'true')
            , ('mapreduce.input.fileinputformat.input.dir.recursive', 'true')
            , ('dfs.client.use.datanode.hostname', 'true')
            # spark 리소스 설정
            , ('spark.driver.cores', driver_cores)
            , ('spark.driver.memory', driver_memory)
            , ('spark.driver.maxResultSize', max_result_size)
            , ('spark.executor.cores', executor_cores)
            , ('spark.executor.memory', executor_memory)
            , ('spark.executor.instances', executor_instances)
            # show 대체용 pandas 처럼 Output 나옴
            , ('spark.sql.repl.eagerEval.enabled', 'true')
            # 스파크 jar 추가
            # , ('spark.jars', ', '.join(jars))
            , ('spark.driver.extraClassPath', classpaths)
            , ('spark.executor.extraClassPath', classpaths)
            # 스파크 timezone 설정
            #, ('spark.sql.session.timeZone', 'UTC')
        ]

        app_name = f"{self.spark_app_name}"

        conf = (
            SparkConf()
            .setAppName(app_name)
            .setAll(spark_settings)
        )

        self.spark = (
            SparkSession
            .builder
            .config(conf=conf)
            .enableHiveSupport()
            .getOrCreate()
        )

        # 하둡 configuration
        self.spark._jsc.hadoopConfiguration().set("dfs.nameservices", cluster)
        self.spark._jsc.hadoopConfiguration().set(f"dfs.ha.namenodes.{cluster}", "nn1,nn2")
        self.spark._jsc.hadoopConfiguration().set(f"dfs.namenode.rpc-address.{cluster}.nn1", f"{URL_1}:8020")
        self.spark._jsc.hadoopConfiguration().set(f"dfs.namenode.rpc-address.{cluster}.nn2", f"{URL_2}:8020")
        self.spark._jsc.hadoopConfiguration().set(f"dfs.client.failover.proxy.provider.{cluster}",
                                      "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider")

    def read_sybase_sql(self, sql, alias='t'):
        return self.spark.read.jdbc(url=self.sybase_url, table=f'({sql}) {alias}', properties=self.sybase_properties)

    def read_oracle_sql(self, sql, alias='t'):
        return self.spark.read.jdbc(url=self.oracle_url, table=f'({sql}) {alias}', properties=self.oracle_properties)

    def read_hive_sql(self, sql):
        return self.spark.sql(sql)

    def read_impala_sql(self, sql, alias='t'):
        return self.spark.read.jdbc(url=self.impala_url, table=f'({sql}) {alias}', properties=self.impala_properties)

    def read_sql(self, sql, conn, alias='t', to_koalas=False):
        self.clear_cache()

        if conn == 1:
            return self.read_sybase_sql(sql, alias).to_koalas() if to_koalas else self.read_sybase_sql(sql, alias)
        elif conn == 2:
            return self.read_oracle_sql(sql, alias).to_koalas() if to_koalas else self.read_oracle_sql(sql, alias)
        elif conn == 3:
            return self.read_hive_sql(sql).to_koalas() if to_koalas else self.read_hive_sql(sql)
        elif conn == 4:
            return self.read_impala_sql(sql, alias).to_koalas() if to_koalas else self.read_impala_sql(sql, alias)

    def write_sql(self, sdf, table_name, mode='overwrite'):
        self.clear_cache()

        '''
        mode: append, overwrite, ignore
        '''
        try:
            sdf.write.jdbc(
                url=self.oracle_url
                , table=f'{table_name}'
                , mode=f'{mode}'
                , properties=self.oracle_properties
            )
        except Exception as e:
            print(e)
            raise '테이블 생성 오류!'

    def read_parquet(self, parquet_name):
        self.clear_cache()

        return self.spark.read.parquet(parquet_name)

    def write_parquet(self, sdf, parquet_name, mode='overwrite'):
        self.clear_cache()

        try:
            sdf.write.mode(mode).format('parquet').save(parquet_name)
            return 'SUCCESS'
        except Exception as e:
            print(e)
            return 'FAIL'

    def to_csv(self, sdf, csv_dir, csv_output_dir=None, header=False, encoding='cp949', mode='overwrite', concat_files=True):
        self.clear_cache()

        try:
            if concat_files:
                sdf = sdf.coalesce(1)

            sdf.write.csv(
                csv_dir
                , header=header
                , encoding=encoding
                , mode=mode
            )

            if csv_output_dir:
                os.system(f'cat {csv_dir}/p* > {csv_output_dir}')
            else:
                os.system(f'cat {csv_dir}/p* > {csv_dir}')
        except Exception as e:
            print(e)
            return 0

        return 1

    def create_in_hive(self):
        self.clear_cache()

        '''
            TODO
        '''
        sql = """
            CREATE TABLE ~~~~~~~~~~~~.test (
                baseym STRING COMMENT '기준년월'
                )
                PARTITIONED BY (p_yyyymm STRING)
                STORED AS parquet
                LOCATION 'hdfs://~~~~~~~~~~~~';
        """
        return

    def clear_cache(self):
        self.spark.catalog.clearCache()


__all__ = [
    'pd'
    , 'SparkSession'
]
