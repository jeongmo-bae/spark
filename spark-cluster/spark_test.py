from datetime import datetime
from pytz import timezone
from pyspark.sql import SparkSession
from pyspark.sql.functions import desc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType

baseyms = datetime.now(timezone('Asia/Seoul')).strftime("%Y%m%d%H%M%S")

spark = SparkSession\
    .builder\
    .appName(f"spark-test-{baseyms}")\
    .config("spark.eventLog.enabled", "true") \
    .config("spark.eventLog.dir", "/opt/spark/logs") \
    .config("spark.sql.shuffle.partitions", "4")\
    .getOrCreate()

schema = StructType([
    StructField("InvoiceNo", StringType(), True),
    StructField("StockCode", StringType(), True),
    StructField("Description", StringType(), True),
    StructField("Quantity", IntegerType(), True),
    StructField("InvoiceDate", TimestampType(), True),
    StructField("UnitPrice", DoubleType(), True),
    StructField("CustomerID", DoubleType(), True),
    StructField("Country", StringType(), True)
])
print("#####\n#####\n#####\n#####\n#####")

sdf = spark\
    .read\
    .option("header","True")\
    .schema(schema) \
    .csv("/opt/spark-data/retail-data/*.csv")
print("#####\n#####\n#####\n#####\n#####")

sdf = sdf\
    .drop_duplicates(['StockCode','CustomerID'])\
    .sort(desc("InvoiceDate"))
print("#####\n#####\n#####\n#####\n#####")

output_path = f"/opt/spark-data/spark-test-{baseyms}/"

# 결과 저장
sdf.write \
    .mode("overwrite") \
    .option("header", "True") \
    .option("encoding", "cp949") \
    .csv(output_path)