from pyspark.sql import SparkSession
import kagglehub
import os

os.environ['KAGGLEHUB_CACHE'] = '/opt/airflow/data/cache'

spark = SparkSession.builder.getOrCreate()

path = kagglehub.dataset_download('shashwatwork/dataco-smart-supply-chain-for-big-data-analysis')

csv = os.path.join(path, 'DataCoSupplyChainDataset.csv')

df = spark.read.csv(csv, header=True, inferSchema=True, quote='"', escape='"', multiLine=True)

df.write.mode('overwrite').csv('/opt/airflow/data/raw', header=True)