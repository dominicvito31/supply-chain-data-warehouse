from pyspark.sql import SparkSession


spark = SparkSession.builder \
                    .appName('WriteToPostgres') \
                    .config('spark.jars.packages', 'org.postgresql:postgresql:42.6.0') \
                    .getOrCreate()

dim_location = spark.read.parquet('/opt/airflow/data/clean/dim_location')
dim_customer = spark.read.parquet('/opt/airflow/data/clean/dim_customer')
dim_product = spark.read.parquet('/opt/airflow/data/clean/dim_product')
dim_shipping = spark.read.parquet('/opt/airflow/data/clean/dim_shipping')
dim_store = spark.read.parquet('/opt/airflow/data/clean/dim_store')
dim_market = spark.read.parquet('/opt/airflow/data/clean/dim_market')
dim_date = spark.read.parquet('/opt/airflow/data/clean/dim_date')
fact_order_item = spark.read.parquet('/opt/airflow/data/clean/fact_order_item')

postgres_url = 'jdbc:postgresql://neondb_connection_string'
postgres_properties = {
'user': 'neondb_user',
'password': 'neondb_password',
'driver': 'org.postgresql.Driver',
'ssl': 'true',
'sslmode': 'require'
}

dim_location.write.jdbc(url=postgres_url, table='dim_location', mode='append', properties=postgres_properties)
dim_date.write.jdbc(url=postgres_url, table='dim_date', mode='append', properties=postgres_properties)
dim_product.write.jdbc(url=postgres_url, table='dim_product', mode='append', properties=postgres_properties)
dim_shipping.write.jdbc(url=postgres_url, table='dim_shipping', mode='append', properties=postgres_properties)
dim_market.write.jdbc(url=postgres_url, table='dim_market', mode='append', properties=postgres_properties)
dim_store.write.jdbc(url=postgres_url, table='dim_store', mode='append', properties=postgres_properties)
dim_customer.write.jdbc(url=postgres_url, table='dim_customer', mode='append', properties=postgres_properties)
fact_order_item.write.jdbc(url=postgres_url, table='fact_order_item', mode='append', properties=postgres_properties)