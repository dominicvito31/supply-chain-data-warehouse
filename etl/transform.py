from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, concat_ws, md5, pandas_udf, split, date_format, year, month, dayofmonth, quarter
from pyspark.sql.types import StringType, BooleanType
import reverse_geocoder as rg
import pycountry
import pandas as pd


spark = SparkSession.builder.getOrCreate()

df = spark.read.csv('/opt/airflow/data/raw', header=True, inferSchema=True, quote='"', escape='"', multiLine=True)

df = df.withColumn('order_date', to_timestamp(col('order date (DateOrders)'), 'M/d/yyyy H:mm')) \
       .withColumn('shipping_date', to_timestamp(col('shipping date (DateOrders)'), 'M/d/yyyy H:mm')) \
       .withColumn('Order Item Id', col('Order Item Id').cast(StringType())) \
       .withColumn('Order Id', col('Order Id').cast(StringType())) \
       .withColumn('Customer Id', col('Customer Id').cast(StringType())) \
       .withColumn('Product Card Id', col('Product Card Id').cast(StringType())) \
       .withColumn('Department Id', col('Department Id').cast(StringType())) \
       .withColumn('Customer Zipcode', col('Customer Zipcode').cast(StringType())) \
       .withColumn('Late_delivery_risk', col('Late_delivery_risk').cast(BooleanType()))

df = df.withColumn('order_date_key', date_format('order_date', 'yyyyMMdd')) \
       .withColumn('shipping_date_key', date_format('shipping_date', 'yyyyMMdd')) \
       .withColumn('customer_location_key', md5(concat_ws('_', col('Customer City'), col('Customer State'), col('Customer Country')))) \
       .withColumn('market_location_key', md5(concat_ws('_', col('Order City'), col('Order State'), col('Order Country')))) \
       .withColumn('shipping_key', md5(concat_ws('_', col('Shipping Mode'), col('Days for shipment (scheduled)'), col('Days for shipping (real)'), col('Delivery Status'), col('Late_delivery_risk')))) \
       .withColumn('market_key', md5(concat_ws('_', col('Market'), col('Order Region'))))

dim_customer = df.select(col('Customer Id').alias('customer_id'),
                         col('customer_location_key').alias('location_key'),
                         col('Customer Fname').alias('first_name'),
                         col('Customer Lname').alias('last_name'),
                         col('Customer Segment').alias('segment'),
                         col('Customer Zipcode').alias('zipcode')
                        ).dropDuplicates(['customer_id'])

dim_product = df.select(col('Product Card Id').alias('product_id'),
                        col('Product Name').alias('product_name'),
                        col('Category Name').alias('category')
                        ).dropDuplicates(['product_id'])

dim_shipping = df.select(col('shipping_key'),
                         col('Shipping Mode').alias('shipping_mode'),
                         col('Days for shipment (scheduled)').alias('days_scheduled'),
                         col('Days for shipping (real)').alias('days_real'),
                         col('Delivery Status').alias('delivery_status'),
                         col('Late_delivery_risk').alias('late_delivery_risk')
                        ).dropDuplicates(['shipping_key'])

def rev_geocoder(lat, long):
    coords = list(zip(lat, long))
    
    results = rg.search(coords)

    output_list = []
    for res in results:
        city = res['name']
        state = res['admin1']
        cc = res['cc']

        try:
            country_name = pycountry.countries.get(alpha_2=cc).name
        except:
            country_name = cc

        output_list.append(f'{city}|{state}|{country_name}')

    return pd.Series(output_list)
geo_udf = pandas_udf(rev_geocoder, StringType())
store_geo = df.withColumn('temp_geo', geo_udf(col('Latitude'), col('Longitude'))) \
              .withColumn('store_city', split(col('temp_geo'), r'\|').getItem(0)) \
              .withColumn('store_state', split(col('temp_geo'), r'\|').getItem(1)) \
              .withColumn('store_country', split(col('temp_geo'), r'\|').getItem(2)) \
              .withColumn('store_location_key', md5(concat_ws('_', col('store_city'), col('store_state'), col('store_country'))))
dim_store = store_geo.select(col('Department Id').alias('department_id'),
                             col('store_location_key').alias('location_key'),
                             col('Department Name').alias('department'),
                             col('Latitude').alias('latitude'),
                             col('Longitude').alias('longitude')
                            ).dropDuplicates(['department_id'])

dim_market = df.select(col('market_key'),
                       col('market_location_key').alias('location_key'),
                       col('Market').alias('market'),
                       col('Order Region').alias('region')
                      ).dropDuplicates(['market_key'])

customer_location = df.select(col('customer_location_key').alias('location_key'),
                              col('Customer City').alias('city'),
                              col('Customer State').alias('state'),
                              col('Customer Country').alias('country'))
market_location = df.select(col('market_location_key').alias('location_key'),
                            col('Order City').alias('city'),
                            col('Order State').alias('state'),
                            col('Order Country').alias('country'))
store_location = store_geo.select(col('store_location_key').alias('location_key'),
                                  col('store_city').alias('city'),
                                  col('store_state').alias('state'),
                                  col('store_country').alias('country'))
dim_location = customer_location.union(market_location).union(store_location).distinct()

dates = df.select(col('order_date').alias('date')) \
          .union(df.select(col('shipping_date').alias('date'))) \
          .distinct()
dim_date = dates.withColumn('date_key', date_format('date', 'yyyyMMdd')) \
                .withColumn('year', year('date')) \
                .withColumn('month', month('date')) \
                .withColumn('day', dayofmonth('date')) \
                .withColumn('quarter', quarter('date'))
dim_date = dim_date.dropDuplicates(['date_key'])

fact_order_item = df.select(col('Order Item Id').alias('order_item_id'),
                            col('Order Id').alias('order_id'),
                            col('Customer Id').alias('customer_id'),
                            col('Product Card Id').alias('product_id'),
                            col('market_key'),
                            col('Department Id').alias('department_id'),
                            col('shipping_key'),
                            col('order_date_key'),
                            col('shipping_date_key'),
                            col('Order Item Quantity').alias('quantity'),
                            col('Sales per customer').alias('sales'),
                            col('Order Item Discount').alias('discount_amount'),
                            col('Order Item Discount Rate').alias('discount_rate'),
                            col('Order Item Product Price').alias('product_price'),
                            col('Order Profit Per Order').alias('profit'),
                            col('Order Item Profit Ratio').alias('profit_ratio'),
                            col('Benefit per order').alias('benefit_per_order'),
                            col('Order Status').alias('order_status'))

dim_location.write.mode('overwrite').parquet('/opt/airflow/data/clean/dim_location')
dim_customer.write.mode('overwrite').parquet('/opt/airflow/data/clean/dim_customer')
dim_product.write.mode('overwrite').parquet('/opt/airflow/data/clean/dim_product')
dim_shipping.write.mode('overwrite').parquet('/opt/airflow/data/clean/dim_shipping')
dim_store.write.mode('overwrite').parquet('/opt/airflow/data/clean/dim_store')
dim_market.write.mode('overwrite').parquet('/opt/airflow/data/clean/dim_market')
dim_date.write.mode('overwrite').parquet('/opt/airflow/data/clean/dim_date')
fact_order_item.write.mode('overwrite').parquet('/opt/airflow/data/clean/fact_order_item')