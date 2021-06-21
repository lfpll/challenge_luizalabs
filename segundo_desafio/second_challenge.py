from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql import functions as F

now = datetime.now()
spark = SparkSession.builder.appName("first_challenge").getOrCreate()
sc = spark.sparkContext
black_friday_dates = ["2020-11-27","2019-11-29","2018-11-23","2017-11-24","2016-11-25"]

df = spark.read.csv("./clientes_pedidos.csv",header=True)

# Correcting datatype of column data_pedido
df_with_time = df.withColumn("date_pedido",F.from_unixtime("data_pedido","yyyy-MM-dd").cast('date'))

# Adding age
df_with_age  = df_with_time.withColumn('age',(F.months_between(F.lit(now),
                                        F.col('data_nascimento_cliente'),True)/12).cast('int'))

# Buyers who bougth on black friday
buyers_from_bf = df_with_age.where(F.col('date_pedido').isin(black_friday_dates))

# Join pedido with date
joined_date_df =  buyers_from_bf.withColumn("joined_pedido",F.array("codigo_pedido","date_pedido"))

# Aggregated values by rules set on test
joined_agg = joined_date_df.where('age > 30').groupBy(
    'codigo_cliente','age').agg(
    F.collect_list("joined_pedido").cast("string").alias("lista_pedidos"),
    F.count("joined_pedido").alias("numero_pedidos")).where("numero_pedidos > 2")


joined_agg.coalesce(1).write.mode("overwrite").option("header","true").csv("./clientes_count_pedidos.csv")