from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("estudio") \
    .getOrCreate()
df = spark.read.csv("ventas_colombia_sucio.csv",header=True,inferSchema=True)

df.createOrReplaceTempView("ESTUDIO")#convierte el dataset en tablas sql

df.printSchema()

df = df.filter((col("order_id").isNotNull()))
df = df.dropDuplicates(["order_id"])

df = df.withColumn("order_date",try_to_timestamp(col("order_date"),lit("yyyy-MM-dd HH:mm:ss")))
df = df.filter((col("order_date")>="2025-01-01")&(col("order_date")<"2026-01-01"))

df = df.filter(col("product").isNotNull())
df = df.filter((col("quantity")>=0)&(col("quantity")<=20))

df = df.filter((col("unit_price_cop")>0)&(col("unit_price_cop")<10000000))

df = df.filter((col("customer_age")>=18)&(col("customer_age")<=100))

df = df.filter((col("shipping_days")>=1)&(col("shipping_days")<=15))

df = df.filter((col("returned_qty")>=0)&(col("returned_qty")<= col("quantity")))

df = df.withColumn("totalVenta",col("quantity")*col("unit_price_cop"))


#df = df.filter(col("quantity").isNotNull() & (col("quantity") > 0) & (col("status")=="Entregado"))
#df = df.filter(col("returned_qty").isNotNull() & (col("returned_qty") > 0))
#df.groupBy("product").agg(sum("quantity").alias("totalPedidosEntregados")).orderBy(col("totalPedidosEntregados")).show()

#df = df.filter(col("status").isNotNull())
#df.groupBy("status").agg(avg("returned_qty").alias("promedioRetornados")).show()
#df.select("status","returned_qty").show()


df = df.withColumn("city", trim(col("city")))


df = df.withColumn(
    "city",
    when(lower(col("city")).isin("bogota", "bogotá"), "Bogotá")
    .when(lower(col("city")).isin("pasto"), "Pasto")
    .when(lower(col("city")).isin("cali"), "Cali")
    .when(lower(col("city")).isin("medellin", "medellín"), "Medellín")
    .when(lower(col("city")).isin("armenia"), "Armenia")
    .when(lower(col("city")).isin("MANIZALES", "manizales"), "Manizales")
    .when(col("city").isin("CARTAGENA", "Cartagena"), "Cartagena")
    .when(lower(col("city")).isin("barranquilla", "BARRANQUILLA"), "Barranquilla")
    .otherwise(col("city"))#es parte de la funcion when, se usa para evitar el retorno de nulos
)
df = df.filter(col("city").isNotNull())

#1
df.groupBy("product").agg(sum("quantity").alias("cantidad")).orderBy(col("cantidad").desc()).show()

#2
df.filter(col("status")=="Devuelto").agg(avg("returned_qty").alias("promedioDevueltos")).show()

#3
df.groupBy("city").agg(sum("quantity").alias("cantidad")).orderBy(col("cantidad").desc()).show()

#4
df.groupBy("category").agg(sum("totalVenta").alias("ventas")).orderBy(col("ventas").desc()).show()

#5
df.groupBy("channel").agg(sum("quantity").alias("Cantidad")).orderBy(col("Cantidad").desc()).show()

#6
df.groupBy("status").agg(avg("unit_price_cop")).show()

#7
df.groupBy("department").agg(sum("quantity").alias("cantidad")).orderBy(col("cantidad").desc()).show()

#8
df.groupBy("payment_method").agg(sum("quantity").alias("cantidad")).orderBy(col("cantidad").desc()).show()

#9
df.agg(avg("shipping_days").alias("diasEntrega")).show()

#10
df.groupBy("customer_type").agg(sum("unit_price_cop").alias("ventas")).show()
#df.groupBy("city").agg(sum("quantity").alias("cantidad")).orderBy(col("cantidad").desc()).show()
df.select("status").distinct().show()

print(df.count())

spark.stop()
