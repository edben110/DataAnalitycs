from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

spark = SparkSession.builder \
    .appName("estudio") \
    .getOrCreate()
df = spark.read.csv("ventas_colombia_sucio.csv",header=True,inferSchema=True)

df.createOrReplaceTempView("ESTUDIO")#convierte el dataset en tablas sql

df.printSchema()

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

df.groupBy("city").agg(sum("quantity").alias("cantidad")).orderBy(col("cantidad").desc()).show()
df.select("city").distinct().show()

