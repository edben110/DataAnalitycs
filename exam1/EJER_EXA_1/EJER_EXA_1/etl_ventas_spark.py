import shutil
from pathlib import Path

import pyarrow.parquet as parquet
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, trim, lower, upper, regexp_replace, initcap, lit,
    try_to_timestamp, when, sum, avg, count, desc, round as spark_round
)
from pyspark.sql.types import IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("ETL_Ventas_Colombia") \
    .getOrCreate()


df = spark.read.option("header", True).option("inferSchema", True).csv(
    "ventas_colombia_sucio.csv"
)

print("\n=== Consulta 1: registros originales ===")
print("Registros originales:", df.count())


# normalización

df = df.withColumn("city", trim(lower(col("city"))))
df = df.withColumn(
    "city",
    when(col("city").isin("bogota", "bogotá"), "Bogotá")
    .when(col("city") == "medellin", "Medellín")
    .when(col("city") == "cali", "Cali")
    .when(col("city").isin("pasto"), "Pasto")
    .when(col("city").isin("barranquilla"), "Barranquilla")
    .when(col("city").isin("cartagena"), "Cartagena")
    .otherwise(initcap(col("city")))
)

df = df.withColumn("product", trim(col("product")))
df = df.withColumn(
    "product",
    when(lower(col("product")) == "smartphone x", "Smartphone X")
    .when(lower(col("product")) == "laptop pro14", "Laptop Pro 14")
    .when(lower(col("product")) == "audifonos bluetooth", "Audífonos Bluetooth")
    .when(lower(col("product")) == "mouse inalambrico", "Mouse Inalámbrico")
    .otherwise(col("product"))
)

df = df.withColumn(
    "order_date_ts",
    try_to_timestamp(col("order_date"), lit("yyyy-MM-dd HH:mm:ss"))
)


# elimina de registros/datos inválidos

clean = df.filter(
    col("order_id").isNotNull() &
    col("order_date_ts").isNotNull() &
    (col("order_date_ts") >= "2025-01-01") &
    (col("order_date_ts") < "2026-01-01") &
    col("city").isNotNull() &
    col("product").isNotNull() &
    col("quantity").isNotNull() &
    (col("quantity") > 0) &
    (col("quantity") <= 20) &
    col("unit_price_cop").isNotNull() &
    (col("unit_price_cop") > 0) &
    (col("unit_price_cop") < 10000000) &
    col("customer_age").isNotNull() &
    (col("customer_age") >= 18) &
    (col("customer_age") <= 100) &
    col("shipping_days").isNotNull() &
    (col("shipping_days") >= 1) &
    (col("shipping_days") <= 15) &
    col("returned_qty").isNotNull() &
    (col("returned_qty") >= 0) &
    (col("returned_qty") <= col("quantity"))
).dropDuplicates(["order_id"])

# Columna calculada
clean = clean.withColumn(
    "sales_cop",
    spark_round(col("quantity") * col("unit_price_cop"), 0)
)

clean.createOrReplaceTempView("ventas")

# LOAD
output_path = Path("salida_ventas_limpias")
shutil.rmtree(output_path, ignore_errors=True)
output_path.mkdir(parents=True, exist_ok=True)
parquet.write_table(clean.toArrow(), output_path / "part-00000.parquet")

print("\n=== Consulta 2: registros después de limpieza ===")
print("Registros después de limpieza:", clean.count())

queries = [
    (
        "1. Unidades vendidas por producto en pedidos Entregados",
        """
        SELECT product, SUM(quantity) AS unidades_vendidas
        FROM ventas
        WHERE status = 'Entregado'
        GROUP BY product
        ORDER BY unidades_vendidas DESC
        """
    ),
    (
        "2. Promedio de unidades devueltas en pedidos Devueltos",
        """
        SELECT AVG(returned_qty) AS promedio_unidades_devueltas
        FROM ventas
        WHERE status = 'Devuelto'
        """
    ),
    (
        "3. Pedidos Entregados por ciudad",
        """
        SELECT city, COUNT(order_id) AS pedidos_entregados
        FROM ventas
        WHERE status = 'Entregado'
        GROUP BY city
        ORDER BY pedidos_entregados DESC
        """
    ),
    (
        "4. Ingresos por categoría en pedidos Entregados",
        """
        SELECT category, SUM(sales_cop) AS ingresos_cop
        FROM ventas
        WHERE status = 'Entregado'
        GROUP BY category
        ORDER BY ingresos_cop DESC
        """
    ),
    (
        "5. Pedidos Entregados por canal de venta",
        """
        SELECT channel, COUNT(order_id) AS pedidos_entregados
        FROM ventas
        WHERE status = 'Entregado'
        GROUP BY channel
        ORDER BY pedidos_entregados DESC
        """
    ),
    (
        "6. Promedio del valor de venta por pedido Entregado",
        """
        SELECT AVG(sales_cop) AS promedio_venta_cop
        FROM ventas
        WHERE status = 'Entregado'
        """
    ),
    (
        "7. Unidades vendidas por departamento en pedidos Entregados",
        """
        SELECT department, SUM(quantity) AS unidades_vendidas
        FROM ventas
        WHERE status = 'Entregado'
        GROUP BY department
        ORDER BY unidades_vendidas DESC
        """
    ),
    (
        "8. Pedidos Entregados por método de pago",
        """
        SELECT payment_method, COUNT(order_id) AS pedidos_entregados
        FROM ventas
        WHERE status = 'Entregado'
        GROUP BY payment_method
        ORDER BY pedidos_entregados DESC
        """
    ),
    (
        "9. Promedio de días de envío en pedidos Entregados",
        """
        SELECT AVG(shipping_days) AS promedio_dias_envio
        FROM ventas
        WHERE status = 'Entregado'
        """
    ),
    (
        "10. Ingresos por tipo de cliente en pedidos Entregados",
        """
        SELECT customer_type, SUM(sales_cop) AS ingresos_cop
        FROM ventas
        WHERE status = 'Entregado'
        GROUP BY customer_type
        ORDER BY ingresos_cop DESC
        """
    ),
]

for title, query in queries:
    print(f"\n=== {title} ===")
    spark.sql(query).show(truncate=False)

spark.stop()
