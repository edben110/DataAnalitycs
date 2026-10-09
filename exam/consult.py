import shutil
#import pyarrow.parquet as parquet
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType, DoubleType

spark = SparkSession.builder \
    .appName("ETL_flights") \
    .getOrCreate()

liga = spark.read.option("header", True).option("inferSchema", True).csv(
    "liga_colombiana_sucia (1).csv"
)

liga.dropDuplicates()
#liga = liga.withColumn(,trim(col()))
liga.select("fecha").distinct().show()
liga = liga.withColumn("fecha",when(col("fecha")=="fecha desconocida","2024-01-04 6:15:00").otherwise(col("fecha")))
liga = liga.withColumn("fecha",try_to_timestamp(col("fecha"),lit("yyyy-MM-dd HH:mm:ss")))

liga= liga.withColumn("equipo", trim(lower(col("equipo"))))
liga= liga.withColumn(
    "equipo",
    when(col("equipo") == "alianza petrolera", "Alianza Petrolera")
    .when(col("equipo") == "américa de cali", "América de Cali")
    .when(col("equipo") == "atlético bucaramanga", "Atlético Bucaramanga")
    .when(col("equipo") == "atlético nacional", "Atlético Nacional")
    .when(col("equipo") == "boyacá chicó", "Boyacá Chicó")
    .when(col("equipo") == "cúcuta deportivo", "Cúcuta Deportivo")
    .when(col("equipo") == "deportes tolima", "Deportes Tolima")
    .when(col("equipo") == "deportivo cali", "Deportivo Cali")
    .when(col("equipo") == "envigado", "Envigado")
    .when(col("equipo") == "fortaleza", "Fortaleza")
    .when(col("equipo") == "independiente medellín", "Independiente Medellín")
    .when(col("equipo") == "jaguares", "Jaguares")
    .when(col("equipo") == "junior", "Junior")
    .when(col("equipo") == "la equidad", "La Equidad")
    .when(col("equipo") == "millonarios", "Millonarios")
    .when(col("equipo") == "once caldas", "Once Caldas")
    .when(col("equipo") == "pasto", "Pasto")
    .when(col("equipo") == "patriotas", "Patriotas")
    .when(col("equipo") == "santa fe", "Santa Fe")
    .when(col("equipo") == "unión magdalena", "Unión Magdalena")
    .otherwise(col("equipo"))
    )
liga = liga.withColumn("equipo", upper(trim(col("equipo"))))

liga.createOrReplaceTempView("liga")































spark.sql(""" select equipo,count(*) as cantidadPartidos from liga group by equipo""").show()

liga.select(mode(col("goles_favor")),mode(col("goles_contra"))).show()

spark.sql(""" select avg(goles_contra) from liga group by equipo""")

liga.where(col("equipo")=="SANTA FE").select("equipo","fecha","goles_favor").orderBy(col("goles_favor").desc()).show()
	

liga.select("fecha").distinct().show()
















# LOAD
#output_path = Path("salida_ventas_limpias")
#shutil.rmtree(output_path, ignore_errors=True)
#output_path.mkdir(parents=True, exist_ok=True)
#parquet.write_table(clean.toArrow(), output_path / "part-00000.parquet")