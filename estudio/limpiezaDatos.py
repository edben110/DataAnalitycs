from pyspark.sql import *
from pyspark.sql.functions import * 
from pyspark.sql.types import * 

spark = SparkSession.builder\
	.appName("tiendaBuilder")\
	.master("local[*]") \
    .getOrCreate()

td = spark.read.csv("datasets/tienda_sucia.csv",header=True,inferSchema=True)
td.createOrReplaceTempView("td")

#td.select("id_venta","fecha","nombre_cliente","id_cliente","ciudad","categoria","producto","cantidad","precio_unitario","metodo_pago","estado","email").distinct().show(40)
#td.printSchema()#es necesario hacerlo para verificar que tipos de datos hay, si los datos numericos dice string es claro ejemplo de contaminacion 
td.describe()#.show()# nos da la cantidad, media, desviacion estandar, minimos y maximos

#td.select("categoria").distinct().show()
#verificar nulos
td.select([#se pone llaves para representar codigo python puro
	count(when(col(c).isNull(),c)).alias(c)#cuenta cuantos datos hay nulos, 
	#y when usa 2 parametros, el primero es si se cumple la condicion de nulo este seguira asi, de no serlo dejara la variable c 
	#tal cual como esta
	# para evitar los nombres tipicos de spark, se le coloca alias y c (nombre de todas las variables tal cual esta)
	# para evitar que se renombre las variables de las columnas
	for c in td.columns # aplicara el cambio a cada columna del dataset
	])#.show()

#datos con espacios vacios ej: "  pasto  "
td.select([
	count(when(trim(col(c))=="",c)#trim se encargara de eliminar los valores vacios elresto sigue la misma logica anterior
	).alias(c)
	for c in td.columns 
	])#.show()

#verificacion de valores de variables
#td.groupBy("ciudad") \.count() \.orderBy(col("count").desc()) \.show(50, truncate=False)

#td.groupBy("categoria").count().show()

#td.groupBy("metodo_pago").count().show()

#td.groupBy("producto").count().show(50, truncate=False)

#valores duplicados
#hay que tener en cuenta que llama todas las columnas y que id_venta significa una sola venta y puede duplicarse
#dado q no todas las ventas pueden ser la misma a pesar de tener valores similares
#td.groupBy(td.columns)\.count()\.filter(col("count") > 1)\.show(truncate=False)#esto es para evitar que ponga limite de caracteres
#mas sencillo es utilizar la siguiente funcion para eliminar nulos
#df = df.dropDuplicates(["order_id"])

#correccion teniendo en cuenta que id_venta tiene diferencias
#td.groupBy("fecha","id_cliente","nombre_cliente","ciudad","categoria","producto","cantidad","precio_unitario","metodo_pago","estado","email").count()\.filter(col("count") > 1) \.show(50, truncate=False)


#solucionar errores
#casos de string en variables que deben ser tipo int
#td.withColumn("cantidad_num",expr("try_cast(cantidad as int)")).filter(col("cantidad_num").isNull() | (col("cantidad_num") <= 0)).select("id_venta","cantidad","cantidad_num")#.show(50)#con esto todos los valores string,NA y demas se convertiran en nulos

#aqui vamos a reescribir la columna de cantidad con los valores limpios en entero incluyendo negativos
td = td.withColumn("cantidad",expr("try_cast(cantidad as int)"))#try_cast cambia el tipado de string a int

#td.select("id_venta", "cantidad").show(20)#al imprimir todo saldra como enteros pero tambien con negativos

td = td.filter(col("cantidad").isNotNull() & (col("cantidad") > 0))#aqui sacamos los valores nulos y menores a 0 ccreando la nueva linea limpia 

# ahora todos los datos son positivos validos
#td.select("cantidad").distinct().show()

#filtrado de precio unitario

#verificacion
#td.select("precio_unitario").distinct().show()
td.withColumn(
    "precio_num",
    expr("try_cast(precio_unitario as double)")
).filter(
    col("precio_num").isNull() |
    (col("precio_num") <= 0)
).select(
    "id_venta",
    "precio_unitario",
    "precio_num"
)#.show(50, truncate=False)

#reescritura de la columna
td = td.withColumn(
    "precio_unitario",
    expr("try_cast(precio_unitario as double)")
)
#eliminacion de valores invalidos
td = td.filter(
    col("precio_unitario").isNotNull() &
    (col("precio_unitario") > 0)
)
#verificacion de las variables verdaderas
#td.select("precio_unitario").distinct().show(truncate=False)


#verificacion de datos en string
#verificacion de datos en ciudad
#cuando hay mas de 20 datos distintos es mejor usar este antes que distinct dado que retorna dato y cantidad repetida
#td.groupBy("ciudad").count().orderBy(col("ciudad")).show(50, truncate=False)

td = td.withColumn(
    "ciudad",
    trim(col("ciudad"))
)

# eliminar espacios innecesarios "" / " " / " a "
#como esto es una validacion universal se puede intentar con todas las que sean realmente string de una vez
td = td.withColumn("nombre_cliente", trim(col("nombre_cliente")))
td = td.withColumn("ciudad", trim(col("ciudad")))
td = td.withColumn("categoria", trim(col("categoria")))
td = td.withColumn("producto", trim(col("producto")))
td = td.withColumn("metodo_pago", trim(col("metodo_pago")))
td = td.withColumn("estado", trim(col("estado")))
td = td.withColumn("email", trim(col("email")))

#normalizacion de datos con mayuscula
td = td.withColumn(
    "ciudad",
    lower(col("ciudad"))
)
#tambien se puede con mayusculas usando upper, pero no siempre es necesario

# validacion de nombres propios con mayuscula la primer letra y acentuaciones
td = td.withColumn(
    "ciudad",
    when(lower(col("ciudad")).isin("bogota", "bogotá"), "Bogotá")
    .when(lower(col("ciudad")).isin("pasto"), "Pasto")
    .when(lower(col("ciudad")).isin("cali"), "Cali")
    .when(lower(col("ciudad")).isin("medellin", "medellín"), "Medellín")
    .when(lower(col("ciudad")).isin("armenia"), "Armenia")
    .when(lower(col("ciudad")).isin("MANIZALES", "manizales"), "Manizales")
    .otherwise(col("ciudad"))#es parte de la funcion when, se usa para evitar el retorno de nulos
)
#eliminacion de nulos
td = td.filter(col("ciudad").isNotNull())

#td.groupBy("ciudad").count().orderBy(col("ciudad")).show(50, truncate=False)

#normalizar categoria

td = td.withColumn(
    "categoria",
    when(lower(col("categoria")).isin("tecnologia", "tecnlogia"), "Tecnologia")
    .when(lower(col("categoria")).isin("hogar", "hogarrr"), "Hogar")
    .when(lower(col("categoria")).isin("oficina", "oficna"), "Oficina")
    .otherwise(col("categoria"))
)

#td.groupBy("categoria").count().show()

#normalizar metodos de pago
td = td.withColumn(
    "metodo_pago",
    when(lower(col("metodo_pago")).isin("tarjeta", "tarjta"), "Tarjeta")
    .when(lower(col("metodo_pago")) == "efectivo", "Efectivo")
    .when(lower(col("metodo_pago")).isin("nequi", "neq"), "Nequi")
    .when(lower(col("metodo_pago")) == "transferencia", "Transferencia")
    .otherwise(col("metodo_pago"))
)

td = td.filter(col("metodo_pago").isNotNull())

#td.groupBy("metodo_pago").count().show()

#normalizar estados

td = td.withColumn(
    "estado",
    when(lower(col("estado")).isin("completada", "completad"), "Completada")
    .when(lower(col("estado")) == "cancelada", "Cancelada")
    .when(lower(col("estado")).isin("pendiente", "pendient"), "Pendiente")
    .when(col("estado")=="Desconocido",lit(None))#retorna ese dato como nulo
    .otherwise(col("estado"))
)

td = td.filter(col("estado").isNotNull())

#td.groupBy("estado").count().show()


#normalizar fechas a formato a-m-d
#td.groupBy("fecha").count().show()
#td.select("fecha").distinct().show(150)

#formatos existentes de fechas con una columna temporal
td = td.withColumn(
    "fecha_limpia",
    coalesce(
        try_to_date(col("fecha"), "yyyy-MM-dd"),
        try_to_date(col("fecha"), "yyyy/MM/dd"),
        try_to_date(col("fecha"), "MM-dd-yyyy"),
        try_to_date(col("fecha"), "dd-MM-yyyy"),
        try_to_date(col("fecha"), "dd/MM/yyyy")
    )
)

# Convertimos al formato d/m/yyyy
td = td.withColumn(
    "fecha",
    date_format(
        col("fecha_limpia"),
        "d/M/yyyy"
    )
)

# Eliminamos la columna temporal
td = td.drop("fecha_limpia")

# Comprobamos
td.select("fecha") \
    .distinct() \
    .show(200, truncate=False)

#hasta el momento coalesce sirve para "unificar" variables, es recomendado no experimentar tanto, su funcion es mas compleja

#pasamos todos los formatos a uno universal
td.filter(col("fecha"))

td.select("fecha").distinct().show(200, truncate=False)
print("Registros después de limpiar: ", td.count())