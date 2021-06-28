// Databricks notebook source
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

val esquema = StructType(Array(
  StructField("COD_DISTRITO", IntegerType, true),
  StructField("DESC_DISTRITO", StringType, true),
  StructField("COD_DIST_BARRIO", IntegerType, true),
  StructField("DESC_BARRIO", StringType, true),
  StructField("COD_BARRIO", IntegerType, true),
  StructField("COD_DIST_SECCION", IntegerType, true),
  StructField("COD_SECCION", IntegerType, true),
  StructField("COD_EDAD_INT", IntegerType, true),
  StructField("EspanolesHombres", IntegerType, true),
  StructField("EspanolesMujeres", IntegerType, true),
  StructField("ExtranjerosHombres", IntegerType, true),
  StructField("ExtranjerosMujeres", IntegerType, true)
))

// COMMAND ----------

 /* 6.1 Comenzamos realizando la misma práctica que hicimos en Hive en Spark, importando el csv. Sería recomendable intentarlo con opciones que quiten las "" de los campos, que ignoren los espacios innecesarios en los campos, que sustituyan los valores vacíos por 0 y que infiera el esquema*/
val dfSinEspacios = spark.read.format("csv")
.option("header", "true")
.option("delimiter", ";")
.option("emptyValue", 0)
.option("inferschema", "false")
.option("quotes", "\"")
.option("encoding", "ISO-8859-1")
.schema(esquema)
.load("/FileStore/tables/padron.csv")

// COMMAND ----------

//Lo uso para mirar los tipos de datos inferidos
dfSinEspacios.describe()

// COMMAND ----------

//Eliminar los espacios en blanco
val padrontrim = dfSinEspacios.withColumn("DESC_DISTRITO", trim(col("DESC_DISTRITO")))
.withColumn("DESC_BARRIO", trim(col("DESC_BARRIO")))
padrontrim.show(false)

// COMMAND ----------

//Cuento los distritos y los barrios existentes
padrontrim.select("desc_distrito", "desc_barrio").agg(countDistinct("desc_distrito").alias("TotalDistritos"), countDistinct("desc_barrio").alias("TotalBarrios")).show()

// COMMAND ----------

//6.3) Enumera todos los barrios diferentes
padrontrim.select("desc_barrio").distinct.orderBy("desc_barrio").show()

// COMMAND ----------

/*6.4)
Crea una vista temporal de nombre "padron" y a través de ella cuenta el número de barrios diferentes que hay.*/
padrontrim.createOrReplaceTempView("padron")

// COMMAND ----------

/*6.5)
Crea una nueva columna que muestre la longitud de los campos de la columna 
DESC_DISTRITO y que se llame "longitud".*/
val padron_tam = padrontrim.withColumn("Longitud", length(col("desc_distrito"))).select("desc_distrito", "Longitud").distinct().show()

// COMMAND ----------

/*6.6)
Crea una nueva columna que muestre el valor 5 para cada uno de los registros de la tabla.*/
val padron_cincos = padrontrim.withColumn("cincos", lit(5))

// COMMAND ----------

//Muestro los distritos y la nueva columna
padron_cincos.select("desc_distrito", "cincos").distinct.show()

// COMMAND ----------

/*6.7)
Borra esta columna.*/
val padron = padron_cincos.drop("cincos")

// COMMAND ----------

//Compruebo que se ha borrado
padron.show(false)

// COMMAND ----------

/*6.8)
Particiona el DataFrame por las variables DESC_DISTRITO y DESC_BARRIO.
*/
val df_particion = padrontrim.repartition(col("DESC_DISTRITO"), col("DESC_BARRIO"))

// COMMAND ----------

/*6.9)
Almacénalo en caché. Consulta en el puerto 4040 (UI de Spark) de tu usuario local el estado de los rdds almacenados*/
df_particion.cache()

// COMMAND ----------

df_particion.show()

// COMMAND ----------

/*6.10)
Lanza una consulta contra el DF resultante en la que muestre el número total de "espanoleshombres", "espanolesmujeres", extranjeroshombres" y "extranjerosmujeres" para cada barrio de cada distrito. Las columnas distrito y barrio deben ser las primeras en aparecer en el show. Los resultados deben estar ordenados en orden de más a menos según la columna "extranjerosmujeres" y desempatarán por la columna 
"extranjeroshombres".*/
df_particion.groupBy("DESC_BARRIO").agg(sum("ESPANOLESHOMBRES").alias("TESPH"), sum("ESPANOLESMUJERES").alias("ESPTM"),sum("EXTRANJEROSHOMBRES").alias("EXTH"),sum("EXTRANJEROSMUJERES").alias("EXTM")).orderBy(col("EXTM").desc, col("EXTH").desc).show(false)

// COMMAND ----------

/* 6.11)
Elimina el registro en caché*/
df_particion.unpersist()

// COMMAND ----------

/*6.12)
Crea un nuevo DataFrame a partir del original que muestre únicamente una columna con DESC_BARRIO, otra con DESC_DISTRITO y otra con el número total de "espanoleshombres" residentes en cada distrito de cada barrio. Únelo (con un join) con el DataFrame original a través de las columnas en común.*/
val df_3cols = padron.select("desc_barrio", "desc_distrito", "espanoleshombres").groupBy("desc_distrito", "desc_barrio").agg(sum("espanoleshombres").alias("TotalHombresBarrios")).orderBy("desc_distrito")

// COMMAND ----------

//Muestro la tabla
df_3cols.show(false)

// COMMAND ----------

//Join del punto 6.12
val df_join = df_3cols.join(padrontrim, df_3cols("DESC_DISTRITO")===padrontrim("DESC_DISTRITO") && df_3cols("DESC_BARRIO")===padrontrim("DESC_BARRIO"))

// COMMAND ----------

//Muestro la tabla
df_join.show(false)

// COMMAND ----------

// MAGIC %sql
// MAGIC /*
// MAGIC 6.13)
// MAGIC Repite la función anterior utilizando funciones de ventana. (over(Window.partitionBy.....))
// MAGIC */
// MAGIC SELECT DISTINCT(DESC_DISTRITO), DESC_BARRIO, SUM(ESPANOLESHOMBRES) OVER(PARTITION BY (DESC_DISTRITO, DESC_BARRIO)) AS TOTALESPH FROM padron GROUP BY DESC_DISTRITO, DESC_BARRIO, ESPANOLESHOMBRES

// COMMAND ----------

/*6.14)
Mediante una función Pivot muestra una tabla (que va a ser una tabla de contingencia) que contenga los valores totales ()la suma de valores) de espanolesmujeres para cada distrito y en cada rango de edad (COD_EDAD_INT). Los distritos incluidos deben ser únicamente CENTRO, BARAJAS y RETIRO y deben figurar como columnas .*/
val df_pivot = padrontrim.groupBy("COD_EDAD_INT").pivot("DESC_DISTRITO", Seq("BARAJAS", "CENTRO", "RETIRO")).sum("EspanolesMujeres").orderBy("COD_EDAD_INT")
df_pivot.describe()
df_pivot.show()

// COMMAND ----------

/*6.15)
Utilizando este nuevo DF, crea 3 columnas nuevas que hagan referencia a qué porcentaje 
de la suma de "espanolesmujeres" en los tres distritos para cada rango de edad representa 
cada uno de los tres distritos. Debe estar redondeada a 2 decimales. Puedes imponerte la 
condición extra de no apoyarte en ninguna columna auxiliar creada para el caso.*/
val df_porcentaje = df_pivot.withColumn("BARAJAS", round(col("BARAJAS")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100, 2))
.withColumn("RETIRO", round(col("BARAJAS")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100, 2))
.withColumn("CENTRO", round(col("BARAJAS")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100, 2))
//Muestro la tabla
df_porcentaje.show()

// COMMAND ----------

/*6.16)
Guarda el archivo csv original particionado por distrito y por barrio (en ese orden) en un directorio local. Consulta el directorio para ver la estructura de los ficheros y comprueba que es la esperada.*/
df_particion.write.format("csv").mode("overwrite").save("/FileStore/tables/padronPrt")

// COMMAND ----------

/*6.17)
Haz el mismo guardado pero en formato parquet. Compara el peso del archivo con el resultado anterior.*/
df_particion.write.format("parquet").mode("overwrite").save("/FileStore/tables/padronParquet")
