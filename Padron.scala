//Practica padron en scala
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.window

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

val dfSinEspacios = spark.read.format("csv")
.option("header", "true")
.option("delimiter", ";")
.option("emptyValue", 0)
.option("inferschema", "false")
.option("quotes", "\"")
.option("encoding", "ISO-8859-1")
.option("ignoreLeadingWhiteSpace", true)
.option("ignoreTrailingWhiteSpace", true)
.schema(esquema)
.load("/FileStore/tables/padron.csv")

dfSinEspacios.describe()

dfSinEspacios.select("desc_distrito", "desc_barrio").agg(countDistinct("desc_distrito").alias("TotalDistritos"), countDistinct("desc_barrio").alias("TotalBarrios")).show()

dfSinEspacios.select("desc_barrio").distinct.orderBy("desc_barrio").show()

dfSinEspacios.createOrReplaceTempView("padron")

val padron_tam = dfSinEspacios.withColumn("Longitud", length(col("desc_distrito"))).select("desc_distrito", "Longitud").distinct().show()

val padron_cincos = dfSinEspacios.withColumn("cincos", lit(5))

padron_cincos.select("desc_distrito", "cincos").distinct.show()

val padron = padron_cincos.drop("cincos")

padron.show(false)

val df_particion = dfSinEspacios.repartition(col("DESC_DISTRITO"), col("DESC_BARRIO"))

df_particion.cache()

df_particion.count()

df_particion.groupBy("DESC_BARRIO").agg(sum("ESPANOLESHOMBRES").alias("TESPH"),
                                        sum("ESPANOLESMUJERES").alias("ESPTM"),
                                        sum("EXTRANJEROSHOMBRES").alias("EXTH"),
                                        sum("EXTRANJEROSMUJERES").alias("EXTM")).orderBy(col("EXTM").desc, col("EXTH").desc).show(false)

df_particion.unpersist()

val df_3cols = padron.select("desc_barrio", "desc_distrito", "espanoleshombres").groupBy("desc_distrito", "desc_barrio").agg(sum("espanoleshombres").alias("TotalHombresBarrios")).orderBy("desc_distrito")

df_3cols.show(false)
padron.describe()
df_3cols.describe()
val df_join = df_3cols.join(dfSinEspacios, df_3cols("DESC_DISTRITO")===dfSinEspacios("DESC_DISTRITO") && df_3cols("DESC_BARRIO")===dfSinEspacios("DESC_BARRIO"))
df_join.show(false)

%sql
SELECT DISTINCT(DESC_DISTRITO), DESC_BARRIO, SUM(ESPANOLESHOMBRES) OVER(PARTITION BY (DESC_DISTRITO, DESC_BARRIO)) AS TOTALESPH
FROM padron GROUP BY DESC_DISTRITO, DESC_BARRIO, ESPANOLESHOMBRES;

val padron_df = dfSinEspacios.select(esquema.fields.map(field =>{if(field.dataType == StringType){trim(col(field.name)).as(field.name)} else {col(field.name)}}):_*)

padron_df.show()

val df_pivot = padron_df.groupBy("COD_EDAD_INT").pivot("DESC_DISTRITO", Seq("BARAJAS", "CENTRO", "RETIRO")).sum("EspanolesMujeres").orderBy("COD_EDAD_INT")
df_pivot.describe()
df_pivot.show()

val df_porcentaje = df_pivot.withColumn("BARAJAS", round(col("BARAJAS")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100, 2))
.withColumn("RETIRO", round(col("BARAJAS")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100, 2))
.withColumn("CENTRO", round(col("BARAJAS")/(col("BARAJAS")+col("CENTRO")+col("RETIRO"))*100, 2))

df_porcentaje.show()

df_particion.write.format("csv").mode("overwrite").save("/FileStore/tables/padronPrt")

df_particion.write.format("parquet").mode("overwrite").save("/FileStore/tables/padronParquet")
