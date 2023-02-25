package scala.MainEs3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, round}

object MainEs3_2 extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Es3Scala")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /** *
   *
   * LETTURA E CREAZIONE DATAFRAME DA FILE EXCEL
   * SE I FILE NON FOSSERO EXCEL NON CREDO CI SAREBBE STATA LA NECESSITA'
   * DI CREARE UN HEADER PERSONALIZZATO MA SAREBBE BASTATO L'OPTION HEADER TRUE
   */

  val header = Seq("Anno", "1995", "1996", "1997", "1998", "1999", "2000",
    "2001", "2002", "2003", "2004", "2005", "2006", "2007",
    "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015")

  val DF_EX1 = spark.read
    .format("com.crealytics.spark.excel")
    .option("useHeader", "false")
    .option("startColumn", 1)
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/datiEs3/United_Airlines.xls")

  val DF_EX2 = spark.read
    .format("com.crealytics.spark.excel")
    .option("useHeader", "false")
    .option("startColumn", 1)
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/datiEs3/United_Airlines_Actuals.xls")

  val DF1 = DF_EX1.na.drop().toDF(header: _*) //.show()
  val DF2 = DF_EX2.na.drop().toDF(header: _*) //.show()

  val DF_UNION = DF1.union(DF2).toDF(header: _*)

  /** *
   *
   * LOOP PER SCRITTURA SU FILE PER ANNO
   *
   */

  val size = DF_UNION.schema

  var i = 0
  var anno = 1994
  val anno_max = DF_UNION.schema.last.name.toInt

  for (i <- size) {
    anno += 1
    if (anno <= anno_max) {
      val DF = DF_UNION.select("Anno", anno.toString).withColumn(anno.toString, round(col(anno.toString))) //.show()
      DF.repartition(1).write.option("header", true).format("csv").save("src/main/resources/Es3_2_OUTPUT_GOOD/OUTPUT" + anno)
    } else {
      println("Fine Anni")
    }
  }

}
