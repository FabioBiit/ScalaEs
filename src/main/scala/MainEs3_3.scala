package scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, lit, round}

/***
 * SOLUZIONE "COMPLETA"
 */

object MainEs3_3 extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Es3Scala")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /***
   *
   * LETTURA E CREAZIONE DATAFRAME DA FILE EXCEL
   * SE I FILE NON FOSSERO EXCEL NON CREDO CI SAREBBE STATA LA NECESSITA'
   * DI CREARE UN HEADER PERSONALIZZATO, MA SAREBBE BASTATO L'OPTION HEADER TRUE
   *
   */

  /**
   * LEGGE FILE EXCEL CON HEADER TRUE
   */

  val DF_EX3 = spark.read
    .format("com.crealytics.spark.excel")
    .option("useHeader", "true")
    .option("startColumn", 0)
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/datiEs3/United_Airlines.xls")

  DF_EX3.printSchema()

  val DF3 = DF_EX3.na.drop().toDF().show()

 val header = Seq( "Anni", "1995", "1996", "1997", "1998", "1999", "2000",
    "2001", "2002", "2003", "2004", "2005", "2006", "2007",
    "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015")

  val DF_EX1 = spark.read
    .format("com.crealytics.spark.excel")
    .option("useHeader", "false")
    .option("startColumn", 1)
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/datiEs3_1/United_Airlines.xls")

  val DF_EX2 = spark.read
    .format("com.crealytics.spark.excel")
    .option("useHeader", "false")
    .option("startColumn", 1)
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/datiEs3/United_Airlines_Actuals.xls")

  val DF1 = DF_EX1.na.drop().toDF(header: _*)//.show()
  val DF2 = DF_EX2.na.drop().toDF(header: _*)//.show()

  val DF_UNION = DF1.union(DF2).toDF(header: _*)


  /***
   *
   * LOOP PER SCRITTURA SU FILE PER ANNO
   *
   */

  val size = DF_UNION.schema

  var i = 0
  var anno = 1994
  val anno_max = DF_UNION.schema.last.name.toInt

  for(i <- size){
    anno += 1
    if(anno <= anno_max) {


      /***
       *
       * SOLUZIONE ABBOZZATA PER EFFETTUARE IL PIVOT
       * SELEZIONANDO LE COLONNE CON I DATI CHE
       * MI INTERESSANO (IL COLLECT PERMETTE
       * DI OTTENERE I DATI DELLA COLONNA)
       * LE CONVERTO IN STRINGA SEPARATE DA ","
       * SOSTITUISCO I CARATTERI NON NECESSARI
       *
       */

      val head = Seq(DF_UNION.select("Anni").collect.mkString(";"))
        .map(_.replace("[", ""))
        .map(_.replace("]", ""))


      val row = Seq(DF_UNION.select(anno.toString)
        .withColumn(anno.toString, round(col(anno.toString), 2 ))
        .collect.mkString(";"))
        .map(_.replace("[", ""))
        .map(_.replace("]", ""))
        .map(_.replace(".", ","))


      /***
       *
       * CREO IL DATAFRAME USANDO SEQ[STRING]
       *
       */

      import spark.implicits._

      val DF = row.toDF(head: _*)//.show()

      DF.withColumn("Anno", lit(anno)).repartition(1).write
        .option("header", true)
        .format("csv")
        .save("src/main/resources/Es3_3_OUTPUT_GOOD/ANNO"+anno)

    } else {
      println("Fine Anni")
    }
  }

}
