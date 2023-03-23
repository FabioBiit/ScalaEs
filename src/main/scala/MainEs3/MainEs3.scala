package MainEs3

import org.apache.spark.sql.SparkSession

/** *
 * VERSIONI ES3_2 E ES3_3 IMPLEMENTANO LA SOLUZIONE
 */


object MainEs3 extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Es3Scala")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val DF_EX1 = spark.read
    .format("com.crealytics.spark.excel")
    .option("useHeader", "false")
    .option("startColumn", 1)
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/datiEs3/United_Airlines.xls")

  val DF1 = DF_EX1.na.drop().toDF("Anni", "1995", "1996", "1997", "1998", "1999", "2000",
    "2001", "2002", "2003", "2004", "2005", "2006", "2007",
    "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015")
  //.show(false)

  val DF_EX2 = spark.read
    .format("com.crealytics.spark.excel")
    .option("useHeader", "false")
    .option("startColumn", 1)
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/datiEs3/United_Airlines_Actuals.xls")

  val DF2 = DF_EX2.na.drop().toDF("Anni", "1995", "1996", "1997", "1998", "1999", "2000",
    "2001", "2002", "2003", "2004", "2005", "2006", "2007",
    "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015") //.withColumn("1995", round(col("1995")))
    .show(false)

  /*val DF_EX1_1 = spark.read
    .format("com.crealytics.spark.excel")
    //.option("sheetName", "Daily")
    .option("useHeader", "false")
    .option("startColumn", 0)
    //.option("addColorColumns", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/datiEs3/United_Airlines.xlsx")//.show()

  val DF_EX2_1 = spark.read
    .format("com.crealytics.spark.excel")
    //.option("sheetName", "Daily")
    .option("useHeader", "false")
    .option("startColumn", 0)
    //.option("addColorColumns", "true")
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/datiEs3/United_Airlines_Actuals.xlsx").show()*/

  /*val prova = DF2.groupBy("1995", "1996" ).pivot("Anni").agg(concat_ws("", collect_list(col("1995"))),
    concat_ws("", collect_list(col("1996"))))*/

  //prova.show()

  //val DF_UNION = DF1.union(DF2)//.show()

  /*DF_EX1_1.createOrReplaceTempView("tab1")
  DF_EX2_1.createOrReplaceTempView("tab2")

  val join_df = spark.sql(
    """
      |select *
      |from tab1 t1
      |join tab2 t2
      |on t1.C0 = t2.C0
      |""".stripMargin)//.show()*/


  try {

    //prova.repartition(1).write.option("header", true).format("csv").save("src/main/resources/datiEs3/OUTPUT3")

    //join_df.repartition(1).write.option("header", true ).format("csv").save("src/main/resources/datiEs3/OUTPUT_UNION")

    //DF_UNION.repartition(1).write.option("header", true ).format("csv").save("src/main/resources/datiEs3/OUTPUT_UNION")

    //DF1.repartition(1).write.option("header", true).format("csv").save("src/main/resources/datiEs3/OUTPUT1")

    //DF2.repartition(1).write.option("header", true).format("csv").save("src/main/resources/datiEs3/OUTPUT2")


  }

  catch {

    case e: Exception => println("File gia esistenti")

  }

}
