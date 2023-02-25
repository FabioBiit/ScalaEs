package scala.MainEs3

import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}

object MainEs3_1 extends App {

  val spark: SparkSession = SparkSession.builder()
    .appName("Spark DataFrame Transpose")
    .master("local[*]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val DataSeq = spark.read
    .format("com.crealytics.spark.excel")
    .option("useHeader", "false")
    .option("startColumn", 1)
    .option("treatEmptyValuesAsNulls", "true")
    .option("inferSchema", "true")
    .load("src/main/resources/datiEs3/United_Airlines_Actuals.xls")

  val productQtyDF = DataSeq.na.drop().toDF("Anni", "1995", "1996", "1997", "1998", "1999", "2000",
    "2001", "2002", "2003", "2004", "2005", "2006", "2007",
    "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015")

  productQtyDF.show(false)

  val productTypeDF = TransposeDF(productQtyDF, Seq("1995", "1996", "1997", "1998", "1999", "2000",
    "2001", "2002", "2003", "2004", "2005", "2006", "2007",
    "2008", "2009", "2010", "2011", "2012", "2013", "2014", "2015"), "Anni")

  productTypeDF.show(false)


  def TransposeDF(df: DataFrame, columns: Seq[String], pivotCol: String): DataFrame = {

    val columnsValue = columns.map(x => "'" + x + "', " + x)
    val stackCols = columnsValue.mkString(",")
    val df_1 = df.selectExpr(pivotCol, "stack(" + columns.size + "," + stackCols + ")")
      .select(pivotCol, "col0", "col1")

    val final_df = df_1.groupBy(col("col0")).pivot(pivotCol).agg(concat_ws("", collect_list(col("col1"))))
      .withColumnRenamed("col0", pivotCol)
    final_df
  }


}
