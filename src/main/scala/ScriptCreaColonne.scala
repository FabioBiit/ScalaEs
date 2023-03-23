import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, desc, lit, regexp_replace}

object ScriptCreaColonne extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Es4Scala")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val dati2 = Seq(
    (32, 67, 77, "prova1")
  )

  val head2 = Seq("colA", "colB", "colD", "colE")

  var DF2 = dati2.toDF(head2: _*)

  val colNames= DF2.columns

  val tipo = DF2.schema.map(x => x.name+" : "+x.dataType)

  var valore = -1

  for (colName<-colNames) {

    valore += 1

    DF2 = DF2.withColumn(colName+"1", lit(s"$tipo".split(",")).getItem(valore))
             .withColumn(colName+"1", regexp_replace(col(colName+"1"), "List[()]", ""))
             .withColumn(colName+"1", regexp_replace(col(colName+"1"), "[)]", ""))

  }

  DF2.printSchema()

  DF2.show()

}
