import org.apache.spark.sql.SparkSession
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object prova extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("prova")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /**
   * CREAZIONE DEI 3 DATAFRAME
   */

  //DATAFRAME 1

  val dati1 = Seq(
    ( 1, "M1", 66.4, 31.6, 82.4, 82.4),
    ( 1, "M1", 15.1, 56.1, 73.2, 72.4),
    ( 2, "M1", 6.0, 60.5, 62.9, 32.4),
    ( 2, "M1", 75.2, 96.1, 17.2, 22.4),
    ( 2, "M2", 66.4, 31.6, 80.4, 92.4),
    ( 2, "M2", 66.4, 31.6, 42.4, 82.4),
    ( 3, "M1", 66.4, 31.6, 80.4, 92.4),
    ( 3, "M1", 66.4, 31.6, 42.4, 82.4),
    ( 3, "M2", 66.4, 31.6, 80.4, 92.4),
    ( 3, "M2", 66.4, 31.6, 42.4, 82.4)
  )

  val head1 = Seq("giorno","categoria", "colB", "colC", "colD", "colE")

  import spark.implicits._

  var DF1 = dati1.toDF(head1: _*)

  val df_sum = DF1.groupBy("giorno","categoria").agg(sum("colB").as("km_a1"),
                                                  sum("colC").as("km_a2"),
                                                  sum("colD").as("km_u1"),
                                                  sum("colE").as("km_u2"))

  val df_km_tot = df_sum.withColumn("km_tot", $"km_a1" + $"km_a2"+ $"km_u1")
                        .withColumn("km_tot_a", $"km_a1" + $"km_a2")
                        .withColumn("km_tot_u", $"km_u1" + $"km_u2")

  var prova1 = df_km_tot.orderBy(asc("giorno"),asc("categoria"))//.show()

  //val prova2 = prova1.agg(sum("km_tot")).show()

  val km_tot = prova1.select("km_tot").map(x => x.getDouble(0)).collect().toList

  var prova = 0.0

  //prova1.createOrReplaceTempView("tab")

  for(km_t<-km_tot){
      prova = prova + km_t
      //println(prova)
      prova1=prova1.withColumn("km_cumulati",  lit(prova) )
      //prova1.show()
    }

  prova1.show()

  //DF1.show()

}