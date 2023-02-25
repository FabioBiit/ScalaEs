package scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql

/**
 * ESERCIZIO NUMERO 6 DELLA EMAIL
 */

object MainEs4 extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Es4Scala")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /**
   * CREAZIONE DEI 3 DATAFRAME
   */

  //DATAFRAME 1

  val dati1 = Seq(
      (34, 66, 31, 82),
      (55, 15, 56, 73),
      (61, 6, 60, 62),
      (8, 75, 96, 17)
    )

    val head1 = Seq("colA", "colB", "colC", "colD")

    import spark.implicits._
    val DF1 = dati1.toDF(head1: _*)

  //DATAFRAME 2

  val dati2 = Seq(
    (34, 66, 31, 82),
    (55, 15, 56, 73),
    (61, 6, 60, 62),
    (8, 75, 96, 17)
  )

  val head2 = Seq("colA", "colB", "colD", "colE")

  var DF2 = dati2.toDF(head2: _*)

  //DATAFRAME 3

  val dati3 = Seq(
    (34, "prova2.1", 31, "prova4.1"),
    (55, "prova2.2", 56, "prova4.2"),
    (61, "prova2.3", 60, "prova4.3"),
    (8, "prova2.4", 96, "prova4.4")
  )

  val head3 = Seq("colA", "colC", "colD", "colE")

  val DF3 = dati3.toDF(head3: _*)

  val df_list = List(DF1, DF2, DF3)

  /**
   *LOOP DROPPA COLONNE PER I TRE DF
   */

  for(df <- df_list){

    funzione_drop(df)

  }

  /**
   * FUNZIONE DROP COLONNE
   */

  def funzione_drop(DF: sql.DataFrame): Unit ={
    val df_drop = DF.drop("colA", "colB", "colC")
    df_drop.show()
  }

}
