package scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.io.File

/***
 * ESERCIZIO NUMERO 9 DELLA EMAIL
 */

object MainEs5 extends App {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("Es5Scala")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  /***
   * LEGGO NELLA CARTELLA TUTTI I FILE CSV
   */

  val dir = new File("src/main/resources/datiEs5")

  var num = 0

  /***
   * CREO I DATAFRAME LEGGENDO TUTTI I FILE NELLA CARTELLA  datiEs5
   * CREO LE TEMPVIEW DI OGNI DATAFRAME COSI' DA POTERLI UTILIZZARE
   */

  for(d <- dir.listFiles){
    num += 1
    val DF = spark.read
      .option("header", true)
      .option("inferSchema", true)
      .option("delimiter", ",")
      .csv(d.toString)

    DF.createOrReplaceTempView("tab"+num.toString)

  }



  /***
   * TASK 1
   * COMPLETO
   */

  val query1 = spark.sql(
    """
      |select t1.country
      |from tab1 t1
      |left join tab5 t5
      |on t1.country = t5.country
      |left join tab3 t3
      |on t5.player_id = t3.player_id
      |left join tab4 t4
      |on t3.match_id = t4.match_id
      |where t4.world_cup is null
      |""".stripMargin)
    .distinct()

    //query1.show()

  println("TASK1 DONE")

  //println(query1.count())

  /***
   * TEASK 2
   * COMPLETO
   */

  /***
   * CREO 2 QUERY PER OTTENERE IL MASSIMO E I MINIMO DELLA COLONNA YEAR
   * SALVO IL RISULTATO IN UNA LISTA UTILIZZANDO COLLECTASLIST()
   * CREO LE VAL MIN E MAX ACCEDENDO AL VALORE DELLA LISTA E CONVERTENDOLO IN TOINT
   */

  val query2_0 = spark.sql(
    """
      |select min(year) as min_y
      |from tab4
      |""".stripMargin).collectAsList()

  val min = query2_0.get(0)(0).toString.toInt

  val query2_0_1 = spark.sql(
    """
      |select max(year) as min_y
      |from tab4
      |""".stripMargin).collectAsList()

  val max = query2_0_1.get(0)(0).toString.toInt

  /***
   * QUI CREO UN RANGE CON PASSO 4 UTILIZZANDO I VALORI MIN E MAX
   * GRAZIE A IMPORT SPARK.IMPLICITS._ POSSO CREARE IL DATAFRAME DA
   * QUESTO RANGE, SUCCESSIVAMENTE CREO LA TEMPVIEW TAB_A
   */

  val anni = ( min to max by 4).toDF("Years")

  //anni.show()

  anni.createOrReplaceTempView("tab_a")

  val query2 = spark.sql(
    """
      |select distinct(year) as year, Years
      |from tab_a ta
      |left join tab4 t4
      |on ta.Years = t4.year
      |where t4.year is null
      |""".stripMargin)
    .orderBy( desc("year"), desc("Years") ) //E' SOLO UN ALTRO MODO PER UTILIZZARE ORDER BY

  //query2.show()

  println("TASK2 DONE")

  /***
   * TASK 3
   * COMPLETO
   */

  val query3 = spark.sql(
    """
      |select t4.year, t4.host, count(t2.event_type) as num_tot_autogoal
      |from tab4 t4
      |left join tab2 t2
      |on t4.match_id = t2.match_id
      |where t2.event_type = 'owngoal'
      |group by t4.year, t4.host
      |order by num_tot_autogoal desc
      |""".stripMargin)

  //query3.show()

  println("TASK3 DONE")

  /***
   * TASK 4
   * COMPLETO
   */

  val query4 = spark.sql(
    """
      |select t5.player_id, t5.player_name, t5.dob, t1.country, t1.continent, t4.home, t4.away, t4.h_score, t4.a_score
      |from tab1 t1
      |left join tab5 t5
      |on t1.country = t5.country
      |left join tab3 t3
      |on t5.player_id = t3.player_id
      |left join tab4 t4
      |on t3.match_id = t4.match_id
      |where t5.position = 'GK'
      |""".stripMargin)
      .distinct()
      .na.drop()
      .withColumn("tot_goal_subiti", when(col("country") === col("home"), col("a_score"))
      .otherwise(col("h_score")))

  //query4.show()

  query4.createOrReplaceTempView("tab_p")

  val query4_2 = spark.sql(
    """
      |select player_id, player_name, dob, country, continent, sum(tot_goal_subiti) as numero_gol_subiti
      |from tab_p
      |group by player_id, player_name, dob, country, continent
      |order by numero_gol_subiti desc
      |""".stripMargin)


  //query4_2.show()

  println("TASK4 DONE")

  /***
   * TASK 5
   * COMPLETO
   */

  val query5 = spark.sql(
    """
      |select t5.country, t5.player_id, t5.player_name, t5.dob, t2.match_id, t2.event_type, t3.team, t3.lineups_type, t4.phase, t4.home, t4.away, t4.year, t4.h_score, t4.a_score
      |from tab5 t5
      |left join tab2 t2
      |on t5.player_id = t2.player_id
      |left join tab3 t3
      |on t2.match_id = t3.match_id
      |left join tab4 t4
      |on t3.match_id = t4.match_id
      |where t3.team = 'home' and t3.lineups_type = 'sub' and t2.event_type = 'goal' and t4.phase = 'Final'
      |order by t4.year desc
      |""".stripMargin)
      .na.drop()
        .withColumn("esito",
           when(col("country") === col("home") and col("h_score") > col("a_score") , "Vittoria")
          .when(col("country") === col("away") and col("h_score") < col("a_score"), "Vittoria")
          .when(col("h_score") === col("a_score"), "Righori")
          .otherwise("Sconfitta"))
        .withColumn("dob", split(col("dob").cast(StringType), "/").getItem(2))
        .withColumn("eta_gol", col("year").cast(IntegerType) - col("dob").cast(IntegerType))
        .withColumn("eta_attuale", lit(2022) - col("dob").cast(IntegerType))
      .distinct()
      .drop("team")

  //query5.show()

  query5.createOrReplaceTempView("tab_f")

  val query5_2 = spark.sql(
    """
      |select country, player_id, player_name, lineups_type, match_id, phase, eta_gol, eta_attuale, esito
      |from tab_f
      |where esito = 'Vittoria' or esito = 'Righori'
      |""".stripMargin)

  //query5_2.show()

  println("TASK5 DONE")

  /***
   * TASK 6
   * COMPLETO
   */


  /***
   * SELEZIONA SOLO I GIOCATORI CHE HANNO FATTO GOAL E PENALITY => (GIOCATORI1)
   */

  val query6 = spark.sql(
    """
      |select t5.player_id, t5.player_name, t5.position, t3.lineups_type, t2.event_type, t3.team, t2.match_id
      |from tab5 t5
      |left join tab3 t3
      |on t5.player_id = t3.player_id
      |left join tab2 t2
      |on t3.player_id = t2.player_id and t3.match_id = t2.match_id
      |where t2.event_type in ('goal', 'penalty')
      |""".stripMargin)
    .distinct()
    .na.drop()
    .withColumnRenamed("event_type", "goal_type")

  //query6.show()

  query6.createOrReplaceTempView("tab_6")

  /***
   * SELEZIONA SOLO I GIOCATORI CHE HANNO FATTO OWNGOAL => (GIOCATORI2)
   */

  val query6_1 = spark.sql(
    """
      |select t5.player_id, t5.player_name, t5.position, t3.lineups_type, t2.event_type, t3.team, t2.match_id
      |from tab5 t5
      |left join tab3 t3
      |on t5.player_id = t3.player_id
      |left join tab2 t2
      |on t3.player_id = t2.player_id and t3.match_id = t2.match_id
      |where t2.event_type = "owngoal"
      |""".stripMargin)
    .distinct()
    .na.drop()
    .withColumnRenamed("event_type", "goal_type")

  //query6_1.show()

  query6_1.createOrReplaceTempView("tab_6_1")

  /***
   * SELEZIONA TUTTI I PORTIERI E RINOMINA COLONNE
   */

  val query6_2 = spark.sql(
    """
      |select t5.player_id, t5.player_name, t5.position, t3.team, t3.match_id
      |from tab5 t5
      |left join tab3 t3
      |on t5.player_id = t3.player_id
      |where t5.position = "GK" and t3.lineups_type = "starting"
      |order by t3.match_id desc
      |""".stripMargin)
    .distinct()
    .na.drop()
    .withColumnRenamed("player_id", "GK_id")
    .withColumnRenamed("player_name", "GK_name")
    .withColumnRenamed("position", "GK")
    .withColumnRenamed("match_id", "id_match")
    .withColumnRenamed("team", "teams")

  //query6_2.show()

  query6_2.createOrReplaceTempView("tab_6_2")

  /***
   * CREA UN NUOVO DF JOINANDO GIOCATORI1 E PORTIERI
   */

  val query6_3 = spark.sql(
    """
      |select *
      |from tab_6 t6
      |left join tab_6_2 t6_2
      |on t6.match_id = t6_2.id_match and t6.team != t6_2.teams
      |order by match_id desc
      |""".stripMargin)
    .distinct()
    .na.drop()

  //query6_3.show()

  /***
   * CREA UN NUOVO DF JOINANDO GIOCATORI2 E PORTIERI
   */

  val query6_4 = spark.sql(
    """
      |select *
      |from tab_6_1 t6_1
      |left join tab_6_2 t6_2
      |on t6_1.match_id = t6_2.id_match and t6_1.team = t6_2.teams
      |order by match_id desc
      |""".stripMargin)
    .distinct()
    .na.drop()

  //query6_4.show()

  val DF_UNION = query6_3.union(query6_4)

  println("TASK6 DONE")


  val listDF = List(query1,query2,query3,query4_2,query5_2,DF_UNION)
  var str = 0

  try {

    for(d <- listDF) {
      str += 1
      d.repartition(1).write.option("header", true).format("csv").save("src/main/resources/ES5_OUTPUT/Es5_OUTPUT_Q"+str.toString)
    }

    println("WRITE FILE DONE")

  } catch {
    case e: Exception => println("FILE EXISTS")
  }


}