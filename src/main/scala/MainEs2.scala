package scala

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, when}

object MainEs2 extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Es2Scala")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /***
   * CREAZIONE DATAFRAME DA LETTURA FILE
   */

  val DF_w_city1 = spark.read.option("header", true)
    .option("inferSchema", true)
    .option("delimiter", ",")
    .csv("src/main/resources/datiEs2/world-cities_csv.csv")
    .withColumnRenamed("name", "city")
    .select("city" ,"country")
    .distinct()
    //.show(50)

  //val size3 = DF_w_city1.count()

  //println(size3)

  val DF_w_city2 = spark.read.option("header", true)
    .option("inferSchema", true)
    .option("delimiter", ",")
    .csv("src/main/resources/datiEs2/worldcities.csv")
    .filter("capital  = 'primary'")
    .distinct()
    //.show(50)

  //val size4 = DF_w_city2.count()

 //println(size4)

  /***
   * CREAZIONE TEMPVIEW PER LE QUERY SPARKSQL
   */

  DF_w_city1.createOrReplaceTempView("tab_c1")
  DF_w_city2.createOrReplaceTempView("tab_c2")


  /***
   * JOINO I DUE DF COSI' DA OTTENERE UNA TABELLA AVENTE LE INFORMAZIONI RICHIESTE DALLE DUE TABELLE
   * EFFETTUO TRASFORMAZIONI LA DOVE NECESSARIO
   */

  val join_DF = spark.sql(
    """
      |select t1.city, t1.country, t2.city_ascii, t2.lat, t2.lng, t2.capital
      |from tab_c1 t1
      |left join tab_c2 t2
      |on t1.city = t2.city_ascii and t1.country = t2.country
      |""".stripMargin).withColumn("city", when(!col("city").isNull, col("city"))
                        .otherwise("Error"))
                        .withColumn("country", when(!col("country").isNull, col("country"))
                        .otherwise("Error"))
                        .withColumn("city_ascii", when(!col("city_ascii").isNull, col("city_ascii"))
                        .otherwise("Error"))
                        .withColumn("lat", when(!col("lat").isNull, col("lat"))
                        .otherwise("-1"))
                        .withColumn("lng", when(!col("lng").isNull, col("lng"))
                        .otherwise("-1"))
                        .withColumn("capital", when(!col("capital").isNull, col("capital"))
                        .otherwise("Error"))
    //.filter("country = 'Russia' and city = 'Moscow'")
                        //.show()

 join_DF.createOrReplaceTempView("tab_join")

  /**
   * RAGGRUPPA PER PAESI CHE HANNO ->CITTA'<- CON (LAT AND LEN) = -1 ->E CONTA QUANTE SIANO PER OGNI PAESE<-
   */

 val DF_STATS1 = spark.sql(
   """
     |select j.country, count(j.city) as cnt_error_city
     |from tab_join j
     |where j.lat = -1 and j.lng = -1
     |group by j.country
     |""".stripMargin)//.show()

  /**
   * CONTA TUTTE LE CITTA' PER OGNI PAESE
   */

  val DF_STATS2 = spark.sql(
    """
      |select j.country, count(j.city) as tot_city_per_paese
      |from tab_join j
      |group by j.country
      |""".stripMargin)//.show()

  //val size = DF_STATS2.count()

  //println(size)

  DF_STATS1.createOrReplaceTempView("tab_s1")
  DF_STATS2.createOrReplaceTempView("tab_s2")

  /**
   * JOINO I DUE DF_STATS PER OTTENERE I DATI RICHIESTI
   */

  val join_STATS = spark.sql(
    """
      |select t1.country, t1.cnt_error_city, t2.tot_city_per_paese
      |from tab_s1 t1
      |left join tab_s2 t2
      |on t1.country = t2.country
      |""".stripMargin)//.show()

  /**
   * CALCOLO LA TERZA COLONNA RELATIVA ALLA PERCENTUALE DI ERRORE E ELIMINO UNA COLONNA NON PIU NECESSARIA
   */

  val STATS = join_STATS.withColumn("prc_error_city", col("cnt_error_city") * 100 / col("tot_city_per_paese") )
    .drop("tot_city_per_paese")

  /*try {

    STATS.repartition(1).write.option("header", true).format("csv").save("src/main/resources/datiEs2/STATS")

  } catch {

    case e: Exception => println("FILE GIA' ESISTENTI!")

  }*/


}
