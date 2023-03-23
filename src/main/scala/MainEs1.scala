import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.sum

object MainEs1 extends App {

  /**
   *
   * CREO SESSIONE SPARK
   *
   */

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Es1Scala")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /**
   *
   * LEGGO DA FILE I DATI E CREO I DATAFRAME
   *
   */

  val Opentau_odata_sesso = spark.read.option("header", true)
    .option("inferSchema", true)
    .option("delimiter", ";")
    .csv("src/main/resources/datiEs1/regpie-Opentau_odata_sesso_14804-all.csv")

  val Opentau_odata_immatricolazione_storici = spark.read.option("header", true)
    .option("inferSchema", true)
    .option("delimiter", ";")
    .csv("src/main/resources/datiEs1/regpie-Opentau_odata_immatricolazione_storici_14805-all.csv")

  val Opentau_odata_sesso_elaborato = Opentau_odata_sesso

  val Opentau_odata_immatricolazione_storici_clean = Opentau_odata_immatricolazione_storici

  //println("Dati per sesso")

  val DF_sex = Opentau_odata_sesso_elaborato.groupBy("tipo_veicolo", "provincia" )
    .agg(sum("femmine") + sum("maschi"))
    .withColumnRenamed("(sum(femmine) + sum(maschi))", "totale")
    //.show(50)

  //println("Dati per immatricolazioni")

  val DF_imm = Opentau_odata_immatricolazione_storici_clean.drop(
    "fascia_anno_immatric_nonstorico" ,
    "fascia_anno_immatric_20",
    "fascia_anno_immatric_30",
    "fascia_anno_immatric_40",
    "fascia_anno_immatric_50",
    "fascia_anno_immatric_60",
    "fascia_anno_immatric_70",
    "fascia_anno_immatric_oltre_70",
    "data_riferimento"
    )
    //.show(50)

  //println("Dati per sesso")

  DF_sex.createOrReplaceTempView("tb_sex")

  val query_sql_DF_sex = spark.sql(
    """
      |select *
      |from tb_sex
      |where tipo_veicolo = "MOTOVEICOLO"
      |""".stripMargin
  )//.show(50)

  //println("Dati per immatricolazioni")

  DF_imm.createOrReplaceTempView("tb_imm")

  val query_sql_DF_imm = spark.sql(
    """
      |select *
      |from tb_imm
      |where tipo_veicolo = "MOTOVEICOLO"
      |""".stripMargin
  )//.show(50)

  //println("Dati per sesso")

  val tot1 = query_sql_DF_sex.groupBy("tipo_veicolo","provincia").agg(sum("totale"))
    .withColumnRenamed("sum(totale)", "tot1")
    //.show()

  //println("Dati per immatricolazioni MOTOVEICOLO")

  val tot2 = query_sql_DF_imm.groupBy("tipo_veicolo", "provincia").agg(sum("totale"))
    .withColumnRenamed("sum(totale)", "tot2")
    //.show()

  /*println("Dati per sesso")
  query_sql_DF_sex.show(50)
  println("Dati per immatricolazioni MOTOVEICOLO")
  query_sql_DF_imm.show(50)*/

  tot1.createOrReplaceTempView("tb_1")
  tot2.createOrReplaceTempView("tb_2")

  tot1.show(50)
  tot2.show(50)

  val query_sql_differenza = spark.sql(
    """
      |select sum(t2.tot2) - sum(t1.tot1) as totale_differenza
      |from tb_1 t1
      |left join tb_2 t2
      |on t1.tipo_veicolo = t2.tipo_veicolo and t1.provincia = t2.provincia
      |""".stripMargin
  ).show()

  //DF_sex.join(DF_imm, DF_sex("tipo_veicolo") === DF_imm("tipo_veicolo") and DF_sex("provincia") === DF_imm("provincia"), "left").show()

}
