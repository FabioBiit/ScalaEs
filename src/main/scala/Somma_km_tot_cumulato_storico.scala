//import com.hortonworks.spark.sql.hive.llap.HiveWarehouseSession.HIVE_WAREHOUSE_CONNECTOR
//import com.hortonworks.spark.sql.hive.llap.{HiveWarehouseBuilder, HiveWarehouseSessionImpl}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._

object Somma_km_tot_cumulato_storico extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("cumulato")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  //Mi aggancio al db multi di move in e all'area di staging

  val db_multi = "db_regpie_multi_movein"
  val stg = "stg_regpie_regpie_movein"

  /*val hive = com.hortonworks.hwc.HiveWarehouseSession.session(spark).build()

  var storico = hive.executeQuery(
    s"""select targa_hmac, di_datafile,data_attivazione,categoria_veicolo,alimentazione_veicolo,classe_ambientale,a1_u_eco,a1_a_noeco,a1_u_noeco,a1_a_eco,a1_e,a2_e,a2_u_eco,a2_u_noeco,a2_a_noeco,a2_a_eco
                                    from $db_multi.movein_piemonte""")*/

  var storico = spark.sql(s"""select targa_hmac, di_datafile,data_attivazione,categoria_veicolo,alimentazione_veicolo,classe_ambientale,a1_u_eco,a1_a_noeco,a1_u_noeco,a1_a_eco,a1_e,a2_e,a2_u_eco,a2_u_noeco,a2_a_noeco,a2_a_eco
                                    from $db_multi.movein_piemonte""")

  val colNames1 = Array("a1_u_eco", "a1_a_noeco", "a1_u_noeco", "a1_a_eco", "a1_e", "a2_e", "a2_u_eco", "a2_u_noeco", "a2_a_noeco", "a2_a_eco")

  import org.apache.spark.sql.functions._

  for (colName <- colNames1) {
    storico = storico.withColumn(colName, translate(col(colName), ",", ".").cast("Double"))
  }

  val veicoli_attivi = storico.where("data_attivazione is not null").
    withColumn("Veicoli_a2", when(($"a2_a_eco" > 0 or $"a2_u_eco" > 0 or $"a2_e" > 0 or $"a2_u_noeco" > 0 or $"a2_a_noeco" > 0), lit(1))).
    withColumn("Veicoli_a1", when(($"a1_a_eco" > 0 or $"a1_u_eco" > 0 or $"a1_e" > 0 or $"a1_a_noeco" > 0 or $"a1_a_noeco" > 0), lit(1))).
    groupBy("targa_hmac", "di_datafile", "categoria_veicolo", "alimentazione_veicolo", "classe_ambientale").
    agg(sum("Veicoli_a2") as "veicoli_attivi_a2", sum("Veicoli_a1") as "veicoli_attivi_a1").
    na.fill(0.0, Seq("veicoli_attivi_a2")).
    na.fill(0.0, Seq("veicoli_attivi_a1")).
    withColumn("veicoli_attivi_totali", $"veicoli_attivi_a2" + $"veicoli_attivi_a1").
    orderBy(asc("di_datafile")).
    drop("data_attivazione")

  //Voglio calcolare il numero totale di km percorsi nelle varie categorie per ogni di_datafile

  var km_percorsi = storico.groupBy("targa_hmac", "di_datafile", "categoria_veicolo", "alimentazione_veicolo", "classe_ambientale").
    agg(sum("a1_u_eco").as("Km_a1_u_eco"), sum("a1_u_noeco").as("Km_a1_u_noeco"), sum("a1_a_noeco").as("Km_a1_a_noeco"), sum("a1_a_eco").as("Km_a1_a_eco"), sum("a1_e").as("Km_a1_e"), sum("a2_e").as("Km_a2_e"), sum("a2_u_eco").as("Km_a2_u_eco"), sum("a2_u_noeco").as("Km_a2_u_noeco"), sum("a2_a_noeco").as("Km_a2_a_noeco"), sum("a2_a_eco").as("Km_a2_a_eco")
    ).orderBy(asc("di_datafile"))

  //nel caso ci fossero valori a NULL per questa colonna, vengono sostituiti da 0

  val colNames2 = Array("Km_a1_u_eco", "Km_a1_a_noeco", "Km_a1_u_noeco", "Km_a1_a_eco", "Km_a1_e", "Km_a2_e", "Km_a2_u_eco", "Km_a2_u_noeco", "Km_a2_a_noeco", "Km_a2_a_eco")

  for (colName <- colNames2) {
    km_percorsi = km_percorsi.na.fill(0, Seq(colName)).
      withColumn(colName, round(col(colName), 2))
  }

  //km_percorsi.show(1000)

  //Creo delle nuove colonne per effettuare dei totali

  val km_percorsi_01 = km_percorsi.withColumn("Km_tot_eco", round($"Km_a1_u_eco" + $"Km_a1_a_eco" + $"Km_a2_u_eco" + $"Km_a2_a_eco", 2)).
    withColumn("Km_tot_noeco", round($"Km_a1_u_noeco" + $"Km_a1_a_noeco" + $"Km_a2_u_noeco" + $"Km_a2_a_noeco", 2)).
    withColumn("Km_tot_a1", round($"Km_a1_u_eco" + $"Km_a1_a_noeco" + $"Km_a1_u_noeco" + $"Km_a1_a_eco" + $"Km_a1_e", 2)).
    withColumn("Km_tot_a2", round($"Km_a2_e" + $"Km_a2_u_eco" + $"Km_a2_u_noeco" + $"Km_a2_a_noeco" + $"Km_a2_a_eco", 2)).
    withColumn("Km_tot_u", round($"Km_a1_u_eco" + $"Km_a1_u_noeco" + $"Km_a2_u_eco" + $"Km_a2_u_noeco", 2)).
    withColumn("Km_tot_a", round($"Km_a1_a_noeco" + $"Km_a1_a_eco" + $"Km_a2_a_noeco" + $"Km_a2_a_eco", 2)).
    withColumn("Km_tot_e", round($"Km_a1_e" + $"Km_a2_e", 2)).
    withColumn("Km_tot_a1_u", round($"Km_a1_u_eco" + $"Km_a1_u_noeco", 2)).
    withColumn("Km_tot_a1_a", round($"Km_a1_a_eco" + $"Km_a1_a_noeco", 2)).
    withColumn("Km_tot_a2_u", round($"Km_a2_u_eco" + $"Km_a2_u_noeco", 2)).
    withColumn("Km_tot_a2_a", round($"Km_a2_a_eco" + $"Km_a2_a_noeco", 2)).
    withColumn("Km_tot", round($"Km_a1_u_eco" + $"Km_a1_a_noeco" + $"Km_a1_u_noeco" + $"Km_a1_a_eco" + $"Km_a1_e" + $"Km_a2_e" + $"Km_a2_u_eco" + $"Km_a2_u_noeco" + $"Km_a2_a_noeco" + $"Km_a2_a_eco", 2)).na.fill(0)


  val windowSpec = Window.partitionBy("targa_hmac", "categoria_veicolo", "alimentazione_veicolo", "classe_ambientale").
    orderBy("targa_hmac", "di_datafile", "categoria_veicolo", "alimentazione_veicolo", "classe_ambientale")

  val km_percorsi_02 = km_percorsi_01.
    withColumn("diff_km_negativi", $"Km_tot" - lag("Km_tot", 1, 0).over(windowSpec)).
    withColumn("diff_km_prev_day", when(($"Km_tot" - lag("Km_tot", 1, 0).over(windowSpec)) > 0, ($"Km_tot" - lag("Km_tot", 1, 0).over(windowSpec))).
      when(($"Km_tot" - lag("Km_tot", 1, 0).over(windowSpec)) <= 0 && $"diff_km_negativi" > -100, lit(0).cast("Double")).
      when(($"Km_tot" - lag("Km_tot", 1, 0).over(windowSpec)) <= 0 && $"diff_km_negativi" < -100, $"Km_tot").
      otherwise(lit(0).cast("Double"))
    )

  val newDf = km_percorsi_02.
    withColumn("km_tot_cumulati", sum($"diff_km_prev_day").over(windowSpec))

  /*newDf.select("targa_hmac", "di_datafile", "categoria_veicolo","alimentazione_veicolo","classe_ambientale","km_tot", "diff_km_prev_day", "km_tot_cumulati").
    filter($"km_tot" > 0 && $"targa_hmac" === "0027703c2e0b3e7129fe26bbf0523828740a7530d422569554a68804710e86ba").
    orderBy($"targa_hmac", $"categoria_veicolo",$"alimentazione_veicolo",$"classe_ambientale", $"di_datafile").show(500, false)

newDf.select("targa_hmac", "di_datafile", "categoria_veicolo","alimentazione_veicolo","classe_ambientale","km_tot", "diff_km_prev_day", "km_tot_cumulati", "diff_km_negativi").
    filter($"diff_km_negativi" < 0).
    orderBy($"targa_hmac", $"categoria_veicolo",$"alimentazione_veicolo",$"classe_ambientale", $"di_datafile").show(500, false)*/

  //Salvo la tabella in modalità overwrite
  newDf.write.format("orc").mode("overwrite").saveAsTable(s"""stg_regpie_regpie_movein.andamento_temporale_arpa_2022_new""")

}
