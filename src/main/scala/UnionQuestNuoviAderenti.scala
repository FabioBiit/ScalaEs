import org.apache.spark.sql.{SaveMode, SparkSession}
import scala.collection.mutable.ListBuffer
import org.apache.spark.sql.functions._

import java.io.File

/** *
 *
 * -------------------------------------------LEGGENDA VARIABILI UTILIZZATE----------------------------------------------
 *
 * var cartelle ==> PATH DELLA CARTELLA: QuestNuoviAderenti
 * var lista_cartelle ==> NECESSARIA PER SALVARE IL PATH DELLE SOTTO CARTELLE TROVATE IN: QuestNuoviAderenti
 * var numero ==> NECESSARIO PER ACCEDERE ALL'INDEX CORRETTO DELLA LISTA: lista_cartelle
 * val dir ==> ACCEDO ALLA/E SOTTO CARTELLA/E
 * val lista_last_id ==> NECESSARIA PER SALVARE I MAX(ID_risposta) ESTRATTI DA OGNI DF
 * var num ==> NECESSARIO PER CREARE CICLICAMENTE UNA NUOVA TEMPVIEW
 * var elemento1 ==> MI SERVE PER ACCEDERE AL MAX ID DEL FILE PRECEDENTE
 * var elemento2 ==>  MI SERVE PER ACCEDERE AL MAX ID DEL FILE CORRENTE
 * var n_domande ==>  MI SERVE PER RINOMINARE LE DOMANDE DINAMICAMENTE PER OGNI FILE
 * val data ==> !!! MI GARANTISCE IL CORRETTO ACCESSO ALLA DATA PRESENTE NEL NOME DEL FILE !!!
 * val ele3 ==> !!! CONSIDERO IL MAX ID DEL FILE PRECEDENTE INCREMENTATO DI 1 PER NON AVERE DUPLICATI INDESIDERATI !!!
 *
 * ---------------------------------------------------------------------------------------------------------------------
 *
 */

object UnionQuestNuoviAderenti extends App {

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("UnionQuestNuoviAderenti")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  /**
   * 1° CICLO FOR:
   * ESTRAGGO DALLA CARTELLA /QuestStileGuida TUTTE LE SOTTO CARTELLE
   */

  var cartelle = new File("src/main/resources/QuestNuoviAderenti")
  var lista_cartelle = new ListBuffer[File]()
  var numero = -1

  for (c <- cartelle.listFiles.filter(_.isDirectory)) {

    lista_cartelle += c

    /** EFFETTUO IL SALVATAGGIO DELLE SOTTO CARTELLE NELLA LISTA: lista_cartelle */
    numero += 1
    /** INCREMENTO L'INDICE AD OGNI CICLO */

    val dir = new File(lista_cartelle.apply(numero).toString)
    val lista_last_id = new ListBuffer[Int]()
    val lista_tempview = new ListBuffer[String]()
    var num = 0

    /**
     * 2° CICLO FOR:
     * LEGGE TUTTI I FILE .XLSX NEL PATH DESIGNATO
     */

    for (n_file <- dir.listFiles) {

      num += 1

      /**
       * LETTURA E CREAZIONE DEI DATAFRAME DA FILE EXCEL
       */

      val DF_EX = spark.read
        .format("com.crealytics.spark.excel")
        .option("header", "true")
        .option("treatEmptyValuesAsNulls", "true")
        .option("inferSchema", "true")
        .load(n_file.toString)
        .withColumnRenamed("ID risposta", "ID_risposta")
        .withColumn("ID_risposta", $"ID_risposta".cast("Int"))

      DF_EX.createOrReplaceTempView("tab" + num.toString)

      lista_tempview += "tab" + num.toString

      /**
       * ESTRAGGO PER OGNI DF, DALLA COLONNA ID_risposta IL VALORE MAX
       * E LO SALVO IN UNA ListBuffer[Int] COSì DA POTER
       * CREARE SUCCESSIVAMENE UN RANGE DI VALORI
       */

      val last_id = spark.sql(
        s"""
           |select max(ID_risposta) as last_id
           |from tab${num.toString}
           |""".stripMargin).collect()(0).getInt(0)

      lista_last_id += last_id

    }

    /** ELIMINA TUTTE LE TEMP VIEW ECCETTO L'ULTIMA */
    lista_tempview.dropRight(1).foreach(tab => spark.catalog.dropTempView(tab))

    /**
     * CREO UN DF DI APPOGGIO
     * CREANDO LA COLONNA Data_invio LEGGENDOLA DAL NOME DEL FILE (SPLITTANDO LA STRINGA)
     */

    var elemento1 = -1
    var elemento2 = 0
    var n_domande = 0

    val out_list = lista_last_id.toList
    val nome_file_split = dir.listFiles.apply(0).toString.split("_").toList
    val data = nome_file_split.apply(5)

    var DF_appoggio = spark.sql(
      s"""
         |select *
         |from tab${num.toString} /** num AVRà IL VALORE DELL'ULTIMA TEMPVIEW, QUELLA DELL'ULTIMO FILE, CHE CONTIENE TUTTI GLI ID */
         |""".stripMargin).withColumn("Data_invio", to_date(lit(data), "yyyyMMdd"))

    /**
     * CONTROLLA SE ESISTE LA COLONNA "Data invio",
     * SE TRUE LA ELIMINA, ALTRIMENTI PROCEDE CON L'ALTRO STEP
     */

    DF_appoggio.columns.foreach(_ => {
      if (DF_appoggio.columns.contains("Data invio")) {
        DF_appoggio = DF_appoggio.drop("Data invio")
      } else {
        DF_appoggio = DF_appoggio
      }
    })

    /** ESCLUDO LE 2 COLONNE CHE NON VOGLIO RINOMINARE */
    val domande = DF_appoggio.columns.filter(x => x != "ID_risposta" && x != "Data_invio")

    /**
     * 3° CICLO FOR:
     * RINOMINO LE DOMANDE DEL DF_appoggio
     */

    for (d <- domande) {
      n_domande += 1
      DF_appoggio = DF_appoggio.withColumnRenamed(d, "domanda" + n_domande.toString)
    }

    n_domande = 0

    /** BISOGNA STARE ATTENTI A GESTIRE OUTOFBOUNDEXCEPTION */ // (FATTO)

    /**
     * 4° CICLO FOR:
     * SUL DF_appoggio APPLICO UN PASSAGGIO DI DATA-PREPARATION
     * CON WITHCOLUMN, WHEN E OTHERWISE AL FINE DI INSERIRE LE DATE CORRETTE
     */

    for (file <- dir.listFiles) {

      elemento1 += 1
      elemento2 += 1

      if (elemento2 <= out_list.length - 1) { /** MI GARANTISCE CHE L'ELEMENTO 2 NON ECCEDA MAI GLI ELEMENTI DELL'ARRAY */

        val ele3 = out_list.apply(elemento1) + 1
        val ele4 = out_list.apply(elemento2)

        val nome_file_split = dir.listFiles.apply(elemento2).toString.split("_").toList
        val data = nome_file_split.apply(5)

        DF_appoggio = DF_appoggio
          .withColumn("Data_invio", when(col("ID_risposta") between(ele3, ele4), to_date(lit(data), "yyyyMMdd"))
            .otherwise(col("Data_invio")))

      } else { /** ALTRIMENTI ESCI DALL'IF */

        println("*********************************DONE***************************************")

      }

    }

    try {
      DF_appoggio.write
        .mode(SaveMode.Overwrite) /** SI PUò USARE ANCHE: SaveMode.Append */
        .format("com.crealytics.spark.excel")
        .option("header", true)
        .option("dateFormat", "yyyy-MM-dd")
        .save("src/main/resources/NewQuest/QuestUnionNuoviAderenti" + (numero + 1).toString + ".xlsx")
    } catch {

      case e: Exception => println("FILE GIA' ESISTENTI!")

    }

  }

}