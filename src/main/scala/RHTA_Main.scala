
/*import NerDLPipeline._
#import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.util.Benchmark
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.NGram*/
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RHTA_Main extends App{
  val formatter = java.text.NumberFormat.getCurrencyInstance
  val performatter = java.text.NumberFormat.getPercentInstance
  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Security and Protection RHTA")
    /*.config("spark.driver.memory", "12G")
    .config("spark.kryoserializer.buffer.max","200M")
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")*/
    .getOrCreate()


   /* .builder()
    .appName("test")
    .master("local[*]")
    .config("spark.driver.memory", "12G")
    .config("spark.kryoserializer.buffer.max","200M")
    .config("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    .getOrCreate()

  */

  import spark.implicits._

  spark.sparkContext.setLogLevel("ERROR")



  val input = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", "\t").csv("/Users/cliftonbest/Documents/Development/R/GWCM_Exports/GWCM_FPDS_CURRENT_MONTH_JUNE_2018.tsv")

  val daterange_df = input.filter($"date_signed" >= "2016-10-01" && $"date_signed" <= "2017-09-30" )
  val rhta_df = daterange_df.filter( $"contract_name" === "Reduced Hazard Training Ammunition (RHTA)")



  //RHTA_Actuals(input)
  RHTA_filter_addressable(input)

  def RHTA_Actuals(local_input: org.apache.spark.sql.DataFrame) = {

    val RHTA_obs = local_input.select( $"dollars_obligated").agg(sum($"dollars_obligated")).first().getDouble(0)
    println("FY18 RHTA market " + formatter.format(RHTA_obs))

    val RHTA_count = local_input.select($"dollars_obligated").count()
    println("FY18 RHTA row count " + RHTA_count)

    //Break out the obligations RHTA obligations in to L1 cat and psc
    val RHTA_breakout_by_obs = local_input.groupBy($"level_1_category",$"product_or_service_code", $"product_or_service_description").agg(sum("dollars_obligated")).show(truncate = false)

    //Break out the obligations RHTA obligations in to L1 cat and psc
    val RHTA_breakout_by_count = local_input.groupBy($"level_1_category",$"product_or_service_code", $"product_or_service_description").agg(count($"product_or_service_code")).show(truncate = false)
  }


  def RHTA_filter_addressable(local_input: org.apache.spark.sql.DataFrame): Unit = {

    val rhta_regex = ".*(?i)(rhta|reduced haz+ard training ammunition|ammunition rhta|frangible).*"

    val ammo_rhta_addressable_market_df = local_input.filter($"description_of_requirement".rlike(rhta_regex) )

    // || $"contract_name" === "Reduced Hazard Training Ammunition (RHTA)").select($"description_of_requirement",$"contract_name", $"dollars_obligated", $"level_1_category", $"product_or_service_code",$"product_or_service_description")

    val addressable_obs_grouping = ammo_rhta_addressable_market_df.
      groupBy($"level_1_category",$"product_or_service_code", $"product_or_service_description").
      agg(sum("dollars_obligated")).show(truncate = false)

    val addressable_count_grouping = ammo_rhta_addressable_market_df.
      groupBy($"level_1_category",$"product_or_service_code", $"product_or_service_description").
      agg(count($"product_or_service_code")).show(truncate = false)

    val ammo_rhta_addressable_market_obs = ammo_rhta_addressable_market_df.select( $"dollars_obligated").agg(sum($"dollars_obligated")).first().getDouble(0)
    println("FY18 Re-shaped RHTA addressable market " + formatter.format(ammo_rhta_addressable_market_obs))


  }



}


