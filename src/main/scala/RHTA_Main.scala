
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object RHTA_Main extends App{

  val spark = SparkSession.builder
    .master("local[*]")
    .appName("Security and Protection RHTA")
    .getOrCreate()
  //.config("spark.driver.maxResultSize","4G")
  //.config("spark.driver.memory", "2G")
  //.config("spark.executor.memory", "2G")


  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  val input = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .option("sep", "\t").csv("/Users/destiny/Documents/development/GWCM_Exports/GWCM_FPDS_CURRENT_MONTH_JUNE_2018.tsv")

  val s_and_p_ammo = input
    .filter($"date_signed" >= "2017-10-01")
  //.filter($"level_1_category" === "Security and Protection")

  val formatter = java.text.NumberFormat.getCurrencyInstance
  val RHTA_obs = s_and_p_ammo
    .filter( $"contract_name" === "Reduced Hazard Training Ammunition (RHTA)")
    .select( $"dollars_obligated")
    .agg(sum($"dollars_obligated")).first().getDouble(0)
  println("FY18 RHTA market " + formatter.format(RHTA_obs))


  val RHTA_count = s_and_p_ammo
    .filter( $"contract_name" === "Reduced Hazard Training Ammunition (RHTA)")
    .select($"dollars_obligated")
    .count()
  println("FY18 RHTA row count " + RHTA_count)

  //1. Calculate the fuzzy addressable market for RHTA by aggregating the transactions that actually came from RHTA and adding the transactions harvested
  //   from the descrption of requirements field using the regex criteria below
  //val rhta_regex = ".*(?i)(train|reduced haz+ard|lead free).*"
  //val formatter = java.text.NumberFormat.getCurrencyInstance
  val rhta_regex = ".*(?i)(rhta|reduced haz+ard training ammunition|green ammo|haz+ard training ammunition|ammunition rhta|haz+ard frangible|rh frangible|lead free ammunition|training ammunition).*"
  val ammo_rhta_addressable_market_df = s_and_p_ammo.filter($"description_of_requirement".rlike(rhta_regex)) //|| $"contract_name" === "Reduced Hazard Training Ammunition (RHTA)")
    .select($"description_of_requirement",$"contract_name", $"dollars_obligated", $"level_1_category", $"product_or_service_description")
  val ammo_rhta_addressable_market_obs = ammo_rhta_addressable_market_df.agg(sum($"dollars_obligated")).first().getDouble(0)
  println("FY18 Re-shaped RHTA addressable market " + formatter.format(ammo_rhta_addressable_market_obs))

  //Break out the obligations RHTA obligations in to L1 cat and psc
  val RHTA_breakout_by_obs = s_and_p_ammo
    .filter( $"contract_name" === "Reduced Hazard Training Ammunition (RHTA)")
    .groupBy($"level_1_category", $"product_or_service_description").agg(sum("dollars_obligated")).show()

  //2. Provide re-shaped RHTA addressable composition: Breakout scope makes it clear that regex alone is not a rigid enough heuristic to characterize RHTA
  //   The addressable market is overinflated by all categories in which dont involve ammunition
  //   Corrective recommendation is to shape from the beginning to limit the input dataframe to level_1_category_group = "GWCM"
  val RHTA_addressable_market_breakout_by_obs = ammo_rhta_addressable_market_df
    .groupBy($"level_1_category", $"product_or_service_description").agg(sum("dollars_obligated")).show()

  //3. Calcuate the percentage utilization of RHTA in respect to the total addressable market for RHTA

  val rhta_addressable_market_cal = RHTA_obs / ammo_rhta_addressable_market_obs * 100

  //val performatter = java.text.NumberFormat.getPercentInstance

  println("FY18 RHTA obligations " + formatter.format(RHTA_obs))
  println("FY18 RHTA percent utilization " + rhta_addressable_market_cal)


  ammo_rhta_addressable_market_df.repartition(1).write.option("header", "true").csv("/Users/destiny/rhta_regex_df2.csv")

}
