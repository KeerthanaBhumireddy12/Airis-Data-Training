package Demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}

object IndiaCases extends App{

  // India Data : LockDown, Partial Lockdown and No Lockdown
  val spark = SparkSession
    .builder()
    .appName("LockDown vs NoLockDown")
    .master("local[*]")
    .getOrCreate()

  // Schema Declaring
  val schema = new StructType()
    .add("date_entry",DateType,nullable = true)
    .add("country",StringType,nullable = true)
    .add("confirmed",IntegerType,nullable = true)
    .add("deaths",IntegerType,nullable = true)
    .add("recovered",IntegerType,nullable = true)

  val data = spark.read
    .option("header",false)
    .format("csv")
    .option("sep",",")
    .schema(schema)
    .option("path","C:\\Users\\keert\\Desktop\\Airis Data\\Corona.csv")
    .option("dateFormat","dd-MM-yyyy") // covert date to yyyy-mm-dd
    .load()

  // Getting India Data
  val India_Data = data.filter(data("Country")==="India");

  // Filtering March and April Data and May till 3rd
  val filteredData = India_Data.filter(India_Data("date_entry")>"2020-02-29" && India_Data("date_entry")<"2020-05-04");

  // March 25 - April 14: Partial, April 15 - May 3 Complete

  // Adding Lag Column
  val window = Window.orderBy(asc("date_entry"));

  val dataLag = filteredData
    .withColumn("last_confirmed",lag(col("confirmed"),1).over(window))
    .withColumn("last_deaths",lag(col("deaths"),1).over(window))


  val percentageData = dataLag
    .withColumn("increaseConfirmed",
      ((col("confirmed")-col("last_confirmed"))/col("last_confirmed"))*100)
    .withColumn("increaseDeath",
      ((col("deaths")-col("last_deaths"))/col("last_deaths"))*100);


  percentageData.createOrReplaceTempView("temp")

  // Averages:

  val ConfirmedpartialLDAvg = spark.sql("select avg(increaseConfirmed) from temp where date_entry>'2020-03-24' and date_entry<'2020-04-15' ")
  val ConfirmedcompleteLockDownAvg = spark.sql("select avg(increaseConfirmed) from temp where date_entry>'2020-04-14' and date_entry<'2020-05-04'")

  val DeathspartialLDAvg = spark.sql("select avg(increaseDeath) from temp where date_entry>'2020-03-24' and date_entry<'2020-04-15' ")
  val DeathscompleteLockDownAvg = spark.sql("select avg(increaseDeath) from temp where date_entry>'2020-04-14' and date_entry<'2020-05-04'")

  // Daily cases Confirmed

  val dailyConfirmed = percentageData
    .withColumn("ConfirmedDailyCases",col("confirmed")-col("last_confirmed"))

  // Daily cases Deaths
  val dailyDeaths = dailyConfirmed
    .withColumn("DeathDailyCases",col("deaths")-col("last_deaths"))

  // If no complete lockdown - (Avg Percent of Partial - Complete) + Cases: April 15th to May 3rd

  // Confirmed and Deaths: For partial
  import spark.implicits._
  val additionpercentConfirmed = ConfirmedpartialLDAvg.map{
    row=>row.getDouble(0) }.first() - ConfirmedcompleteLockDownAvg.map{ row=>row.getDouble(0)}.first();

  val additionpercentDeaths= DeathspartialLDAvg.map{
    row=>row.getDouble(0) }.first() - DeathscompleteLockDownAvg.map{ row=>row.getDouble(0)}.first();

  // If Only Parital Lockdown from March to May
  val totalPartial = dailyDeaths
    .withColumn("ConfirmedtotalPartial",when($"date_entry">"2020-04-14",(col("ConfirmedDailyCases")+col("ConfirmedDailyCases")*(additionpercentConfirmed/100)).cast("Int")).otherwise($"ConfirmedDailyCases"))
    .withColumn("DeathsTotalPartial",when($"date_entry">"2020-04-14",(col("DeathDailyCases")+col("DeathDailyCases")*(additionpercentDeaths/100)).cast("Int")).otherwise($"DeathDailyCases"))


  // If no lockdown : (Avg of No Lockdown - Avg of (partial Lockdown+complete)) + current cases

  // Average of No Lockdown - March 1st to March 25
  val NoLDCompleteAvg = spark.sql("select avg(increaseConfirmed) from temp where date_entry>'2020-02-29' and date_entry<'2020-03-26' ")
  val NoLDDeathAvg = spark.sql("select avg(increaseConfirmed) from temp where date_entry>'2020-02-29' and date_entry<'2020-03-26'")

  // For No LockDown

  val NLDAddConfirmed = NoLDCompleteAvg.map{
    row=>row.getDouble(0)}.first() -
    (ConfirmedpartialLDAvg.map{
      row=>row.getDouble(0) }.first() + ConfirmedcompleteLockDownAvg.map{ row=>row.getDouble(0)}.first())/2;

  val NLDAddDeaths= NoLDDeathAvg.map{
    row=>row.getDouble(0)}.first() - (DeathspartialLDAvg.map{
    row=>row.getDouble(0) }.first() + DeathscompleteLockDownAvg.map{ row=>row.getDouble(0)}.first())/2;

  // Adding the additional Percentage to the Data from March 25 to May

  val totalComplete = totalPartial
    .withColumn("ConfirmedtotalNLD",when($"date_entry">"2020-03-25",(col("ConfirmedDailyCases")+col("ConfirmedDailyCases")*(NLDAddConfirmed/100)).cast("Int")).otherwise($"ConfirmedDailyCases"))
    .withColumn("DeathsTotalNLD",when($"date_entry">"2020-03-25",(col("DeathDailyCases")+col("DeathDailyCases")*(NLDAddDeaths/100)).cast("Int")).otherwise($"DeathDailyCases"))

  // Saving the Data
  totalComplete.select("date_entry","ConfirmedDailyCases","DeathDailyCases","ConfirmedtotalPartial","DeathstotalPartial","ConfirmedtotalNLD","DeathsTotalNLD")
    .na.fill(0)
    .write
    .mode("overwrite")
    .format("csv")
    .save("C:\\Users\\keert\\Desktop\\Airis Data\\Question4_Output");

}