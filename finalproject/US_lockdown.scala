package Demo

import Demo.US_Data.{data, dataLag, percentageData}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}

object US_lockdown extends App{

  // Analyse the Cases if there was lockdown before April 1st 2020

  // Analysing in March and April Month

  val spark = SparkSession
    .builder()
    .appName("US Cases Analysis")
    .master("local[*]")
    .getOrCreate();

  val schema = new StructType()
    .add("date_entry",DateType,nullable = false)
    .add("confirmed",IntegerType,nullable = false)
    .add("deaths",IntegerType,nullable = false)
    .add("recovered",IntegerType,nullable = false)
    .add("country",StringType,nullable = false)

  val data = spark.read
    .format("csv")
    .option("header","true")
    .option("sep",",")
    .schema(schema)
    .option("path","C:\\Users\\keert\\Desktop\\Airis Data\\us_data.txt")
    .option("dateFormat","dd-MM-yyyy") // covert date to yyyy-mm-dd
    .load()

  // Adding Lag Column
  val window = Window.orderBy(asc("date_entry"));


  val dataLag = data
    .withColumn("last_confirmed",lag(col("confirmed"),1).over(window))

  // Percentage increase in confirmed cases
  val percentageData = dataLag
    .withColumn("increaseConfirmed",
      ((col("confirmed")-col("last_confirmed"))/col("last_confirmed"))*100)

  // Getting only March and April Data (LockDown Date April 1st 2020)

  val filteredData = percentageData.filter(percentageData("date_entry")>"2020-02-29" && percentageData("date_entry")<"2020-05-01");

  // March Average Rate of Increase in confirmed cases

  filteredData.createOrReplaceTempView("data_table");

  // Before Lock Down
  val avgPercentageMarch =
    spark.sql("select avg(increaseConfirmed) as mar_avg from data_table where date_entry like '2020-03-%'");

  avgPercentageMarch.select("mar_avg").show();

  // After Lock Down
  val avgPercentageApril =
    spark.sql("select avg(increaseConfirmed) as apr_avg from data_table where date_entry like '2020-04-%'");

  avgPercentageApril.select("apr_avg").show();

  import spark.implicits._
  val changeInPercentage = avgPercentageMarch.map{
    row=>row.getDouble(0) }.first() - avgPercentageApril.map{ row=>row.getDouble(0)}.first();

  System.out.println("Demo"+changeInPercentage);

  // Adding Daily cases column
  val dailyLD = filteredData
    .withColumn("WithLD_DailyCases",col("confirmed")-col("last_confirmed"))

  dailyLD.show(dailyLD.collect().length,false);

  // With Early LockDown the new cases would be [current new cases - (Average of March - April Average)]

  val dailyELD = dailyLD
    .withColumn("WithELD_DailyCases",
      (col("WithLD_DailyCases")-col("WithLD_DailyCases")*(changeInPercentage/100)).cast("Int"));

  dailyELD.show(dailyELD.collect().length,false);

  // Save to Write
  dailyELD.select("date_entry","WithELD_DailyCases","WithLD_DailyCases")
    .write
    .mode("overwrite")
    .format("csv")
    .save("C:\\Users\\keert\\Desktop\\Airis Data\\Question3_Output");

}