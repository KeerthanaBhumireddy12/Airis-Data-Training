package Demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object US_Data extends App {

  // Percentage increase in Confirmed cases and Death cases before and After LockDown (April 1st, 2020)

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
    .withColumn("last_deaths",lag(col("deaths"),1).over(window))


  val percentageData = dataLag
    .withColumn("increase%Confirmed",
      ((col("confirmed")-col("last_confirmed"))/col("last_confirmed"))*100)
    .withColumn("increase%Death",
      ((col("deaths")-col("last_deaths"))/col("last_deaths"))*100);

  percentageData.show(percentageData.collect().length,false);

  // getting on march and april data

  val filteredData = percentageData.filter(percentageData("date_entry")>"2020-02-29" && percentageData("date_entry")<"2020-05-01");

  filteredData.show(filteredData.collect().length,false);

  // Percentage Increased and Percentage Death

  filteredData.select("date_entry","increase%Confirmed","increase%Death")
    .write
    .mode("overwrite")
    .format("csv")
    .save("C:\\Users\\keert\\Desktop\\Airis Data\\Question2_Output");


}