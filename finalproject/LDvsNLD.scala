package Demo

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DateType, IntegerType, StringType, StructType}

object LDvsNLD extends App {
  // Japan No LockDown - March and April Month
  // Germany LockDown - March 27th (March and April Month Analysis)

  val spark = SparkSession
    .builder()
    .appName("LockDown vs NoLockDown")
    .master("local[*]")
    .getOrCreate()

  // Schema Declaring
  val schema = new StructType()
    .add("date_entry",DateType,nullable = true)
    .add("confirmed",IntegerType,nullable = true)
    .add("deaths",IntegerType,nullable = true)
    .add("recovered",IntegerType,nullable = true)
    .add("country",StringType,nullable = true)

  val japan_data = spark.read
    .option("header",false)
    .format("csv")
    .option("sep",",")
    .schema(schema)
    .option("path","C:\\Users\\keert\\Desktop\\Airis Data\\japan_data.txt")
    .option("dateFormat","dd-MM-yyyy") // covert date to yyyy-mm-dd
    .load()

  val germany_data = spark.read
    .option("header",false)
    .format("csv")
    .option("sep",",")
    .schema(schema)
    .option("path","C:\\Users\\keert\\Desktop\\Airis Data\\germany_data.txt")
    .option("dateFormat","dd-MM-yyyy") // covert date to yyyy-mm-dd
    .load()

  japan_data.show(10,false);

  // Filtering the Japan and Germany Data for Month March and April

  val japan_filter = japan_data.filter(japan_data("date_entry")>"2020-02-29" && japan_data("date_entry")<"2020-05-01");

  val germany_filter = germany_data.filter(germany_data("date_entry")>"2020-02-29" && germany_data("date_entry")<"2020-05-01");

  // Adding Lag Column into the Data for Japan and Germany: For Deaths, Confirmed and Recovered

  val window = Window.orderBy("date_entry");

  val japan_lag_added = japan_filter
    .withColumn("confirmed_lag",lag("confirmed",1).over(window))
    .withColumn("deaths_lag",lag("deaths",1).over(window))
    .withColumn("recovered_lag",lag("recovered",1).over(window));

  val germany_lag_added = germany_filter
    .withColumn("confirmed_lag",lag("confirmed",1).over(window))
    .withColumn("deaths_lag",lag("deaths",1).over(window))
    .withColumn("recovered_lag",lag("recovered",1).over(window));

  // Adding Increase percentage for confirmed, deaths and recovered

  val percentage_japan_added = japan_lag_added
    .withColumn("confirmed%increase",((col("confirmed")-col("confirmed_lag"))/col("confirmed_lag"))*100)
    .withColumn("death%increase",((col("deaths")-col("deaths_lag"))/col("deaths_lag"))*100)
    .withColumn("recovered%increase",((col("recovered")/col("recovered_lag"))/col("recovered_lag"))*100)

  val percentage_germany_added = germany_lag_added
    .withColumn("confirmed%increase",((col("confirmed")-col("confirmed_lag"))/col("confirmed_lag"))*100)
    .withColumn("death%increase",((col("deaths")-col("deaths_lag"))/col("deaths_lag"))*100)
    .withColumn("recovered%increase",((col("recovered")/col("recovered_lag"))/col("recovered_lag"))*100)


  // Removing unwanted columns and making null column values to 0
  val final_japan = percentage_japan_added.drop("confirmed_lag","recovered_lag","deaths_lag")
    .na.fill(0);;
  val final_germany = percentage_germany_added.drop("confirmed_lag","recovered_lag","deaths_lag")
    .na.fill(0);

  // Displaying Data
  final_japan.show(20,false);
  final_germany.show(20,false);

  // Saving the Data: Japan and Germany
  final_japan.select("date_entry","confirmed%increase","death%increase","recovered%increase")
    .write.mode("overwrite")
    .format("csv")
    .save("C:\\Users\\keert\\Desktop\\Airis Data\\Question1_Japan_Output");

  final_germany.select("date_entry","confirmed%increase","death%increase","recovered%increase")
    .write.mode("overwrite")
    .format("csv")
    .save("C:\\Users\\keert\\Desktop\\Airis Data\\Question1_Germany_Output");
}