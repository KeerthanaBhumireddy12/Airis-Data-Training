import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
object udfspark extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
  def ageCheck(age:Int): String ={
    if(age>18) "Y" else "N"
  }
  val sparkConf=new SparkConf()
  sparkConf.set("spark.app.name","my first application")
  sparkConf.set("spark.master","local[2]")

  val spark=SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()
  val df=spark.read
    .format("csv")
    .option("inferschema",true)
    .option("path","C:\\Users\\keert\\Desktop\\Airis Data\\scala ide\\readwrite dataset.txt")
    .load()

  import spark.implicits._
  val df1: Dataset[Row] = df.toDF("name","age","city")

 /* val parseAgeFunction=udf(ageCheck(_:Int):String)
  val df2=df1.withColumn("Adult(Y/N)",parseAgeFunction(col("age")))
  df2.show()
*/
  spark.udf.register("parseAgeFunction",ageCheck(_:Int):String)
  val df3=df1.withColumn("Adult(Y/N)",expr("parseAgeFunction(age)"))
  df3.show()

  spark.catalog.listFunctions().filter(x=>x.name=="parseAgeFunction").show()

  df1.createOrReplaceTempView("peopletable")
  spark.sql("select name,age,city,parseAgeFunction(age) as adult from peopletable").show()
  spark.stop()




}
