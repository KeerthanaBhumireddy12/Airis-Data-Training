import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row
case class person1 (name:String,age:Int,city:String)
object dataframedatasets extends App{
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

  val df1: Dataset[Row] = df.toDF("name","age","city")
  import spark.implicits._
  val ds1=df1.as[person1]
  ds1.groupByKey(x=>x.name)
  ds1.filter(x=>x.age>18).show()
  df1.filter("age>18").show()
  df1.withColumn("Increased_Value",col("age")*10).show()
  df1.createOrReplaceTempView("Person")
  spark.sql("SELECT name,age,city,age*100 as age100,'INDIA' as country FROM Person").show()
  df1.withColumn("age",col("age")*10).show()
}
