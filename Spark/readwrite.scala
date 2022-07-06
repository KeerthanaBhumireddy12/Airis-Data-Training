import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
object readwrite extends App {
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

  df.printSchema()
  df.show()

  val df1=df.toDF("name","age","city")
  df1.printSchema()
  df1.show()

  df.write
    .format("csv")
    .option("path","C:\\Users\\keert\\Desktop\\Airis Data\\scala ide\\newfolder1")
    .save()

  spark.stop()
}
