import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.SparkContext
object problem1 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","wordcount")
  val input=sc.textFile("C:\\Users\\keert\\Desktop\\Datasets\\customerorders_dataset_prblm1.csv")
  val mappedInput=input.map(x=>(x.split(",")(0),x.split(",")(2).toFloat))
  val total=mappedInput.reduceByKey((x,y)=>x+y)
  val sortedTotal=total.sortBy(x=>x._2,false)
  val result=sortedTotal.collect
  result.take(10).foreach(println)

}
