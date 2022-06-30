import org.apache.spark.SparkContext

object Aggregate extends App{
  val sc=new SparkContext("local[*]","wordcount")
  val input=sc.textFile("C:\\Users\\keert\\Desktop\\search_data.txt")
  val words=input.flatMap(x=>x.split(" "))
  val wordMap=words.map(x=>(x,1))
  val aggregate=wordMap.reduceByKey((x,y)=>x+y).map(x=>(x._2,x._1)).sortByKey(false).map(x=>(x._2,x._1))
  aggregate.collect.foreach(println)
}
