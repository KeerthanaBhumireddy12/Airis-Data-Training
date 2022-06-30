import org.apache.spark.SparkContext
import org.apache.log4j.Logger
import org.apache.log4j.Level
object problem2 extends App{
  def extract(row:String)={
    val words=row.split(",")
    val a=words(2).toInt
    val b=words(3).toInt
    (a,b)
  }
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc=new SparkContext("local[*]","wordcount")
  val input=sc.textFile("C:\\Users\\keert\\Desktop\\Datasets\\friendsdataNew_dataset_prblm2.txt")
  val mappedInput=input.map(extract)
  //mappedInput.collect()
  val mappedFinal=mappedInput.map(x=>(x._1,(x._2,1)))
  val totals=mappedFinal.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
  val avg=totals.map(x=>(x._1,x._2._1/x._2._2))
  avg.foreach(println)

}
