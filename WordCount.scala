package sparkdemo.sparkScala
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext._ 
import org.apache.spark.SparkContext

object WordCount {
  def main(args:Array[String]):Unit ={
  
   var sparkConf=new SparkConf().setAppName("WordCount").setMaster("local[2]").set("spark.executor.memory","1g");
 var sc=new SparkContext(sparkConf)
 
 val orders=sc.textFile("/Users/Sid/Desktop/SPARK/User.txt")
 
 val count=orders.flatMap(rec=>(rec.split(","))).
     map(rec=>(rec,1)).
     reduceByKey(_+_).
     collect().
     foreach(println)
  
}
}