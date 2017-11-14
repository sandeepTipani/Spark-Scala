package sparkdemo.sparkScala
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext._ 
import org.apache.spark.SparkContext

object SparkSQL {
  

  def main(args: Array[String]) {

    /**
      * Create RDD and Apply Transformations
      */
var sparkConf=new SparkConf().setAppName("SPARKSQL").setMaster("local[2]").set("spark.executor.memory","1g");
 var sc=new SparkContext(sparkConf)
 val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 import sqlContext.implicits._
    val fruits = sc.textFile("/Users/Sid/Desktop/final.txt")
      .map(_.split(","))
      .map(frt => Fruits(frt(0),  frt(1).trim.toInt))
      .toDF();

    /**
      * Store the DataFrame Data in a Table to run queries on the data
      * Two ways to run queries on a data
      * 1.Register the DataFrame as a TempTable and use the sqlContext to query
      * 2.Direct access to the Hive tables using HiveContext/SqlContext, either of them works but the execution mode changes.
      * 
      * Using DataFrame functions directly, you can get the query like feel
      * 
      */
    fruits.registerTempTable("fruits")

    /**
      * Select Query on DataFrame
      */
    val records = sqlContext.sql("SELECT * FROM fruits")
    val gp=sqlContext.sql("SELECT id,count(1) FROM fruits as f where quantity>30 group by 1")
    val grpcount=gp.count

    gp.show()
    
    


    /**
      * To see the result data of allrecords DataFrame
      */
    records.show()

  }


case class Fruits(id: String, quantity: Int)
}



