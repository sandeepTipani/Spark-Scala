package sparkdemo.sparkScala

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object TopNProductsByCategory {

  def getTopNPricedProducts(rec: (Int, Iterable[String]), topN: Int): Iterable[String] = {
    //Exract all the prices into a collection
    val productsList = rec._2.toList
    val topNPrices = productsList.
      map(x => x.split(",")(4).toFloat).
      sortBy(x => -x).
      distinct.
      slice(0, topN)

    val topNPricedProducts = productsList.
      sortBy(x => -x.split(",")(4).toFloat).
      filter(x => topNPrices.contains(x.split(",")(4).toFloat))

    topNPricedProducts
  }

  def main(args: Array[String]) {

    val conf = new SparkConf().
      setAppName("TopNProductsByCategory").
      setMaster("local")

    val sc = new SparkContext(conf)
    val topN = 3 
      //args(0).toInt
    val products = sc.textFile("/Users/Sid/Desktop/SPARK/Dense_Rank.txt")
    val productsFiltered = products.
      filter(rec => rec.split(",")(0).toInt != 685)
    val productsMap = productsFiltered.map(rec => (rec.split(",")(1).toInt, rec))
    val productsGBK = productsMap.groupByKey()

    productsGBK.
      flatMap(rec => getTopNPricedProducts(rec, topN)).
      collect().
      foreach(println)

//    productsGBK.
//      flatMap(rec => getTopNPricedProducts(rec, topN)).
//      saveAsTextFile("c:/users/viswa_000/Research/data/topNPricedProducts")

  }

}