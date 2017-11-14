package sparkdemo.sparkScala
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext._ 
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
//import org.apache.spark.RDD._
 //import ss.implicits._
 import org.apache.spark.sql._
 import org.apache.spark.mllib.recommendation.{ALS,
  MatrixFactorizationModel, Rating}
//import sqlContext.implicits._
// input format MovieID::Title::Genres
 


object MovieRatingPrediction {
  
  case class Movie(movieId: Integer, title: String, genres: Seq[String])

// input format is UserID::Gender::Age::Occupation::Zip-code
case class User(userId: Integer, gender: String, age: Integer,
  occupation: Integer, zip: String)

//case class Rating(userId:Integer,movieId:Integer,rating:Integer)
  
  //function to parse input into Movie class
def parseMovie(str: String): Movie = {
      val fields = str.split(",")
      assert(fields.size == 3)
      Movie(fields(0).toInt, fields(1), Seq(fields(2)))
 }

// function to parse input into User class
def parseUser(str: String): User = {
      val fields = str.split(",")
      assert(fields.size == 5)
      User(fields(0).toInt, fields(1).toString, fields(2).toInt,
        fields(3).toInt, fields(4).toString)
 }
// function to parse input UserID::MovieID::Rating
// Into org.apache.spark.mllib.recommendation.Rating class
def parseRating(str: String): Rating= {
      val fields = str.split(",")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toInt)
}
  
def main(args: Array[String]) {
var sparkConf=new SparkConf().setAppName("Prediction").setMaster("local[2]").set("spark.executor.memory","1g");
 var sc=new SparkContext(sparkConf)
 //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 val sqlContext = new org.apache.spark.sql.SQLContext(sc)
 
import sqlContext.implicits._

   
// load the data into a RDD
//import sparkSession.implicits._
 //import ss.implicits._ 
 import sqlContext.implicits._
   //load the data into DataFrames
val usersDF = sc.textFile("/Users/Sid/Desktop/SPARK/User.txt").
  map(parseUser).toDF()
val moviesDF = sc.textFile("/Users/Sid/Desktop/SPARK/Movie.txt").
  map(parseMovie).toDF()
val ratingText = sc.textFile("/Users/Sid/Desktop/SPARK/Rating.txt")

// Return the first element in this RDD
ratingText.first()

// function to parse input UserID::MovieID::Rating
// Into org.apache.spark.mllib.recommendation.Rating class
/*def parseRating(str: String): Rating= {
      val fields = str.split("::")
      Rating(fields(0).toInt, fields(1).toInt, fields(2).toDouble)
}
*/
// create an RDD of Ratings objects
val ratingsRDD = ratingText.map(parseRating).cache()
println("OUTPUT PRINT TEST")

println("Total number of ratings: " + ratingsRDD.count())

println("Total number of movies rated: " +
  ratingsRDD.map(_.product).distinct().count())

println("Total number of users who rated movies: " +
  ratingsRDD.map(_.user).distinct().count())

// create a DataFrame from the ratingsRDD
val ratingsDF = ratingsRDD.toDF()

// register the DataFrames as a temp table
ratingsDF.registerTempTable("ratings")
moviesDF.registerTempTable("movies")
usersDF.registerTempTable("users")

usersDF.printSchema()

moviesDF.printSchema()

ratingsDF.printSchema()

  
  // Get the max, min ratings along with the count of users who have
// rated a movie.
val results = sqlContext.sql(
  """select movies.title, movierates.maxr, movierates.minr, movierates.cntu
    from(SELECT ratings.product, max(ratings.rating) as maxr,
    min(ratings.rating) as minr,count(distinct user) as cntu
    FROM ratings group by ratings.product ) movierates
    join movies on movierates.product=movies.movieId
    order by movierates.cntu desc""")

// DataFrame show() displays the top 20 rows in  tabular form
results.show()

 //Show the top 10 most-active users and how many times they rated
// a movie
val mostActiveUsersSchemaRDD = sqlContext.sql(
  """SELECT ratings.user, count(*) as ct from ratings
  group by ratings.user order by ct desc limit 10""")

println(mostActiveUsersSchemaRDD.collect().mkString("\n"))

// Find the movies that user 4169 rated higher than 4
val results1 = sqlContext.sql("""SELECT ratings.user, ratings.product,
  ratings.rating, movies.title FROM ratings JOIN movies
  ON movies.movieId=ratings.product
  where ratings.user=1 and ratings.rating > 2""")

results1.show


//PREDICTION ALGORITHM
 // Randomly split ratings RDD into training  
// data RDD (80%) and test data RDD (20%)
val splits = ratingsRDD.randomSplit(Array(0.8, 0.2), 0L)

val trainingRatingsRDD = splits(0).cache()
val testRatingsRDD = splits(1).cache()

val numTraining = trainingRatingsRDD.count()
val numTest = testRatingsRDD.count()
println(s"Training: $numTraining, test: $numTest.")

// build a ALS user product matrix model with rank=20, iterations=10
val model = (new ALS().setRank(20).setIterations(10)
  .run(trainingRatingsRDD))

  // Get the top 4 movie predictions for user 4169
val topRecsForUser = model.recommendProducts(3,2)
topRecsForUser.foreach(println)

/*
// get movie titles to show with recommendations
val movieTitles=moviesDF.map(array => (array(0), array(1))).collect()
 // .collectAsMap()

// print out top recommendations for user 4169 with titles
topRecsForUser.map(rating => (movieTitles(
  rating.product), rating.rating)).foreach(println)
*/
  // get user product pair from testRatings
val testUserProductRDD = testRatingsRDD.map {
  case Rating(user, product, rating) => (user, product)
}

// get predicted ratings to compare to test ratings
val predictionsForTestRDD  = model.predict(testUserProductRDD)

predictionsForTestRDD.take(10).mkString("\n")

// prepare predictions for comparison
val predictionsKeyedByUserProductRDD = predictionsForTestRDD.map{
  case Rating(user, product, rating) => ((user, product), rating)
}

// prepare test for comparison
val testKeyedByUserProductRDD = testRatingsRDD.map{
  case Rating(user, product, rating) => ((user, product), rating)
}

//Join the test with predictions
val testAndPredictionsJoinedRDD = testKeyedByUserProductRDD.
  join(predictionsKeyedByUserProductRDD)

// print the (user, product),( test rating, predicted rating)
testAndPredictionsJoinedRDD.take(3).mkString("\n")
println("Sample Test Data")
testRatingsRDD.foreach(println)
println("Top 2 Record for User 3")
topRecsForUser.foreach(println)
println("ALS Predictions based on the TEST data")
predictionsForTestRDD.foreach(println)
println("Combined Pridictions of User and ALS")
testAndPredictionsJoinedRDD.foreach(println)
val falsePositives = (
  testAndPredictionsJoinedRDD.filter{
    case ((user, product), (ratingT, ratingP)) => (ratingT <= 1 && ratingP >=3)
  })
//falsePositives.foreach(println)

falsePositives.count()
// Evaluate the model using Mean Absolute Error (MAE) between test
// and predictions
val meanAbsoluteError = testAndPredictionsJoinedRDD.map {
  case ((user, product), (testRating, predRating)) =>
    val err = (testRating - predRating)
    Math.abs(err)
}.mean()
println(meanAbsoluteError)

}

  
}
