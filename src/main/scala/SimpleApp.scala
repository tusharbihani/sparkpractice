import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SQLContext

object SimpleApp {
  def main(args: Array[String]) {
    //    val logFile = "YOUR_SPARK_HOME/README.md" // Should be some file on your system
    //    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
    //    val logData = spark.read.textFile(logFile).cache()
    //    val numAs = logData.filter(line => line.contains("a")).count()
    //    val numBs = logData.filter(line => line.contains("b")).count()
    //    println(s"Lines with a: $numAs, Lines with b: $numBs")
    //    spark.stop()

    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.ERROR)

    // Create a SparkContext using every core of the local machine, named RatingsCounter
    val sc = new SparkContext("local[*]", "SimpleApp")
    
    val sqlContext = new SQLContext(sc)
    
   

    // Load up each line of the ratings data into an RDD
    //    val lines = sc.textFile("../ml-100k/u.data")

    val lines = sc.textFile("data/airports-extended.csv")
    
    val df = lines.map(x => x.toString().split(","))


    val lineLengths = lines.map(s => s.length)
    println("lineLengths : "+lineLengths)
    val totalLength = lineLengths.reduce((a, b) => a + b)
    println("totalLength : "+totalLength)

    val records = lines.count();

//    println("Records = " + records)

//    println("First record = " + lines.first())

    // Convert each line to a string, split it out by tabs, and extract the third field.
    // (The file format is userID, movieID, rating, timestamp)
//    val ratings = lines.map(x => x.toString().split(",")(2))

    // Count up how many times each value (rating) occurs
//    val results = ratings.countByValue()

    // Sort the resulting map of (rating, count) tuples
//    val sortedResults = results.toSeq.sortBy(_._1)

    // Print each result on its own line.
//    sortedResults.foreach(println)

    //    val content = scala.io.Source.fromURL("http://ichart.finance.yahoo.com/table.csv?s=FB")
    //    println(content)

    //    val list = content.split("\n").filter(_ != "")
    //
    //    val rdd = sc.parallelize(list)
    //
    //    val df = rdd.toDF
    //
    //    println(df)

  }
}
