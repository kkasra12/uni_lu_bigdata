import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex
import org.apache.spark.mllib.rdd.RDDFunctions._



// (2) Find all word pairs (not numbers) with a count of exactly 1,000 (using a distance of m = 1 for
// immediate neighbours of words in the mappers).

object Q2b2 {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: SparkWordCount <input-directory(s)> <output-directory>")
      println(args)
      System.err.println("Correct arguments: <input-directory(s)> <output-directory>")
      System.exit(1)
    }
    val word_pattern = new Regex("""[a-z]+""")
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val ctx = new SparkContext(sparkConf)
    
    val all_maps_results = ArrayBuffer.empty[RDD[((String, String), Int)]]
    for (inputDir <- args.dropRight(1)) {
      val this_map = ctx.wholeTextFiles(inputDir)
        .flatMap{case (path, text) => word_pattern.findAllIn(text.toLowerCase)}
        .sliding(2)
        .map{case Array(word1, word2) => ((word1, word2), 1)}
        .reduceByKey(_ + _)
      
      // this_map.take(10).foreach(println)
      all_maps_results += this_map
    }
    val counts = all_maps_results.reduce(_ union _).reduceByKey(_ + _).filter{case (word, count) => (count - 1000).abs < 10}
    counts.saveAsTextFile(args.last)
    println("Word count done!")
    ctx.stop()
  }
}

// to run this code, you can use the following command
// spark-submit --class "SparkWordCount" target/scala-2.10/sparkwordcount_2.10-1.0.jar /path/to/input /path/to/output