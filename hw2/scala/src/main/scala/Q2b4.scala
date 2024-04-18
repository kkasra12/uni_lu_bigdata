import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex
import org.apache.spark.mllib.rdd.RDDFunctions._


object Q2b4 {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: SparkWordCount <input-directory(s)> <output-directory>")
      println(args)
      System.err.println("Correct arguments: <input-directory(s)> <output-directory>")
      System.exit(1)
    }
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val ctx = new SparkContext(sparkConf)
    
    val all_maps_results = ArrayBuffer.empty[RDD[((String, String), Int)]]
    for (inputDir <- args.dropRight(1)) {
      val this_map = ctx.wholeTextFiles(inputDir)
        .flatMap{case (path, text) => text.split("""[\s.]+""")}
         .sliding(2)
         .filter{case Array(word1, word2) => word1.matches("""\d+""") && word2.matches("""[a-z]+""")}
         .map{case Array(word1, word2) => ((word1, word2), 1)}
         .reduceByKey(_ + _)
      
      this_map.take(70).foreach(println)
       all_maps_results += this_map
    }
    val counts = all_maps_results.reduce(_ union _).reduceByKey(_ + _).take(100)
    ctx.parallelize(counts).saveAsTextFile(args.last)
    ctx.stop()
  }
}

// to run this code, you can use the following command
// spark-submit --class "SparkWordCount" target/scala-2.10/sparkwordcount_2.10-1.0.jar /path/to/input /path/to/output