import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex

object Q2a2 {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: SparkWordCount <input-directory(s)> <output-directory>")
      println(args)
      System.err.println("Correct arguments: <input-directory(s)> <output-directory>")
      System.exit(1)
    }
    val word_pattern = new Regex("""[0-9]{2,12}""")
    val sparkConf = new SparkConf().setAppName("SparkWordCount")
    val ctx = new SparkContext(sparkConf)
    
    val all_maps_results = ArrayBuffer.empty[RDD[(String, Int)]]
    for (inputDir <- args.dropRight(1)) {
      val this_map = ctx.wholeTextFiles(inputDir).flatMap{case (path, text) =>
        word_pattern.findAllIn(text)}.map(word => (word.toLowerCase, 1)).reduceByKey(_ + _)
      // this resduceByKey can be considered as a combiner
      // this_map.take(10).foreach(println)
      all_maps_results += this_map
    }
    val counts = all_maps_results.reduce(_ union _).reduceByKey(_ + _)
    counts.saveAsTextFile(args.last)
    println("Word count done!")
    ctx.stop()
  }
}

// to run this code, you can use the following command
// spark-submit --class "SparkWordCount" --master local[4] target/scala-2.10/sparkwordcount_2.10-1.0.jar /path/to/input /path/to/output