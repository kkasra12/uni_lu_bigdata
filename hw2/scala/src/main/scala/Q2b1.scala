import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex
// (1) Find all individual words (not numbers) with a count of 1000 +- 10

object Q2b1 {
  def main(args: Array[String]) {
    if (args.length < 2) {
      println("Usage: SparkWordCount <input-directory(s)> <output-directory>")
      println(args)
      System.err.println("Correct arguments: <input-directory(s)> <output-directory>")
      System.exit(1)
    }
    val word_pattern = new Regex("""[a-z]+""")
    val sparkConf = new SparkConf().setAppName("SparkWordCount")
    val ctx = new SparkContext(sparkConf)
    
    val all_maps_results = ArrayBuffer.empty[RDD[(String, Int)]]
    for (inputDir <- args.dropRight(1)) {
      val this_map = ctx.wholeTextFiles(inputDir).flatMap{case (path, text) =>
        word_pattern.findAllIn(text.toLowerCase)}.map(word => (word, 1))
      // this resduceByKey can be considered as a combiner
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
// spark-submit --class "SparkWordCount" --master local[4] target/scala-2.10/sparkwordcount_2.10-1.0.jar /path/to/input /path/to/output