package example

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object WordCount {
  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: WordCount <file>")
      System.exit(1)
    }

    val sconf = new SparkConf().setAppName("Word Count").set("spark.ui.port","4141")
    val sc = new SparkContext(sconf)

    val file = args(0)
    val counts = sc.textFile(file).
       flatMap(line => line.split("\\W")).
       map(word => (word,1)).
       reduceByKey((v1,v2) => v1+v2)

    counts.take(10).foreach(println)
  }
}

