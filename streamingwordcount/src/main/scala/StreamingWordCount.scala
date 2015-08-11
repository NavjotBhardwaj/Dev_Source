import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkConf

object StreamingWordCount {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: StreamingWordCount <hostname> <port>")
      System.exit(1)
    }  
    
    val hostname = args(0)
    val port = args(1).toInt
    
    val ssc = new StreamingContext(new SparkConf(),Seconds(2))
    val lines = ssc.socketTextStream(hostname, port)
    val words = lines.flatMap(line => line.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey((x,y) => x+y)
    //wordCounts.saveAsTextFiles("wcountdir")
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
 }
}

