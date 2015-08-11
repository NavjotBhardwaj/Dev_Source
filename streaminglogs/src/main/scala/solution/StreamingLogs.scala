package solution

import org.apache.spark._
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds

object StreamingLogs {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: StreamingLogs <hostname> <port>")
      System.exit(1)
    }  
    val hostname = args(0)
    val port = args(1).toInt
    
    // Configure the Streaming Context with a 1 second batch duration
    val ssc = new StreamingContext(new SparkConf(),Seconds(1))

    // Create a DStream of log data from the server at localhost:4444  
    val logs = ssc.socketTextStream(hostname,port)

    // count the total number of requests from each userID for each batch.
    // create a new dstream with (userid,numrequests) pairs
    val kbreqs = logs.filter(line => line.contains("KBDOC"))

    // Print out the count of each batch RDD in the stream
    kbreqs.foreachRDD(rdd => println("Number of KB requests: " + rdd.count()))

    // Save the filters logs
    kbreqs.saveAsTextFiles("logoutput/kblogs")

    // Challenge: every two seconds, display the total number of KB Requests over the 
    // last 10 seconds
    ssc.checkpoint("logcheckpt")
    kbreqs.countByWindow(Seconds(10),Seconds(2)).print()

    ssc.start()
    ssc.awaitTermination()
  }
}
