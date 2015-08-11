package stubs

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
    
    // TODO
    println("Stub not yet implemented")

  }
}
