// test using the ~/training_materials/sparkdev/examples/streamtest.py script
// e.g. ~/training_materials/sparkdev/examples/streamtest.py localhost 4444 50 /home/training/training_materials/sparkdev/data/weblogs/*

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object StreamingRequestCount {

  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: StreamingRequestCount <hostname> <port>")
      System.exit(1)
    }  
    
    val hostname = args(0)
    val port = args(1).toInt

    val ssc = new StreamingContext(new SparkConf(),Seconds(2))
            
    val logs = ssc.socketTextStream(hostname,port)

    /* ----- Example 1: Count the number of requests for each user in each batch ----- */

    // create a new dstream with (userid,numrequests) pairs
    // count the total number of requests from each userID for each batch.
    val userreqs = logs. 
       map(line => (line.split(' ')(2),1)).
       reduceByKey((v1,v2) => v1+v2)

    // save the full list of user/count pairs for each patch to HDFS files
    userreqs.saveAsTextFiles("streamreq/reqcounts")

    /* ----- Example 2: Display the top 5 users in each batch ----- */

    // create a new dstream by transforming each rdd: 
    // map to swap key (userid) and value (numrequests, sort by numrequests in descending order
    val sortedreqs=userreqs.map(pair => pair.swap).transform(rdd => rdd.sortByKey(false))

    // print out the top 5 numrequests / userid pairs
    sortedreqs.foreachRDD((rdd,time) => {
        println("Top users @ " + time)
        rdd.take(5).foreach(pair => printf("User: %s (%s)\n",pair._2, pair._1))
        }
    )

    /* ----- Example 3: total counts for all users over time ----- */
    // checkpointing must be enabled for state operations
    ssc.checkpoint("checkpoints")

    def updateCount = (newCounts: Seq[Int], state: Option[Int]) => {
          // sum the new counts for the key (user)
          val newCount = newCounts.foldLeft(0)(_ + _)

          // get the previous count for the current user
          val previousCount = state.getOrElse(0)

          //the new state for the user is the old count plus the new count
          Some(newCount + previousCount)
        }
    
    val totalUserreqs = userreqs.updateStateByKey(updateCount)

    totalUserreqs.foreachRDD(rdd => {
        println("Total users: " + rdd.count())
        rdd.take(5).foreach(println)
        }
    )

    /* ----- Example 4: Display top 5 users over 30 second window, output every 6 seconds  ----- */

    val reqcountsByWindow = logs.
           map(line => (line.split(' ')(2),1)).
           reduceByKeyAndWindow((x: Int,y: Int) => x+y, Seconds(30),Seconds(6))
    val topreqsByWindow=reqcountsByWindow.
           map(pair => pair.swap).transform(rdd => rdd.sortByKey(false))
    topreqsByWindow.foreachRDD(rdd => {
        println("Top users by window:")
        rdd.take(5).foreach(pair => printf("User: %s (%s)\n",pair._2, pair._1))
        }
    )


    // after setup, start the stream
    ssc.start()
    ssc.awaitTermination()
  }
}
