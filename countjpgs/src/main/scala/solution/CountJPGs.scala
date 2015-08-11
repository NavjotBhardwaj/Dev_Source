package solution

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object CountJPGs {
   def main(args: Array[String]) {
     if (args.length < 1) {
       System.err.println("Usage: CountJPGs <logfile>")
       System.exit(1)
     }

     val sc = new SparkContext()
     
     //Challenge: Set name and UI port programmatically
     //val conf = new SparkConf().setAppName("Count JPGs").set("spark.ui.port","4141")
     //val sc = new SparkContext(conf)

     val logfile = args(0)
     val jpgcount = sc.textFile(logfile).
        filter(line => line.contains(".jpg")).
        count()

     println( "Number of JPG requests: " + jpgcount)

   }
 }

