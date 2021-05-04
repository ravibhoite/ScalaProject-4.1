import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object MyPOC {
    def main(args: Array[String]) {
   
    // Set the log level to only print errors
    Logger.getLogger("org").setLevel(Level.OFF)
    
     // Create a SparkContext using every core of the local machine
    val sc = new SparkContext("local[*]", "MyPOC")   
    
    // Read each line of my book into an RDD
    val input = sc.textFile("F:\\VMWareWin10\\Module-5\\Spark\\POCFile\\subtitle2.txt")
    
    // Split into words separated by a space character
    val words = input.flatMap(x => x.split(" "))
    
    val hashword = words.filter(x => x.contains('#'))
    val results = hashword.filter(x => x.charAt(0)== '#' && x.charAt(x.length()-1) == '#' )
    
    // Print the results.
    results.foreach(println)
  }
}