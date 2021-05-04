import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object XMLParse {
  def main(args : Array[String]){
      Logger.getLogger("org").setLevel(Level.OFF)
      
      val spark = SparkSession
                  .builder()
                  .appName("XMLParse")
                  .master("local[*]")
                  .config("spark.sql.warehouse.dir", "file:///C:/temp")
                  .getOrCreate()
                        
val products = spark.read.format("com.databricks.spark.xml").option("rowTag","product").load("F:\\VMWareWin10\\Module-5\\Spark\\XMLParser\\products.xml")

products.show(2)
val test = products.select("Description","createdDate")
test.show(2)

products.createOrReplaceTempView("prod")
val output = spark.sql("select * from prod")
//val output = spark.sql("select Description,createdDate from prod")
output.show()
}
}