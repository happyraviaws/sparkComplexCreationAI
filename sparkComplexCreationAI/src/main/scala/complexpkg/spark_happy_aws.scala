package complexpkg
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source
object spark_happy_aws {
  def main(args:Array[String]):Unit = {
    
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Random URL Read and flatten it completely")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    val urlData = Source.fromURL("https://randomuser.me/api/0.8/?results=10")
    val strURLData = urlData.mkString
    
    val urlRDD = sc.parallelize(Seq("strURLData"))
    
    val urlDF = spark.read.json(urlRDD)
    urlDF.show(false)
  }
}