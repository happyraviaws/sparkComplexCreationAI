package complexpkg
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf,SparkContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import scala.io.Source
import org.apache.spark.storage.StorageLevel._
object spark_happy_aws {
  def main(args:Array[String]):Unit = {
    
    val conf = new SparkConf().setMaster("local[*]").setAppName("Spark Random URL Read and flatten it completely")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    
    //Read the URL 
    val urlData = Source.fromURL("https://randomuser.me/api/0.8/?results=10")
    val strURLData = urlData.mkString
    
    //convert to RDD
    val urlRDD = sc.parallelize(Seq(strURLData))
    
    //Read REST API Data to JSON
    val urlDF = spark.read.json(urlRDD)
    
    //Raw Data need to see
    urlDF.show(false)
    urlDF.printSchema()
    
    val allColDF = urlDF.withColumn("results", explode(col("results")))
                        .select(col("nationality"),
                                col("results.user.*"),
                                col("results.user.location.*"),
                                col("results.user.name.*"),
                                col("results.user.picture.*"),
                                col("seed"),
                                col("version")
                            ).drop("location","name","picture")
                            .persist(org.apache.spark.storage.StorageLevel.MEMORY_ONLY)
    
    val urlSchema = allColDF.columns.mkString(",").split(",").toList
                         
    println("===== URL Schema  ======")
    urlSchema.foreach(println)
    
    val flattenDF = allColDF
                         .select(urlSchema.map(col):_*)
    
    
    /*
    val flattenDF = urlDF.withColumn("results", explode(col("results")))
                         .select(
                                   col("nationality"),
                                   //col("results.user.SSN"),
                                   col("results.user.cell"),
                                   col("results.user.dob"),
                                   col("results.user.email"),
                                   col("results.user.gender"),
                                   col("results.user.location.city"),
                                   col("results.user.location.state"),
                                   col("results.user.location.street"),
                                   col("results.user.location.zip"),
                                   col("results.user.md5"),
                                   col("results.user.name.first"),
                                   col("results.user.name.last"),
                                   col("results.user.name.title"),
                                   col("results.user.password"),
                                   col("results.user.phone"),
                                   col("results.user.picture.large"),
                                   col("results.user.picture.medium"),
                                   col("results.user.picture.thumbnail"),
                                   col("results.user.registered"),
                                   col("results.user.salt"),
                                   col("results.user.sha1"),
                                   col("results.user.sha256"),
                                   col("results.user.username"),
                                   col("seed"),
                                   col("version")
                                 )
                                 
   */                              
                                 
                                 
   flattenDF.show(false)
   flattenDF.printSchema()
  
  }
}