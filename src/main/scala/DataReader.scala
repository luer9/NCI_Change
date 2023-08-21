import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object DataReader {
  val spark: SparkSession = Configuration.sparkSession
  val _sc: SparkContext = spark.sparkContext
  import spark.implicits._
  case class Triple(sub: String, pred: String, obj: String)

  def getTriples(triplesFile: String): DataFrame = {
    val triDF = spark.sqlContext.read.parquet(triplesFile).toDF()
//    triDF.show(false)
    triDF
  }
  // print pred info
  def getPreds(predsFile: String): DataFrame = {
    val predsDF = spark.sqlContext.read.parquet(predsFile).toDF()
//    predsDF.show(false)
    predsDF
  }
  def getSos(sosFile: String): DataFrame = {
    val sosDF = spark.sqlContext.read.parquet(sosFile).toDF()
//    sosDF.show(false)
    sosDF
  }
  def read(inputFile: String): DataFrame = {
    val tris = _sc.textFile(inputFile)
      .map(str => str.split("\\s+"))
      .map(p => Triple(p(0), p(1), p(2) + " ."))
      .toDF()
    tris
  }
}
