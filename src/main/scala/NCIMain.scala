import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object NCIMain {
  val spark: SparkSession = Configuration.sparkSession
  val _sc: SparkContext = spark.sparkContext
  import spark.implicits._
  def main(args: Array[String]): Unit = {
    val originDataFile = "F:\\data\\uobm100.nt"
    val inputSosFile = "F:\\dbproject\\NCI_MAPPING\\data\\uobm100\\uobm100.so"
    val inputPredFile = "F:\\dbproject\\NCI_MAPPING\\data\\uobm100\\uobm100.p"
    val NCIDIR = "F:\\dbproject\\NCI_INDEX\\RESULT\\UOBM100"
    val outputDIR = "F:\\dbproject\\NCI_Change\\RESULT"
    //     todo: read triple (get data)
//    val triDF = DataProcess.DataReader.getTriples(args(0))
//    val predsDF = DataProcess.DataReader.getPreds(args(1))
//    val outputDIR = args(2)
//    Configuration.Configuration.loadUserSettings(args(0), args(1), args(2))

    // =============
    Configuration.loadUserSettings(originDataFile, inputSosFile, inputPredFile, NCIDIR, outputDIR)
    val sosDF = DataReader.getSos(inputSosFile)
    val predsDF = DataReader.getPreds(inputPredFile)

    val st = System.currentTimeMillis()
    // nci文件，so 映射，谓词映射
    NCIChange.Main(NCIDIR, sosDF, predsDF)
    val end = System.currentTimeMillis()
    println("[DONE] " + (end - st) + "ms")

  }


}
