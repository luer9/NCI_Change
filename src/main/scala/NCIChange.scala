import DataReader.spark
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.{col, concat, concat_ws, explode, explode_outer, expr, first, lit, monotonically_increasing_id, posexplode, split}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File

object NCIChange {
  val spark: SparkSession = Configuration.sparkSession
  val _sc: SparkContext = spark.sparkContext
  import spark.implicits._
  def Main(NCIDir: String,
           sosDF: DataFrame,
           predsDF: DataFrame
          ): Unit = {
    val paths = new File(NCIDir)

    val schema1 = StructType(
      Seq(
        StructField("sub", StringType, true),
        StructField("obj", StringType, true),
        StructField("_pred", StringType, true)

      ))
    var finalRes = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1) // NCI + 二次模块
    var finalPath = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1) // NCI PATH

    for(path <- subdirs(paths)) {
      val infor = path.getName.split("_")
      val pp = infor.apply(0)
      val cir = infor.apply(1)
      val len = infor.apply(2).toInt
      val curNCIDF = spark.sqlContext.read.parquet(path + File.separator + "NCI").toDF()
      println("--[PP] " + pp)
      println("--[TYPE] " + cir)
      println("---[processing...] ")
      finalRes = finalRes.union(processNCI(curNCIDF, pp, len, cir)) // NCI + RE
      finalPath = finalPath.union(processNCI(curNCIDF, pp, len, "path")) // NCI(Path)
    }

    // 做映射
    val df = NCIMap(finalRes, sosDF, predsDF)
    val dfpath = NCIMap(finalPath, sosDF, predsDF)

//    df.show(false)
    // save 【nci + re】  file: xxxNCI
    df.rdd
      .map(_.toString()
        .replace("[","")
        .replace("]", "")
        .replace(",", " ")
      .replace(",", " ")).repartition(1)
      .saveAsTextFile(Configuration.outputDIR + File.separator + paths.getName + "NCI")

    val list = df.select("pred").distinct().toDF().map(_.toString()).collect().toSeq.mkString(" ")

    // [nci(path), 原数据（清洗）] NAME: XXXNCIPATH
    DataReader.read(Configuration.originFile).toDF()
      .filter(!lit(list).contains(col("pred"))).toDF()
      .union(dfpath)
      .rdd
      .map(_.toString()
        .replace("[", "")
        .replace("]", "")
        .replace(",", " ")
        .replace(",", " ")).repartition(1)
      .saveAsTextFile(Configuration.outputDIR + File.separator + paths.getName + "ORINCIPATH")



    // 【nci + re + 原数据（清洗）】  name: xxxexp
    DataReader.read(Configuration.originFile).toDF()
      .filter(! lit(list).contains(col("pred"))).toDF()
      .union(df)
      .rdd
      .map(_.toString()
        .replace("[", "")
        .replace("]", "")
        .replace(",", " ")
        .replace(",", " ")).repartition(1)
      .saveAsTextFile(Configuration.outputDIR + File.separator + paths.getName + "ORINCI")

    println("----[SAVE DONE]")


    //    df.collect().saveAsTextFile(Configuration.outputDIR + File.separator + paths.getName)
  }

  def NCIMap(finalRes: DataFrame, sosDF: DataFrame, predsDF: DataFrame): DataFrame = {

    finalRes
      .join(sosDF, finalRes("sub") === sosDF("sosID") )
      .drop("sub", "sosID")
      .select(col("sos").alias("sub"), col("_pred"), col("obj"))
      .join(predsDF, finalRes("_pred") === predsDF("predID"))
      .drop("_pred", "predID")
      .join(sosDF, finalRes("obj") === sosDF("sosID"))
      .drop("obj", "sosID")
      .select(col("sub"), col("pred"), col("sos").alias("obj"))
      .withColumn("obj", concat_ws(" ", col("obj"), lit(".")))
      .toDF()

  }

  def processNCI(curNCI: DataFrame,  pp: String, len: Int, cir: String): DataFrame = {
    val schema1 = StructType(
      Seq(
        StructField("sub", StringType, true),
        StructField("obj", StringType, true),
        StructField("_pred", StringType, true)

      ))
    var finalRes = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema1)
    // 拆开
    var df = curNCI.select(split(col("value"), ",").alias("path")).withColumn("id", monotonically_increasing_id)

    df = df.select(col("id"), posexplode(col("path")))
      .withColumn("value", concat_ws("",lit("path"), col("pos"))).drop("pos")
      .groupBy("id").pivot("value").agg(first("col")).drop("id")
      .toDF()
//    df.show(false)

    val maxLen = df.columns.length
    // left, right
    // 行与行之间 选择
    if(cir == "false") {
      for (left <- 0 to maxLen - 1) {
        for (right <- left + 1 to maxLen - 1) {
          finalRes = finalRes.union(df.select(col("path" + left).alias("sub"), col("path" + right).alias("obj")).na.drop()
            .withColumn("_pred", lit(pp))).toDF()
        }
      }
    }else if(cir == "true") {
      for (left <- 0 to maxLen - 1) {
        for (right <- 0 to maxLen - 1) {
          finalRes = finalRes.union(df.select(col("path" + left).alias("sub"), col("path" + right).alias("obj")).na.drop()
            .withColumn("_pred", lit(pp))).toDF()
        }
      }
    }else if(cir == "path") {
      for (left <- 0 to maxLen - 2) {
        val right = left + 1
        finalRes = finalRes.union(df.select(col("path" + left).alias("sub"), col("path" + right).alias("obj")).na.drop()
            .withColumn("_pred", lit(pp))).toDF()

      }
    }
    finalRes = finalRes.distinct()
    finalRes
  }
  //遍历目录
  def subdirs(dir: File): Iterator[File] = {
    val children = dir.listFiles.filter(_.isDirectory())
    children.toIterator
//    ++ children.toIterator.flatMap(subdirs _)
  }

}
