/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */

package queryExecutor

import org.apache.spark.sql.SparkSession

import java.io.{File, PrintWriter}
import scala.collection.mutable.ArrayBuffer

/**
 * Different settings for the Query Executor
 * TODO: implement reading of settings from config file.
 */
object Settings {

  // Settings to change
  val testLoop = 1;
  val partNumberExtVP = 50
  val partNumberVP = 200

  //  val sparkContext = loadSparkContext();
  //  val sqlContext = loadSqlContext();
  val sparkSession = loadSparkSession()

  var superEdgesDir = ""
  var hashtablePath = ""
  //  var initialGraphDir = ""
  var queryFile = ""
  //  var resultDir = ""
  //  var resultFile = ""
  var resultTimes = ""
  val resArray: ArrayBuffer[String] = ArrayBuffer()

  def loadUserSettings(SEDir: String, htPath:String, qrFile:String, resDir: String): Unit = {
    /*this.superNodeFile = {
      if (SEDir.endsWith("\\") || SEDir.endsWith("/")){
        SEDir + "supernodes.txt"
      } else {
        SEDir + "\\supernodes.txt"
      }
    }*/
    this.superEdgesDir = SEDir
    this.hashtablePath = htPath
    //    this.initialGraphDir = IGDir
    this.queryFile = qrFile
    this.resultTimes = resDir
    //    val splitSign: Char = {
    //      if (localRun) {
    //        '\\'
    //      } else {
    //        '/'
    //      }
    //    }
    this.superEdgesDir += "/"
    //    this.initialGraphDir += "/"
    //    this.superEdgesDir += "VP" + splitSign
    //    this.initialGraphDir += "IGVP/"

    /*val splitLine: Char = {
      if (localRun) {
        '\\'
      } else {
        '/'
      }
    }
    if (dbDir.charAt(dbDir.length - 1) != splitLine)
      this.databaseDir += splitLine
    this.queryFile = qrFile
    var p: Int = qrFile.lastIndexOf(splitLine)
    if (p < 0)
    {
      this.resultFile = "./results.txt";
      this.resultFileTimes = "./resultTimes.txt";
    } else {
      this.resultFile = qrFile.substring(0, p) + splitLine +"results.txt";
      this.resultFileTimes = qrFile.substring(0, p) + splitLine + "resultTimes.txt";
    }*/
  }

  def displayPath(): Unit ={
    println(s"hashtableFile: $hashtablePath")
    println(s"superEdgesDir: $superEdgesDir")
    //    println(s"initialGraphDir: $initialGraphDir")
    println(s"queryFile: $queryFile")
  }

  def addQueryResult(result: String): Unit ={
    resArray += result
  }

  def saveResultAsFile(): Unit = {
    val printWriter = new PrintWriter(new File(resultTimes))
    resArray.toArray.foreach(printWriter.println)
    printWriter.close()
  }

  def loadSparkSession(): SparkSession = {
    //    localRun = true
    val ss = SparkSession.builder()
      .appName("QueryExecutor")
      //      .config("SPARK_MASTER_WEBUI_PORT", "8090")
      //      .config("spark.some.config.option","some-value")
      //      .config("spark.eventLog.enabled","true")
      .getOrCreate()
    ss
  }

  def replaceSymbol(string: String): String = {
    string.replaceAll("<", "_L_")
      .replaceAll(">", "_B_")
      .replaceAll(":", "__")
      .replaceAll("~", "W_W")    //波浪线(wave)
      .replaceAll("-", "H_H")    //水平线
      .replaceAll("/", "D_D")    //斜线(diagonal)
      .replaceAll("\\.","P_P")   //点
      .replaceAll("#", "S_S")    //符号#
  }
}
