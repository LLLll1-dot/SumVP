/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */

package dataCreator

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SparkSession

import java.io.File
import scala.sys.process._
/**
 * The set of different help-functions 
 * TODO: move to the places, where they are used due to small number of 
 * functions
 */
object Helper {

  /**
   * transform table name for storage table in HDFS
   */
  def getPartName(v: String): String = {
    v.replaceAll("<", "_L_")
      .replaceAll(">", "_B_")
      .replaceAll(":", "__")
      .replaceAll("~", "W_W") //波浪线(wave)
      .replaceAll("-", "H_H") //水平线
      .replaceAll("/", "D_D") //斜线(diagonal)
      .replaceAll("\\.", "P_P") //点
      .replaceAll("#", "S_S") //符号#
  }
  
  /**
   * Float to String formated
   */
  def fmt(v: Any): String = v match {
    case d : Double => "%1.2f" format d
    case f : Float => "%1.2f" format f
    case i : Int => i.toString
    case _ => throw new IllegalArgumentException
  }
  
  /**
   * get ratio a/b as formated string
   */
  def ratio(a: Long, b: Long): String = {
    fmt(a.toFloat/b.toFloat)
  }    
  
  /**
   * remove directory in HDFS (if not exists -> it's ok :))
   */
//  def removeDirInHDFS(path: String) = {
//    val cmd = "hdfs dfs -rm -f -r " + path
//    val output = cmd.!!
//  }
  def removeDirInHDFS(spark: SparkSession, dirPath: String) = {

    val hconf = spark.sparkContext.hadoopConfiguration
    val dfs = FileSystem.get(hconf)

    var path = new Path(dirPath)
    if (dfs.exists(path)) {
      dfs.delete(path, true)
    }
  }

  def createDirInHDFS(spark: SparkSession, dirPath: String) = {

    val hconf = spark.sparkContext.hadoopConfiguration
    val dfs = FileSystem.get(hconf)

    var path = new Path(dirPath)
    if (!dfs.exists(path)) {
      dfs.mkdirs(path)
    }
  }

  /**
   * create directory in HDFS
   */
//  def createDirInHDFS(path: String) = {
//    try{
//      val cmd = "hdfs dfs -mkdir " + path
//      val output = cmd.!!
//    } catch {
//      case e: Exception => println("Cannot create directory->"
//                                   + path + "\n" + e)
//    }
//  }

  def writeLineToFile(line: String, fileName: String) = {
    val fw = new java.io.FileWriter(fileName, true)
    try {
      fw.write(line + "\n")
    }
    finally fw.close()
  }

//  def createDirInHDFS(destDirName: String) {
//    val dir = new File(destDirName);
//    if (dir.exists()) {
//      System.out.println("创建目录" + destDirName + "失败，目标目录已经存在");
//      return false;
//    }
//    /*if (!destDirName.endsWith(File.separator)) {
//      destDirName = destDirName + File.separator;
//    }*/
//    //创建目录
//    if (dir.mkdirs()) {
//      System.out.println("创建目录" + destDirName + "成功！");
//      return true;
//    } else {
//      System.out.println("创建目录" + destDirName + "失败！");
//      return false;
//    }
//  }

}
