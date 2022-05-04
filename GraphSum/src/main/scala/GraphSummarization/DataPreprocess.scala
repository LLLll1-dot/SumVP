package GraphSummarization

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import java.io.{File, PrintWriter}
import GraphActor.Main._sparkSession
import scala.io.Source

/**
  * @Author lqs
  * @Date 2022/3/11 19:10
  * @Function 功能简介
  */
object DataPreprocess {

  import _sparkSession.implicits._
  /**
    * prefix
    */

  //对于刚生成的lubm数据集进行处理，主要为去掉包含<unknown:namespace>的行，并去掉空格点，然后合并为一个文件，分隔符替换为\t
  def processLUBM(folder: String): Dataset[String] = {
    val f = _sparkSession.read.text(folder)
    val file = f.distinct()
    println("file edge count: "+f.count())
//    println("file edge count: "+f.count() + ", distinct count: " + file.count())
    val array: Dataset[String] = file
      .map(_.get(0).toString)
      .filter(x => !x.startsWith("#") && !x.contains("<unknown:namespace>"))
      .map(x => x.stripSuffix(" ."))
      .map(_.split(" ").mkString("\t"))
//    statisticDS(array)
    val mergeArray = mergePredDS(prefixDS(array))
    mergeArray
  }


  //处理刚生成的watdiv数据集，主要为去掉结尾的" ."，然后将相同的subject和object组合合在一起
  def processWatdiv(sourcePath: String): Dataset[String] = {
    val file: Dataset[String] = _sparkSession.read.text(sourcePath)
      .distinct()
      .map(_.get(0).toString)
      .map(x => x.stripSuffix(" .").trim)
    //统计文件
    statisticDS(file)
    val resFile: Dataset[String] = mergePredDS(prefixDS(file))
    resFile
    //    val resFile = prefixDS(file)
  }

  def statistic(array: Array[String]): Unit ={
    println("edge count(after filter repeat, # and <unknown:namespace>): "+array.size)
    val nodeCount = array.map(x => x.split("\t")).flatMap(x => Array(x(0), x(2))).distinct.length
    println("nodeCount: "+nodeCount)
  }
  def statisticDS(array: Dataset[String]): Unit ={
    println("edge count(after filter repeat, # and <unknown:namespace>): "+array.count())
    val nodeCount = array.map(x => x.split("\t")).flatMap(x => Array(x(0), x(2))).distinct.count()
    println("nodeCount: "+nodeCount)
  }
  def prefixDS(array: Dataset[String]): Dataset[String] = {
    val prefixes = Helper.broadcastPrefixes(_sparkSession)
    array.map(r => Helper.parseDataset(r.mkString, prefixes))
  }

  def mergePred(array: Array[String]): Array[String] = {
    val res = array.groupBy(x => x.split("\t")(0) + "\t" + x.split("\t")(2))
      .map(x => (x._1, x._2.map(y => y.split("\t")(1))))
      .map(x => (x._1, x._2.mkString(",")))
      .map(x => x._1.split("\t")(0) + "\t" + x._2 + "\t" + x._1.split("\t")(1))
      .toArray
    res
  }
  def mergePredDS(array: Dataset[String]): Dataset[String] = {
    array.map(x => {
      val xSplit = x.split("\t")
      (xSplit(0)+"\t"+xSplit(2), xSplit(1))
    })
      .groupByKey(_._1)
      .mapGroups{case(k, v) => (k, v.map(x => x._2).toList.sorted.mkString(",") )}
      .map(x => x._1.split("\t")(0) + "\t" + x._2 + "\t" + x._1.split("\t")(1))
  }

}
