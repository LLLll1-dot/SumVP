/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */

package dataCreator

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SQLContext, SparkSession}

/**
 * Different settings for the DataSetGenerator
 * TODO: implement reading of settings from config file.
 */
object Settings {
  val sparkSession = loadSparkSession()
  
  // ScaleUB (Scale Upper Bound) controls the storage overhead.
  // Set it to 1 (default) to store all possible ExtVP tables. Reduce this value
  // to avoid storage of ExtVP tables having size bigger than ScaleUB * (size of
  // corresponding VP table). In this way, we avoid storage of the biggest 
  // tables, which are most ineffective at the same time, since they are 
  // not able to significantly improve the selectivity of correponding triple 
  // pattern
  // database name
  //val baseName = "WatDiv1000M_test"
  // the database directory in HDFS
  var workingDir = ""
  // path to the input RDF file  
  var inputRDFSet = ""
  // path to Parquet file for the Triple Table
  var tripleTable = ""
  // path to the directory for all VP tables 
  var vpDir = ""
  // path to the directory for all ExtVP tables 
  var extVpDir = ""

  var datasetType = ""
  var supernodesFile = ""
  var igVpDir = ""
  var sgVpDir = ""
  
  def loadUserSettings(inFilePath:String, 
                   inFileName:String, dt: String) = {
//    this.ScaleUB = scale
    this.workingDir = inFilePath
    this.inputRDFSet = inFilePath + inFileName
    this.tripleTable = this.workingDir + "/base.parquet"
    datasetType = dt.toUpperCase()
    if (!"VP,CONN".contains(datasetType)) {
      throw new Exception(s"$datasetType should be one of VP, CONN.")
    }
    this.vpDir = this.workingDir + s"/$datasetType/"
//    this.vpDir = {
//      if(datasetType == "VPIG") this.workingDir + "VP/" + inFileName.split("\\.")(0) + "/"
//      else this.workingDir + "VP/" + inFileName.split("\\.")(0) + "/"
//    }

    this.igVpDir = this.workingDir + "/VP/"
    this.sgVpDir = this.workingDir + "/SGVP/"
    this.extVpDir = this.workingDir + "/CONN/"
  }
  
  /**
   * Create SparkContext.
   * The overview over settings: 
   * http://spark.apache.org/docs/latest/programming-guide.html
   */   
  /*def loadSparkContext(): SparkContext = {
    
    val conf = new SparkConf().setAppName("DataSetsCreator")
                              .set("spark.executor.memory", "6g")
                              .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
                              //.set("spark.sql.autoBroadcastJoinThreshold", "-1")
                              .set("spark.sql.parquet.filterPushdown", "true")
                              .set("spark.sql.inMemoryColumnarStorage.batchSize", "20000")
                              .set("spark.storage.blockManagerSlaveTimeoutMs", "3000000")
                              //.set("spark.sql.shuffle.partitions", "200")
                              .set("spark.storage.memoryFraction", "0.5")
    new SparkContext(conf)
  }
  
  /**
   * Create SQLContext.
   */
 
  def loadSqlContext(): SQLContext = {
    val context = new org.apache.spark.sql.SQLContext(sparkContext)
    import context.implicits._
    context    
  }*/

//  def loadSparkConf(): SparkConf = {
//    val sparkConf = new SparkConf().setAppName("DataCreator")
//      .set("spark.sql.inMemoryColumnarStorage.compressed", "true")
//      .set("spark.sql.autoBroadcastJoinThreshold", "-1")
//      .set("spark.sql.parquet.filterPushdown", "true")
//    //      .set("spark.storage.memoryFraction", "0.5")
//    //      .set("spark.sql.shuffle.partitions", "100")
//          .setMaster("local[*]")
//    sparkConf
//  }

  def loadSparkSession(): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("DataCreator")
//      .config(sparkConf)
      .getOrCreate()
  }
}

