/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */

package dataCreator

import org.apache.spark.sql.{SQLContext, SparkSession}

object Settings {
  val sparkSession = loadSparkSession()
  
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

    this.igVpDir = this.workingDir + "/VP/"
    this.sgVpDir = this.workingDir + "/SGVP/"
    this.extVpDir = this.workingDir + "/CONN/"
  }
  
  def loadSparkSession(): SparkSession = {
    SparkSession.builder()
      .appName("DataCreator")
//      .config(sparkConf)
      .getOrCreate()
  }
}

