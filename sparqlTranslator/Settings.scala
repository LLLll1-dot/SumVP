package sparqlTranslator

import org.apache.spark.sql.SparkSession

object Settings {
  val sparkSession: SparkSession = loadSparkSession()
  //supernodes file
  var nodeIdMap = ""
  //sparql file
  var spqFile = ""
  //summary graph statistic file
  var s_VPFile = ""
  var s_SSFile = ""
  var s_OSFile = ""
  var s_OOFile = ""
  var sqlFile = ""  //SPARQL查询语言的路径
  var oriFlag = 1   //初始输入的flag
  var flag = 1      //判断是否基于摘要优化
  var dataType = 0  //是否是watdiv数据集

  def settingFilePath (nodeIdMap: String, statDir: String, sqlFile: String, dataType: Int): Unit ={
    this.nodeIdMap = nodeIdMap
    this.s_VPFile = statDir + "/stat_vp.txt"
    this.s_SSFile = statDir + "/stat_ss.txt"
    this.s_OSFile = statDir + "/stat_os.txt"
    this.s_OOFile = statDir + "/stat_oo.txt"
    this.sqlFile = sqlFile
    this.dataType = dataType
    /*this.sqlFile = {
      if (flag == 1) spqFile.substring(0, spqFile.lastIndexOf("\\")+1)+"sg"+spqFile.substring(spqFile.lastIndexOf("\\")+1)
      else spqFile.substring(0, spqFile.lastIndexOf("\\")+1)+"ig"+spqFile.substring(spqFile.lastIndexOf("\\")+1)
    }*/
  }

  def setFlag(f: Int): Unit ={
    this.flag = f
  }

  def setSpqFile(file: String): Unit ={
    this.spqFile = file
  }


  def loadSparkSession (): SparkSession = {
    val ss = new SparkSession.Builder()
      .enableHiveSupport()
      .appName("Translator")
      .config("spark.some.config.option","some-value")
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
//      .replaceAll(";", "S__S")   //符号;
  }


}
