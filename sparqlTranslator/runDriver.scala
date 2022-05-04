package sparqlTranslator

import org.apache.log4j.{Level, Logger}

object runDriver {

  def main(args: Array[String]): Unit = {
    /**输入:
     * 0: supernodesFile(超点文件);
     * 1: statDir(统计文件目录);
     * 2: spqPath(sparql文件路径);
     * 3: resultFile(结果文件);
     * 4: flag;
     * */

    Logger.getLogger("org").setLevel(Level.OFF)

//    val flag: Int = {
//      if (args.length < 5) 1
//      else args(4).toInt
//    }
//        val dataType: Int = {
//          if (args.length < 5 || args(4).equals("0")) 0
//          else 1
//        }

//    Settings.loadFilePath(args(0), args(1), args(3), flag, dataType)
//    DataSetTranslator.runTranslator(args(2).trim)

//    val dir = args(0)
//    val supernodesFile = dir +"/supernodes.txt"
//    val nodeIDMap = args(1)
//    val spqPath = args(2)
//    val statDir = args(3)
//    val resultFile = "query.txt"
//    val dataType = 0
//
//
//    val dir = "D:\\ysData\\dataset\\lubm\\lubm1\\lubm1\\id\\2642"
//    val supernodesFile = dir+"\\supernodes.txt"
    val nodeIDMap = args(0)
    val spqPath = args(1)
    val statDir = args(2)
    val resultFile = args(3)
    val dataType = 0

    Settings.settingFilePath(nodeIDMap, statDir, resultFile, dataType)
    DataSetTranslator.runTranslator(spqPath.trim)

  }

  case class triple(sub:String, pre:String, obj:String)

}
