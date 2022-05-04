package sparqlTranslator

import org.apache.log4j.{Level, Logger}

object runDriver {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.OFF)

    val nodeIDMap = args(0)
    val spqPath = args(1)
    val statDir = args(2)
    val resultFile = args(3)

    Settings.settingFilePath(nodeIDMap, statDir, resultFile, 0)
    DataSetTranslator.runTranslator(spqPath.trim)

  }

  case class triple(sub:String, pre:String, obj:String)

}
