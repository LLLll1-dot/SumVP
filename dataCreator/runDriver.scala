package dataCreator

import org.apache.log4j.{Level, Logger}

object runDriver {
  def main(args: Array[String]) {
    // parse Args
    Logger.getLogger("org").setLevel(Level.ERROR)
    Settings.loadUserSettings(args(0), args(1), "VP")

    DataSetGenerator.generateDataSet()

  }
}
