package queryExecutor

import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast

object runDriver {
  var loopTimes = 1
  def main(args:Array[String]) = {
    //    val runStartTime = System.currentTimeMillis()
    Logger.getLogger("org").setLevel(Level.OFF)

    val hdfsDir = args(0)
    val hashtablePath = args(1)
    val queryFile = args(2)

    val resultFile = "resultTime.txt"


    Settings.loadUserSettings(hdfsDir, hashtablePath, queryFile, resultFile)

    //    Settings.displayPath()
    QueryExecutor.parseQueryFile()
    val edgeMap: Broadcast[Map[String, Array[(String, String)]]] = QueryExecutor.readSupernodeFile()
    //    println()
    loopTimes = args(3).toInt
    for(i <- 1 to loopTimes) {
      //    for(i <- 0 to 2) {
      QueryExecutor.executeQuery(edgeMap)
    }

    Settings.saveResultAsFile()
    //测试输出
    //    println("The total run time is: " + (System.currentTimeMillis() - runStartTime))
  }
}