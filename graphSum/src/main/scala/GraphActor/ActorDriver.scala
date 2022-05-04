package GraphActor

import java.io.{File, PrintWriter}
import java.text.SimpleDateFormat
import java.util.Calendar
import org.apache.spark.sql.SparkSession

import GraphSummarization.Const._

case class STOP()

case class NO_MORE_NUMBERS()

case class I_AM_DONE()

case class GIVE_ME_A_NEW_ONE(threadId: Int)

case class MERGE_ACCOMPLISHED(viId: Long, vjId: Long, threadId: Int)

case class MERGE_NOT_SATISFIED(viId: Long, vjId: Long, threadId: Int)

case class NOT_FIND()

case class SUMMARIZATION_ACCOMPLISHED()

case class SUMMARIZATION_EXIT(threadId: Int)

case class SAVE_GRAPH_TO_FILE()

object ActorDriver {

  val _sparkSession: SparkSession = loadSparkSession()

  def main(args: Array[String]): Unit = {
    val startTime: Long = Calendar.getInstance.getTimeInMillis
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    println("start time is " + dateFormat.format(startTime))
    //    val actorNumber: Int = Runtime.getRuntime.availableProcessors()
    val actorNumber: Int = 1
    val consolePrinter = new PrintWriter(new File(ENRON_GRAPH_SUMMARIZATION_PATH + actorNumber + "-console.txt"))
    //    val consolePrinter = new PrintWriter(new File(GOWALLA_GRAPH_SUMMARIZATION_PATH + actorNumber + "-console.txt"))

    val assembler = new Assembler(startTime, consolePrinter)
    assembler.start()

    val dispatcher = new Dispatcher(consolePrinter, actorNumber, assembler)
    dispatcher.start()

    for (i <- 0 until actorNumber) {
      println(s"actorNumber i: $i")
      new Processor(dispatcher, assembler, i).start()
    }

    //    val endTime: Long = Calendar.getInstance.getTimeInMillis
    //    println("程序运行时长为：" + (endTime - startTime)/1000.0 + "s")
  }

  def loadSparkSession(): SparkSession = {
    //    localRun = true
    val ss = SparkSession.builder()
      .master("local[*]")
      .appName("QueryExecutor")
      .config("SPARK_MASTER_WEBUI_PORT", "8090")
      .config("spark.driver.maxResultSize", "60g")
      //      .config("spark.some.config.option","some-value")
      //      .config("spark.eventLog.enabled","true")
      .getOrCreate()
    ss
  }
}

