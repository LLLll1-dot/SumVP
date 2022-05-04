package GraphActor

import GraphSummarization.ReadFile._
import GraphSummarization.VertexAttribute3._
import GraphSummarization.VertexClass
import org.apache.spark.sql.SparkSession
import java.io.{BufferedWriter, File, FileWriter}
import java.util.Calendar
import scala.collection.mutable.Set
import org.apache.log4j.{Level, Logger}


object Main {
  Logger.getLogger("org").setLevel(Level.ERROR)
  val _sparkSession: SparkSession = loadSparkSession()
  var steps: Int = 0
  var edgeCount: Int = 0
//  consolePrinter.
    println("Reading dataset file...")

  val (vertexArray, nodeIdArray) = readFile(driver.inputPath, _sparkSession)
//  consolePrinter.
  println("Adding node id to edge...")
  val size: Long = vertexArray.length
//  consolePrinter.
    var numberOfSupernodes: Long = size
    println(s"initial node count is: $size")
  //If the running time is too slow, it can be adjusted appropriately.
  private val iterateStepLen = 1
  //  consolePrinter.
  println(s"the length of every step of iteration is: $iterateStepLen")
  val boundOfSuperndoes: Int = (size * 0.1).toInt
  val recordSupernodes: Set[Int] = Set(0, (size * 0.1).toInt, (size * 0.2).toInt, (size * 0.4).toInt,
    (size * 0.5).toInt, (size * 0.6).toInt, (size * 0.8).toInt)
  //      val boundOfSuperndoes: Int = 15000
  //  val recordSupernodes: Set[Int] = Set(0, 15000, 30000, 60000, 75000, 90000, 120000)
//  var pairsSet: Set[Long] = Set()
  var neighborUpdateSet: Set[Int] = Set()
  var totalError: Double = 0
  var time = System.currentTimeMillis()
  var Error: Long = 0
  val logFile = new File("logFile.txt")
  val log = new BufferedWriter(new FileWriter(logFile))

  val startTime: Long = System.currentTimeMillis()

  def run(): Unit = {
    while(numberOfSupernodes > boundOfSuperndoes){
      val (vi, vj, iterations) = GIVE_ME_A_NEW_ONE()
      if(vi != null) {
        mergeNode(vi, vj, iterations)
      }
      steps = steps + iterateStepLen
    }
    SUMMARIZATION_EXIT()
    println("the total error is " + totalError)
    val endTime: Long = Calendar.getInstance.getTimeInMillis
    println("runtime: " + (endTime - startTime)/1000.0 + "s")

    log.close()
  }

  def mergeNode(vi :VertexClass, vj: VertexClass, iterations: Int) : Unit = {

    val time = System.currentTimeMillis()
    val errorIncrease = calculateErrorIncrease(vi, vj, vertexArray)
//    println(s"processor errorIncrease: $errorIncrease, iterations: $iterations, time: ${System.currentTimeMillis() - time}")
    val viId = getNodeID(vi)
    val vjId = getNodeID(vj)

    //adjust error threshold
    if(steps >= numberOfSupernodes) {
      Error += 2 * (size / numberOfSupernodes)
      steps = steps - numberOfSupernodes.toInt
      log.write(s"ErrorThreshold: $Error\n")
    }
    if (errorIncrease <= Error) {
      //            consolePrinter.println("candidate is satisfied: " + vi + "----->" + vj)
      if (viId < vjId) {
        updateMerge(vi, vj, vertexArray)
        mergeVertex(vi, vj, vertexArray)
//        updateSuperNodes(viId, vjId, superNodes)
      }
      else {
        updateMerge(vj, vi, vertexArray)
        mergeVertex(vj, vi, vertexArray)
//        updateSuperNodes(vjId, viId, superNodes)
      }
      //            consolePrinter.println("update accomplished:" + dispatcher.superNodes)
      MERGE_ACCOMPLISHED(viId, vjId)
      updateError(errorIncrease)
    }
    else {
      //            consolePrinter.println("merge error is not satisfied")
      MERGE_NOT_SATISFIED(viId, vjId)
    }
  }

  def updateError(error: Double): Unit = {
//    println("the error increase of this merge is " + error)
    totalError = totalError + error
  }
  def displaySupernodeNum(supernodeNum: Long): Unit = {
    println("supernodes record: " + supernodeNum)
    println("the total error is " + totalError)
    val endTime: Long = Calendar.getInstance.getTimeInMillis
    println("runtime: " + (endTime - startTime)/1000.0 + "s");
  }

  def GIVE_ME_A_NEW_ONE(): (VertexClass, VertexClass, Int) = {
    //get random num
    var index = scala.util.Random.nextInt(size.toInt)
    steps = steps + iterateStepLen
    while (vertexArray(index) == null) {
      index = (index + 1) % size.toInt
    }
    val vertex: VertexClass = vertexArray(index)
    val vId = vertex.nodeID
    if (vertex.edgeList.nonEmpty) {
      //              pairsSet += vId
      val twoHopNeighbor: VertexClass = findBestCandidate(vertex, vertexArray, 0.1 * (steps / size))
      if (twoHopNeighbor != null) {
//        val twoHopNeighborId = twoHopNeighbor.nodeID
          //            consolePrinter.
//          pairsSet += (vId, twoHopNeighborId)
          //            consolePrinter.
//          println(s"add candidates: " + (vId, twoHopNeighborId) + ", steps: " + steps)
          return (vertex, twoHopNeighbor, steps)
      }
    }
    (null, null, 0)
  }

  def MERGE_ACCOMPLISHED(viId: Long, vjId: Long): Unit = {
    log.write(s"merge accomplished---release candidate: " + viId + "\t" + vjId + "\n")
    println(s"merge nodes ($viId, $vjId)")
    //          consolePrinter.println("Accomplished---before updated pairsSet" + pairsSet)
//    pairsSet -= (viId, vjId)
    numberOfSupernodes = numberOfSupernodes - 1
    if (numberOfSupernodes % 1000 == 0) {
      log.write(s"Current number Of supernodes: $numberOfSupernodes\n")
    }
    //          if (recordSupernodes.contains(numberOfSupernodes)) {
    if (recordSupernodes.max >= numberOfSupernodes) {
      recordSupernodes -= recordSupernodes.max
      displaySupernodeNum(numberOfSupernodes)
      log.write("numberOfSupernodes:" + numberOfSupernodes + "\n")
      val snFile = driver.outputPath + "supernodes-" + numberOfSupernodes + ".txt"
      val seFile = driver.outputPath +  "superedges-" + numberOfSupernodes + ".txt"
      saveHashtable(vertexArray, nodeIdArray, seFile)
//      val typeSet = saveSuperedgesAsFile(vertexArray, nodeIdArray, seFile)
//      saveSupernodesAsFile(numberOfSupernodes, vertexArray, nodeIdArray, typeSet, snFile)
    }
    time = System.currentTimeMillis()
  }

  def MERGE_NOT_SATISFIED(viId: Long, vjId: Long): Unit = {
    log.write(s"merge error is not Satisfied---release candidate: " + viId + "\t" + vjId+ "\n")
    //          consolePrinter.println("Not Satisfied---before updated pairsSet" + pairsSet)
//    pairsSet -= (viId, vjId)
    //          consolePrinter.println("Not Satisfied---updated pairsSet" + pairsSet)
  }

  def SUMMARIZATION_EXIT(): Unit = {
    println("All thread have stopped!")
    println("stop dispatcher!!!")
    val seFile = driver.outputPath + "superedges.txt"
    val snFile = driver.outputPath + "supernodes.txt"
    saveHashtable(vertexArray, nodeIdArray, seFile)
//    val typeSet = saveSuperedgesAsFile(vertexArray, nodeIdArray, seFile)
//    saveSupernodesAsFile(numberOfSupernodes, vertexArray, nodeIdArray, typeSet, snFile)
  }

  def loadSparkSession(): SparkSession = {
    //    localRun = true
    val ss = SparkSession.builder()
      .appName("Graph Summarization")
      .config("SPARK_MASTER_WEBUI_PORT", "8090")
      .config("spark.driver.maxResultSize", "20g")
      .config("spark.driver.memory", "30g")
      .config("spark.executor.memory", "30g")
      //      .config("spark.some.config.option","some-value")
      //      .config("spark.eventLog.enabled","true")
      .getOrCreate()
    ss
  }

}
