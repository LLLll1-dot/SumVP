package GraphActor

import java.io._
import scala.collection.mutable.{HashMap, Set}
import scala.actors.Actor
import GraphSummarization.Const._
import GraphSummarization.ReadFile._
import GraphSummarization.VertexAttribute3._
import GraphSummarization.VertexClass
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

class Dispatcher(consolePrinter: PrintWriter, actorNumber: Int, assembler: Assembler) extends Actor {
  val _sparkSession: SparkSession = ActorDriver._sparkSession
  var index: Int = 0 //当前节点index
  var steps: Int = 0 //寻找candidate次数
  var actors: Int = actorNumber
  var edgeCount: Int = 0
  //  val idArray = new ArrayBuffer[String]()
  //  val vertexArray: Array[String] = readAdjacentList("./sample.dat")
  //  val vertexArray: Array[String] = readEdge(ENRON_PATH)
  //  val Array(vertexArray, nodeIdArray) = readEdge(rdfDataset_Path)
  consolePrinter.println("Reading dataset file...")
  //  val nodeIdMap:HashMap[String, Int] = HashMap()
  //读取文件夹中的数据,第一个类型是数据集路径，第二个是路径类型，第三个是结点id
  val (vertexArray, nodeIdArray) = readFile("E:\\dataset\\LUBM\\merged\\10\\uni10.nt", _sparkSession)
  consolePrinter.println("Adding node id to edge...")
  println("Adding node id to edge...")
  //  val superNodes: mutable.HashMap[Long, Set[String]] = mutable.HashMap()
  //  for(i <- 0 until superNodes.size) {
  //    superNodes.put(i, Set(i))
  //  }
  val size: Int = vertexArray.length
  consolePrinter.println(s"initial node count is: $size")
  println(s"initial node count is: $size")
  //  val iterateStepLen = math.sqrt(size).toInt
  private val iterateStepLen = 1
  consolePrinter.println(s"the length of every step of iteration is: $iterateStepLen")
  println(s"the length of every step of iteration is: $iterateStepLen")
  //  val superNodes: HashMap[Int, Set[Int]] = createInitialSupernodes(nodeIdArray)
  //  mergeTypeNode()
  //    mergeOneDegreeNode(vertexArray, superNodes)
  var numberOfSupernodes: Int = size //数量都相等，用谁都一样
  val boundOfSuperndoes: Int = (size * 0.1).toInt
  val recordSupernodes: Set[Int] = Set(0, (size * 0.1).toInt, (size * 0.2).toInt, (size * 0.4).toInt,
    (size * 0.5).toInt, (size * 0.6).toInt, (size * 0.8).toInt)
  //      val boundOfSuperndoes: Int = 15000
  //  val recordSupernodes: Set[Int] = Set(0, 15000, 30000, 60000, 75000, 90000, 120000)
  var pairsSet: Set[Long] = Set()
  var neighborUpdateSet: Set[Int] = Set()
  var time = System.currentTimeMillis()

  def act() = {
    while (true) {
      receive {
        case GIVE_ME_A_NEW_ONE(threadId: Int) => {
          //获得当前系统时间
          if (numberOfSupernodes > boundOfSuperndoes) {
            //获得一个随机数
            index = scala.util.Random.nextInt(size)
            steps = steps + iterateStepLen
            //获得一个随机的没有被合并的节点
            while (vertexArray(index) != null) {
              //              steps = steps + iterateStepLen
              index = (index + 1) % size
              //              index = steps % size
            }
            val vertex = vertexArray(index)
            val vId = vertex.nodeID
            //            if (vertex.edgeList.nonEmpty && !pairsSet.contains(vId) &&
            //              (getAllNeighborsId(vertex.edgeList.values) intersect pairsSet).isEmpty) {
            if (vertex.edgeList.nonEmpty && !pairsSet.contains(vId)){
              //              pairsSet += vId
              val twoHopNeighbor: VertexClass = findBestCandidate(vertex, vertexArray, 0.1 * (steps / size))
              if (twoHopNeighbor != null) {
                val twoHopNeighborId = twoHopNeighbor.nodeID
                //可能找不到二跳邻居
                if (!pairsSet.contains(twoHopNeighbor.nodeID) && (getAllNeighborsId(twoHopNeighbor) intersect pairsSet).isEmpty) {
                  consolePrinter.println(s"thread $threadId: index is:" + index + ", node id is:" + vId)
                  pairsSet += (vId, twoHopNeighborId)
                  println(s"findBestCandidate time: ${System.currentTimeMillis() - time} ms")
                  consolePrinter.println(s"thread $threadId: add candidates: " + (vId, twoHopNeighborId) + ", steps: " + steps)
                  sender ! (vertex, twoHopNeighbor, steps / size)
                }
                else {
                  sender ! NOT_FIND
                }
              }
              else {
                sender ! NOT_FIND
              }
            }
            else {
              sender ! NOT_FIND
            }
            steps = steps + iterateStepLen
            index = steps % size
          }
          else {
            sender ! SUMMARIZATION_ACCOMPLISHED
            consolePrinter.println(s"thread $threadId: graph summarization finished, a node exits...")
          }
        }

        case MERGE_ACCOMPLISHED(viId: Long, vjId: Long, threadId: Int) => {
          consolePrinter.println(s"thread $threadId: merge accomplished---release candidate: " + viId + "\t" + vjId)
          //          consolePrinter.println("Accomplished---before updated pairsSet" + pairsSet)
          pairsSet -= (viId, vjId)
          numberOfSupernodes = numberOfSupernodes - 1
          if (numberOfSupernodes % 1000 == 0) {
            consolePrinter.println(s"Current number Of supernodes: $numberOfSupernodes")
          }
          //          if (recordSupernodes.contains(numberOfSupernodes)) {
          if (recordSupernodes.max >= numberOfSupernodes) {
            recordSupernodes -= recordSupernodes.max
            assembler ! numberOfSupernodes
            println("numberOfSupernodes:" + numberOfSupernodes)
            val snFile = ENRON_GRAPH_SUMMARIZATION_PATH + actorNumber + "-supernodes-" + numberOfSupernodes + ".txt"
            val seFile = ENRON_GRAPH_SUMMARIZATION_PATH + actorNumber + "-superedges-" + numberOfSupernodes + ".txt"
            val typeSet = saveSuperedgesAsFile(vertexArray, nodeIdArray, seFile)
            saveSupernodesAsFile(numberOfSupernodes, vertexArray, nodeIdArray, typeSet, snFile)

            /*val path = s"./results/${recordSupernodes.size}-splitFile/"
            saveSupernodesForQuery(superNodes, nodeIdArray, path)
            saveSuperedgesForQuery(vertexArray, nodeIdArray, path)
            saveSupernodesIdForQuery(superNodes, path)
            saveSuperedgesIdForQuery(vertexArray, path)*/
          }
          time = System.currentTimeMillis()
          //          consolePrinter.println("Accomplished---updated pairsSet" + pairsSet)
        }

        case MERGE_NOT_SATISFIED(viId: Long, vjId: Long, threadId: Int) => {
          consolePrinter.println(s"thread $threadId: merge error is not Satisfied---release candidate: " + viId + "\t" + vjId)
          //          consolePrinter.println("Not Satisfied---before updated pairsSet" + pairsSet)
          pairsSet -= (viId, vjId)
          //          consolePrinter.println("Not Satisfied---updated pairsSet" + pairsSet)
        }

        //        case I_AM_DONE =>{
        //          actors = actors - 1
        //          consolePrinter.println("current number of actors is " + actors)
        //          if(actors == 0){
        //            consolePrinter.println("stop dispatcher!!!")
        //
        //            //保存superNodes和vertexArray
        //            saveSupernodesAsFile(superNodes)
        //            saveSuperedgesAsFile(vertexArray)
        //
        //            assembler ! STOP
        //            exit()
        //          }
        //        }
        case SUMMARIZATION_EXIT(threadId: Int) => {
          actors = actors - 1
          consolePrinter.println(s"thread $threadId has stopped!")
          consolePrinter.println("current number of actors is " + actors)
          if (actors == 0) {
            consolePrinter.println("All thread have stopped!")
            consolePrinter.println("stop dispatcher!!!")
            //保存superNodes和vertexArray

            val seFile = ENRON_GRAPH_SUMMARIZATION_PATH + actorNumber + "-superedges.txt"
            val snFile = ENRON_GRAPH_SUMMARIZATION_PATH + actorNumber + "-supernodes.txt"
            val typeSet = saveSuperedgesAsFile(vertexArray, nodeIdArray, seFile)
            saveSupernodesAsFile(numberOfSupernodes, vertexArray, nodeIdArray, typeSet, snFile)
            /*val path = "./results/splitFile/"
            saveSupernodesForQuery(superNodes, nodeIdArray, path)
            saveSuperedgesForQuery(vertexArray, nodeIdArray, path)

            saveSupernodesIdForQuery(superNodes, path)
            saveSuperedgesIdForQuery(vertexArray, path)*/
            assembler ! STOP
            exit()
          }
        }
        case SAVE_GRAPH_TO_FILE => {
          consolePrinter.println("Thread 0 start to save initial graph to file. ")
          //          val initialGraphArray: Array[VertexClass] = vertexArray
          //          saveInitialInfoToFile(initialGraphArray, nodeIdArray, ENRON_GRAPH_SUMMARIZATION_PATH)
          consolePrinter.println("Thread 0 has finished saving the initial Graph and start to summarize. ")
        }
      }
    }
  }
}
