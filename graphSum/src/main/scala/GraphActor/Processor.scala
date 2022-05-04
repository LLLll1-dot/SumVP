package GraphActor

import scala.actors.Actor
import GraphSummarization.VertexAttribute3._
import GraphSummarization.VertexClass

class Processor(dispatcher: Dispatcher, assembler: Assembler, threadId: Int) extends Actor {

  val initialError = 5
  //互斥集合，解决多个work节点同时对一个图结点进行更新处理
  var excluSet: Set[Long] = Set()

  def act(): Unit = {
    if (threadId == 0) {
      dispatcher ! SAVE_GRAPH_TO_FILE
    }
    dispatcher ! GIVE_ME_A_NEW_ONE(threadId)
    while (true) {
      receive {
        //        case NO_MORE_NUMBERS =>{
        //          consolePrinter.println("I am done")
        //          sender ! I_AM_DONE
        //          exit()
        //        }

        case SUMMARIZATION_ACCOMPLISHED =>
          //          consolePrinter.println("summarization finished, exit...")
          sender ! SUMMARIZATION_EXIT(threadId)
          exit()

        case NOT_FIND =>
          sender ! GIVE_ME_A_NEW_ONE(threadId)

        case (vi: VertexClass, vj: VertexClass, iterations: Int) => {
          val time = System.currentTimeMillis()
          val errorIncrease = calculateErrorIncrease(vi, vj, dispatcher.vertexArray)
          println(s"processor errorIncrease: $errorIncrease, iterations: $iterations, time: ${System.currentTimeMillis() - time}")
          val viId = getNodeID(vi)
          val vjId = getNodeID(vj)

          //误差增量阈值
          if (errorIncrease <= 2 * iterations + initialError) {
            var timeA: Long = System.currentTimeMillis()
            var timeB = 0L
            var timeC = 0L
            //            consolePrinter.println("candidate is satisfied: " + vi + "----->" + vj)
            if (viId < vjId) {
              updateMerge(vi, vj, dispatcher.vertexArray)
              timeB = System.currentTimeMillis()
              mergeVertex(vi, vj, dispatcher.vertexArray)
              timeC = System.currentTimeMillis()
//              updateSuperNodes(viId, vjId, dispatcher.superNodes)
            }
            else {
              updateMerge(vj, vi, dispatcher.vertexArray)
              timeB = System.currentTimeMillis()
              mergeVertex(vj, vi, dispatcher.vertexArray)
              timeC = System.currentTimeMillis()
//              updateSuperNodes(vjId, viId, dispatcher.superNodes)
            }
            println(s"update: ${timeB - timeA}, merge: ${timeC - timeB}")
            //            consolePrinter.println("update accomplished:" + dispatcher.superNodes)
            dispatcher ! MERGE_ACCOMPLISHED(viId, vjId, threadId)
            assembler ! errorIncrease
            dispatcher ! GIVE_ME_A_NEW_ONE(threadId)
          }
          else {
            //            consolePrinter.println("merge error is not satisfied")
            sender ! MERGE_NOT_SATISFIED(viId, vjId, threadId)
            sender ! GIVE_ME_A_NEW_ONE(threadId)
          }
        }
      }
    }
  }
}
