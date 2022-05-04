package GraphSummarization
import scala.collection.mutable.HashMap

class EdgeClass(eId: Long, eRel: List[String], conn: List[String], eType: Int) {
  var edgeID: Long = eId
  var edgeRelation = eRel
//  var edgeSize = s
  var edgeConn = conn
  var edgeType = eType


//  def displayEdgeMap(edgeMap: HashMap[Long, EdgeClass]): Unit = {
//    edgeMap.foreach(edge => {
//      print(edge._1+": ")
//      displayEdge(edge._2)
//    })
//  }
//  def displayEdgeList(edgeArray: Array[EdgeClass]): Unit = {
//    edgeArray.foreach(edge => {
//      displayEdge(edge)
//    })
//  }
//  def displayEdge(edge: EdgeClass): Unit = {
//    println(s"${edge.edgeID},${edge.edgeRelation},${edge.edgeSize},${edge.edgeConn},${edge.edgeType}")
//  }

}
