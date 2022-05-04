package GraphSummarization

import scala.collection.mutable.HashMap

class VertexClass(nId: Long, node: List[String], nAttr: Set[String], sConn: List[String], e: HashMap[Long, EdgeClass]) {
  var nodeID = nId
  var supernode = node
  var nodeAttr: Set[String] = nAttr
  var selfConn = sConn
  var edgeList: HashMap[Long, EdgeClass] = e

//  def displayVertex(vertex: VertexClass): Unit = {
//    print(s"${vertex.nodeID};${vertex.nodeAttr};${vertex.nodeSize};${vertex.selfConn};")
//    displayEdgeMap(vertex.edgeList)
//  }

}
