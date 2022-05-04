package GraphSummarization

import scala.collection.mutable.Set
import scala.math._

/*
* Vertex Structure:
* nodeID: ownerID, attri, size, selfConn
*         edgeList: relation, size, conn
*
* 节点集为Array[String]
* 每一个节点结构为："nodeID;ownerID;attribute;size;selfConn;edgeList"
* edgeList中每个边用逗号隔开，同一边中的各属性用空格隔开: "neighborID relation size conn"
*/

object Vertex {

  def getVertexInfo(ID: Int, vertexArray: Array[String]): String = {
    //从所有节点的数组vertexArray中获得边id与ID相等的边信息，并取第一条（因为可能会获得多条同样的信息，随机取一条即可）
    val vertex: String = vertexArray.filter(_.split(";")(0).toInt == ID)(0)

    vertex
  }

  def getNodeID(v: String): Int = {
    val nodeID = v.split(";")(0).toInt

    nodeID
  }

  def getOwnerID(v: String): Int = {
    val ownerID = v.split(";")(1).toInt

    ownerID
  }

  def getAttribute(v: String): Int = {
    val attribute = v.split(";")(2).toInt

    attribute
  }

  def getSize(v: String): Int = {
    val size = v.split(";")(3).toInt

    size
  }

  def getSelfConn(v: String): Int = {
    val selfConn = v.split(";")(4).toInt

    selfConn
  }

  def getEdgeList(v: String): Array[String] = {
    val edgeList = v.split(";")(5).split(",")

    edgeList
  }

  def getNeighborID(e: String): Int = {
    val neighborID = e.split(" ")(0).toInt

    neighborID
  }

  def getNeighborRelation(e: String): Int = {
    val neighborRelation = e.split(" ")(1).toInt

    neighborRelation
  }

  def getNeighborSize(e: String): Int = {
    val neighborSize = e.split(" ")(2).toInt

    neighborSize
  }

  def getNeighborConn(e: String): Int = {
    val neighborConn = e.split(" ")(3).toInt

    neighborConn
  }

  def getRandomNeighbor(v: String): Int = {
    val edgeList: Array[String] = getEdgeList(v)

    val size: Int = edgeList.size

    if(size > 0){
      val rn: Int = scala.util.Random.nextInt(size) //生成0 ~ size-1 之间的随机数

      val randomNeighborID: Int = getNeighborID(edgeList(rn)) //获取随机邻居ID

      randomNeighborID
    }
    else{
      -1
    }
  }

  def getAllNeighbors(v: String): Set[Int] = {
    val allNeighborsSet: Set[Int] = Set()

    val edgeList: Array[String] = getEdgeList(v)
    for (e <- edgeList) {
      allNeighborsSet += getNeighborID(e)
    }
    allNeighborsSet
  }

  def findCandidate(v: String, vertexArray: Array[String]): String = { //随机选择一个二跳候选节点
    val neighbor: Int = getRandomNeighbor(v)

    if(neighbor != -1){
      //过滤掉自身，防止寻找二跳邻居又找到自身
      val twoHopNeighbor: Array[Int] = getAllNeighbors(getVertexInfo(neighbor, vertexArray)).filter(_ != getNodeID(v)).toArray

      if(twoHopNeighbor.size > 0){
        val candidateID: Int = twoHopNeighbor.apply(scala.util.Random.nextInt(twoHopNeighbor.size)) //随机选择一个候选节点nodeID

        val candidate: String = getVertexInfo(candidateID,vertexArray)

        candidate
      }
      else{
        ""
      }
    }
    else{
      ""
    }
  }

  //找到所有的两跳邻居
  def findCandidates(v: String, vertexArray: Array[String]): Array[String] = {
    val neighbor: Int = getRandomNeighbor(v)

    if(neighbor != -1){
      //防止寻找二跳邻居又找到自身
      val twoHopNeighborsID: Set[Int] = getAllNeighbors(getVertexInfo(neighbor, vertexArray)).filter(_ != getNodeID(v))

      //选择所有二跳邻居
      val twoHopNeighbors: Array[String] = twoHopNeighborsID.map(x => getVertexInfo(x,vertexArray)).toArray

      twoHopNeighbors
    }
    else{
      Array[String]()
    }

  }

  def calculateErrorIncrease(vi: String, vj: String, vertexArray: Array[String]): Int = {
    val neighborsOfVi: Set[Int] = getAllNeighbors(vi)
    val neighborsOfVj: Set[Int] = getAllNeighbors(vj)

    val commonNeighbors: Set[Int] = neighborsOfVi intersect neighborsOfVj
    //注意vi、vj为直接邻居的情况，特有邻居集中需要除去彼此
    val uniqueNeighborsOfVi: Set[Int] = (neighborsOfVi diff commonNeighbors).filter(_ != getNodeID(vj))
    val uniqueNeighborsOfVj: Set[Int] = (neighborsOfVj diff commonNeighbors).filter(_ != getNodeID(vi))

    var commonError: Int = 0
    for (cn <- commonNeighbors) {
      commonError += calculateCommonError(vi, vj, getVertexInfo(cn, vertexArray)) //
    }

    var uniqueErrorOfVi: Int = 0
    for (uni <- uniqueNeighborsOfVi) {
      uniqueErrorOfVi += calculateUniqueError(vi, vj, getVertexInfo(uni, vertexArray)) //
    }

    var uniqueErrorOfVj: Int = 0
    for (unj <- uniqueNeighborsOfVj) {
      uniqueErrorOfVj += calculateUniqueError(vj, vi, getVertexInfo(unj, vertexArray)) //
    }

    val selfError: Int = calculateSelfError(vi, vj)

    val totalError: Int = commonError + uniqueErrorOfVi + uniqueErrorOfVj + selfError

    totalError
  }

  def calculateCommonError(vi: String, vj: String, vp: String): Int = { //commonError = em,p - ei,p - ej,p
    val edgeOfVip = getEdgeList(vi).filter(getNeighborID(_) == getNodeID(vp))(0)
    val connOfVip = getNeighborConn(edgeOfVip)

    val edgeOfVjp = getEdgeList(vj).filter(getNeighborID(_) == getNodeID(vp))(0)
    val connOfVjp = getNeighborConn(edgeOfVjp)

    val emp: Int = min((getSize(vi) + getSize(vj)) * getSize(vp) - (connOfVip + connOfVjp), connOfVip + connOfVjp)
    val eip: Int = min(getSize(vi) * getSize(vp) - connOfVip, connOfVip)
    val ejp: Int = min(getSize(vj) * getSize(vp) - connOfVjp, connOfVjp)

    val commonError: Int = emp - eip - ejp

    commonError
  }

  def calculateUniqueError(vi: String, vj: String, vq: String): Int = { //uniqueError = em,q - ei,q
    val edgeOfViq = getEdgeList(vi).filter(getNeighborID(_) == getNodeID(vq))(0)
    val connOfViq = getNeighborConn(edgeOfViq)

    val emq: Int = min((getSize(vi) + getSize(vj)) * getSize(vq) - connOfViq, connOfViq)
    val eiq: Int = min(getSize(vi) * getSize(vq) - connOfViq, connOfViq)

    val uniqueError: Int = emq - eiq

    uniqueError
  }

  def getConnOfVij(vi: String, vj: String): Int = {
    val edgeOfVij: Array[String] = getEdgeList(vi).filter(getNeighborID(_) == getNodeID(vj))
    var connOfVij: Int = 0
    if(edgeOfVij.size > 0){
      connOfVij = getNeighborConn(edgeOfVij(0))
    }
    connOfVij
  }

  def calculateSelfError(vi: String, vj: String): Int = { //selfError = em,m - ei,i - ej,j - ei,j
    val size: Int = getSize(vi) + getSize(vi)

    val connOfVij: Int = getConnOfVij(vi, vj)

    val selfConnOfVm: Int = getSelfConn(vi) + getSelfConn(vj) + connOfVij

    val f = (size: Int, selfConn: Int) => min(size * (size - 1) / 2 - selfConn, selfConn) //定义函数，计算自连接误差

    val eii: Int = f(getSize(vi), getSelfConn(vi))
    val ejj: Int = f(getSize(vj), getSelfConn(vj))
    val emm: Int = f(size, selfConnOfVm)
    val eij: Int = min(getSize(vi) * getSize(vj) - connOfVij, connOfVij) //算法原始定义误差ei,j计算方式
    //    val eij: Int = getSize(vi) * getSize(vj) - connOfVij//假设节点合并，引入误差计算方式

    val selfError: Int = emm - eii - ejj - eij

    selfError
  }

  def mergeVertex(vi: String, vj: String, vertexArray: Array[String]): Unit = { //假设vi.nodeID < vj.nodeID
    val viArray: Array[String] = vi.split(";")
    val vjArray: Array[String] = vj.split(";")

    var tempvi = s"${viArray(0)};${viArray(1)};0;${viArray(3).toInt + vjArray(3).toInt};"//修改size

    val connOfVij: Int = getConnOfVij(vi, vj)

    tempvi = tempvi + (viArray(4).toInt + vjArray(4).toInt + connOfVij) + ";" //修改selfConn

    //更新vi邻居集
    val neighborsOfVi: Set[Int] = getAllNeighbors(vi)
    val neighborsOfVj: Set[Int] = getAllNeighbors(vj)

    val commonNeighbors: Set[Int] = neighborsOfVi intersect neighborsOfVj
    //注意vi、vj为直接邻居的情况，特有邻居集中需要除去彼此
    val uniqueNeighborsOfVi: Set[Int] = (neighborsOfVi diff commonNeighbors).filter(_ != getNodeID(vj))
    val uniqueNeighborsOfVj: Set[Int] = (neighborsOfVj diff commonNeighbors).filter(_ != getNodeID(vi))

    val edgeListOfVi = getEdgeList(vi)
    val edgeListOfVj = getEdgeList(vj)
    for (cn <- commonNeighbors) {
      val ni: Array[String] = edgeListOfVi.filter(getNeighborID(_) == cn)(0).split(" ")
      val nj: Array[String] = edgeListOfVj.filter(getNeighborID(_) == cn)(0).split(" ")

      tempvi = tempvi + s"${cn} 0 ${ni(2)} ${ni(3).toInt + nj(3).toInt}," //修改common neighbor edge
    }

    for (uni <- uniqueNeighborsOfVi) {
      val ni: String = edgeListOfVi.filter(getNeighborID(_) == uni)(0)

      tempvi = tempvi + ni + ","
    }

    for (unj <- uniqueNeighborsOfVj) {
      val nj: String = edgeListOfVj.filter(getNeighborID(_) == unj)(0)

      tempvi = tempvi + nj + ","
    }

    vertexArray(vertexArray.indexWhere(getNodeID(_) == getNodeID(vi))) = tempvi //更新
    vertexArray(vertexArray.indexWhere(getNodeID(_) == getNodeID(vj))) = s"${vjArray(0)};${viArray(0)};${vjArray(2)};${vjArray(3)};${vjArray(4)};${vjArray(5)}"
  }

  def updateMerge(vi: String, vj: String, vertexArray: Array[String]): Unit = {//假设vi.nodeID < vj.nodeID
    val neighborsOfVi: Set[Int] = getAllNeighbors(vi)
    val neighborsOfVj: Set[Int] = getAllNeighbors(vj)

    val commonNeighbors: Set[Int] = neighborsOfVi intersect neighborsOfVj
    //注意vi、vj为直接邻居的情况，特有邻居集中需要除去彼此
    val uniqueNeighborsOfVi: Set[Int] = (neighborsOfVi diff commonNeighbors).filter(_ != getNodeID(vj))
    val uniqueNeighborsOfVj: Set[Int] = (neighborsOfVj diff commonNeighbors).filter(_ != getNodeID(vi))
//    println("i------")
//    uniqueNeighborsOfVi.foreach(println)
//    println("j------")
//    uniqueNeighborsOfVj.foreach(println)


    for (cn <- commonNeighbors) {
      val cnIndex = vertexArray.indexWhere(getNodeID(_) == cn)

      //      vertexArray(cnIndex)//共有邻居vertex
      val cniEdgeIndex = vertexArray(cnIndex).split(";")(5).split(",").indexWhere(getNeighborID(_) == getNodeID(vi))
      val cnjEdgeIndex = vertexArray(cnIndex).split(";")(5).split(",").indexWhere(getNeighborID(_) == getNodeID(vj))

//      val viEdge: Array[String] = vertexArray(cnIndex).split(";")(5).split(",")(cniEdgeIndex).split(" ")
      val iedge: String = vertexArray(cnIndex).split(";")(5).split(",")(cniEdgeIndex)
      val viEdge: Array[String] = iedge.split(" ")
//      println(s"$cn $cnIndex $cniEdgeIndex ${iedge}")
      val jedge: String = vertexArray(cnIndex).split(";")(5).split(",")(cnjEdgeIndex)
      val vjEdge: Array[String] = jedge.split(" ")
//      val vjEdge: Array[String] = vertexArray(cnIndex).split(";")(5).split(",")(cnjEdgeIndex).split(" ")
      println(s"$cn $cnIndex $cnjEdgeIndex          ${iedge}---${jedge}")
      //ys:在neighbor节点更新edgeList信息，这是修改的部分
      val viUpdateEdge: String = s"${viEdge(0)} 0 ${viEdge(2).toInt + vjEdge(2).toInt} ${viEdge(3).toInt + vjEdge(3).toInt},"

      //更新commonNeighbors的edgeList
      val cnArray: Array[String] = vertexArray(cnIndex).split(";")
      var temp = s"${cnArray(0)};${cnArray(1)};${cnArray(2)};${cnArray(3)};${cnArray(4)};"

      val edgeArray = cnArray(5).split(",")

      for (e <- edgeArray) {
        val eID = getNeighborID(e)

        if (eID == getNodeID(vi)) {
          temp = temp + viUpdateEdge
        }
        else if (eID == getNodeID(vj)) { //ys:id=vj,删除该边信息

        }
        else {
          temp = temp + e + ","
        }
      }

      vertexArray(cnIndex) = temp
    }

    for (uni <- uniqueNeighborsOfVi) {
      val uniIndex = vertexArray.indexWhere(getNodeID(_) == uni)

      val uniEdgeIndex = vertexArray(uniIndex).split(";")(5).split(",").indexWhere(getNeighborID(_) == getNodeID(vi))
      val uniEdge: Array[String] = vertexArray(uniIndex).split(";")(5).split(",")(uniEdgeIndex).split(" ")
      //ys:在neighbor节点更新edgeList信息，这是修改的部分
      val uniUpdateEdge = s"${uniEdge(0)} 0 ${getSize(vi) + getSize(vj)} ${uniEdge(3)},"

      //更新邻居域,先使用vi信息更新
      val uniArray: Array[String] = vertexArray(uniIndex).split(";")
      var temp = s"${uniArray(0)};${uniArray(1)};${uniArray(2)};${uniArray(3)};${uniArray(4)};"

      val edgeArray = uniArray(5).split(",")

      for (e <- edgeArray) {
        if (getNeighborID(e) == getNodeID(vi)) {
          temp = temp + uniUpdateEdge
        }
        else {
          temp = temp + e + ","
        }
      }

      vertexArray(uniIndex) = temp
    }

    for (unj <- uniqueNeighborsOfVj) {
      val unjIndex = vertexArray.indexWhere(getNodeID(_) == unj)

      val unjEdgeIndex = vertexArray(unjIndex).split(";")(5).split(",").indexWhere(getNeighborID(_) == getNodeID(vj))
      val unjEdge: Array[String] = vertexArray(unjIndex).split(";")(5).split(",")(unjEdgeIndex).split(" ")
      //ys:在neighbor节点更新edgeList信息，这是修改的部分
      val unjUpdateEdge = s"${getNodeID(vi)} 0 ${getSize(vi) + getSize(vj)} ${unjEdge(3)},"

      //更新邻居域,先使用vi信息更新
      val unjArray: Array[String] = vertexArray(unjIndex).split(";")
      var temp = s"${unjArray(0)};${unjArray(1)};${unjArray(2)};${unjArray(3)};${unjArray(4)};"

      val edgeArray = unjArray(5).split(",")

      for (e <- edgeArray) {
        if (getNeighborID(e) == getNodeID(vj)) {
          temp = temp + unjUpdateEdge
        }
        else {
          temp = temp + e + ","
        }
      }
      vertexArray(unjIndex) = temp
    }
  }


  def main(args: Array[String]): Unit = {
    println("class: Vertex")
    val vertex = Array(
      "1;1;0;1;0;2 0 1 1,3 0 1 1,4 0 1 1,",
      "2;2;0;1;0;1 0 1 1,4 0 1 1,5 0 1 1,7 0 1 1,",
      "3;3;0;1;0;1 0 1 1,4 0 1 1,9 0 1 1,",
      "4;4;0;1;0;1 0 1 1,2 0 1 1,3 0 1 1,5 0 1 1,7 0 1 1,9 0 1 1,10 0 1 1,",
      "5;5;0;1;0;2 0 1 1,4 0 1 1,6 0 1 1,7 0 1 1,8 0 1 1,",
      "6;6;0;1;0;5 0 1 1,7 0 1 1,8 0 1 1,10 0 1 1,11 0 1 1,12 0 1 1,",
      "7;7;0;1;0;2 0 1 1,4 0 1 1,5 0 1 1,6 0 1 1,8 0 1 1,9 0 1 1,10 0 1 1,",
      "8;8;0;1;0;5 0 1 1,6 0 1 1,7 0 1 1,10 0 1 1,",
      "9;9;0;1;0;3 0 1 1,4 0 1 1,7 0 1 1,10 0 1 1,",
      "10;10;0;1;0;4 0 1 1,6 0 1 1,7 0 1 1,8 0 1 1,9 0 1 1,",
      "11;11;0;1;0;6 0 1 1,13 0 1 1,",
      "12;12;0;1;0;6 0 1 1,13 0 1 1,",
      "13;13;0;1;0;11 0 1 1,12 0 1 1,"
    )

    while(vertex.filter(x => getNodeID(x) == getOwnerID(x)).size >= 5){
      val vi = vertex(scala.util.Random.nextInt(13))
      println(vi)
      if(getNodeID(vi) == getOwnerID(vi)){
        val vj = findCandidate(vi, vertex)
        println(vj)
        if(vj != "" && getNodeID(vj) == getOwnerID(vj) && getAttribute(vi) == getAttribute(vj)) {
          val totalError = calculateErrorIncrease(vi, vj, vertex)
          if(totalError <= 5){
            updateMerge(vi, vj, vertex)
            mergeVertex(vi, vj, vertex)
          }
        }
      }
      println("------------------")
      vertex.filter(x => getNodeID(x) == getOwnerID(x)).foreach(println)
      println()
     }
  }
}
