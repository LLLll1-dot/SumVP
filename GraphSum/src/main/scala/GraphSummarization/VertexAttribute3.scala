package GraphSummarization

import java.io.{File, PrintWriter}


import scala.collection.mutable.{ArrayBuffer, HashMap}

import scala.collection.mutable

/*
* Vertex Structure:
* nodeID: ownerID, attri, size, selfConn
*         edgeList: relation, size, conn
*
* 节点集为Array[String]
* 每一个节点结构为："nodeID;ownerID;attribute;size;selfConn;edgeList"
* edgeList中每个边用逗号隔开，同一边中的各属性用空格隔开: "neighborID relation size conn"
*/

// 误差计算考虑节点属性和边关系，加入关系引入误差计算，更新边关系

//发现一个问题，误差计算时不需要从全局vertexArray中查找邻居信息，vi vi邻居域中的信息足以用于计算

//原算法每次合并总是取最小误差增量，存在删除连接超点的边的情况，该实现过程中只增边、不删边

object VertexAttribute3 {

  //拓扑和关系误差计算所占比重系数
  val COEFFICIENT: Double = 1.0

  def getVertexInfo(ID: Long, vertexArray: Array[VertexClass]): VertexClass = {
   /* val vertex = vertexArray.filter(_.split(";")(0).toInt == ID)(0)
    vertex*/
//    println(s"id: $ID, nodeString: ${vertexArray(ID)}")
    vertexArray(ID.toInt)
  }

  def getVertexInfo(ID: Int, vertexArray: Array[VertexClass]): VertexClass = {
    vertexArray(ID)
  }

  def getNodeID(v: VertexClass): Long = {
    val nodeID = v.nodeID
    nodeID
  }

  def getOwnerID(v: String): Int = {
    val ownerID = v.split(";")(1).toInt

    ownerID
  }

  def getAttribute(v: String): String = {
    val attribute = v.split(";")(2)
    attribute
  }
  def getIdAttribute(id: Int, vertexArray: Array[VertexClass]): Set[String] ={
    getVertexInfo(id, vertexArray).nodeAttr
  }

  def attributeIsEqual(vi: VertexClass, vj: VertexClass): Boolean = {
    vi.nodeAttr == vj.nodeAttr
  }

  def relationIsEqual(iRel: Set[String], jRel: Set[String]): Boolean = {
    val iSet = iRel
    val jSet = jRel
    iSet == jSet
  }


  def getSize(v: VertexClass): Int = {
    val size = v.supernode.size

    size
  }

  def getSelfConn(v: VertexClass): Int = {
    val selfConn = v.selfConn.size

    selfConn
  }

  def getEdgeList(v: VertexClass): Array[EdgeClass] = {
    val edgeList = v.edgeList.values.toArray

    edgeList
  }

  def getEdgeInfo(v: VertexClass, neighborID: Long): EdgeClass = {
    val edgeMap = v.edgeList
    val edge = edgeMap(neighborID)

    edge
  }

  def getEdgeAttr(v: VertexClass, index: Int): List[String] = {
    v.edgeList(index).edgeRelation
  }

  def getEdgeType(edgeString: String): Int={
    val edgeType = edgeString.split("\t")(4).toInt
    edgeType
  }

  def getVijEdgeType(vi: Int, vj: Int, vertexArray: Array[VertexClass]): Int={
    getVertexInfo(vi, vertexArray)
    getVertexInfo(vi, vertexArray).edgeList(vj).edgeType
  }

  def getNeighborID(e: String): Int = {
    val neighborID = e.split("\t")(0).toInt
    neighborID
  }

  def getNeighborRelation(e: String): String = {
    val neighborRelation = e.split("\t")(2)

    neighborRelation
  }

  /*def getNeighborSize(v: VertexClass, neighborID: Long): Int = {
    getEdgeInfo(v, neighborID).edgeSize

  }*/

  def getNeighborConn(e: String): Int = {
    val neighborConn = e.split("\t")(3).toInt

    neighborConn
  }

  def getRandomNeighbor(v: VertexClass): EdgeClass = {
    val edgeList = getEdgeList(v)
    val size: Int = edgeList.length
    if (size > 0) {
      val randomNeighbor: Int = scala.util.Random.nextInt(size) //生成0 ~ size-1 之间的随机数
      val randomEdge = edgeList(randomNeighbor)
      /*val randomNeighborID: Int = getNeighborID(randomEdge) //获取随机邻居ID
      val randomEdgeType = getEdgeType(randomEdge)*/
      randomEdge
    }
    else {
      null
    }
  }
  def getAllRandomNeighbor(v: VertexClass): Array[EdgeClass] = {
    getEdgeList(v)
  }

  def getAllNeighborsId(v: VertexClass): Set[Long] = {
    v.edgeList.keys.toSet
  }
  /*def displayVertex(vertex: VertexClass): Unit = {
    print(s"${vertex.nodeID};${vertex.nodeAttr};${vertex.nodeSize};${vertex.selfConn};")
    displayEdgeMap(vertex.edgeList)
  }*/
  def displayEdgeMap(edgeMap: HashMap[Long, EdgeClass]): Unit = {
    edgeMap.foreach(edge => {
      print(edge._1+": ")
      displayEdge(edge._2)
    })
  }
  def displayEdgeList(edgeArray: Array[EdgeClass]): Unit = {
    edgeArray.foreach(edge => {
      displayEdge(edge)
    })
  }
  def displayEdge(edge: EdgeClass): Unit = {
    println(s"${edge.edgeID},${edge.edgeRelation},${edge.edgeConn},${edge.edgeType}")
  }
  /** vi 为随机选择的超点;
    * vk 为vi的邻居结点;
    * vj 为vk的邻居结点,也就是vi的两跳邻居结点.
    * */
  def getCandidateNeigNodeId(vi: VertexClass, vikEdge: EdgeClass, vertexArray: Array[VertexClass]): Array[VertexClass] = {
    val vjArray: ArrayBuffer[VertexClass] = ArrayBuffer()
    val viId = vi.nodeID
    val vkId = vikEdge.edgeID
//    //第一条边的属性
//    val edgeiRel = edgeInfo.edgeRelation
    val vk = getVertexInfo(vkId, vertexArray)
    val allEdgeMap = vk.edgeList.values.toArray
//    //测试输出
//    vk.edgeList.foreach(x => {
//      print(x._1+": ")
//      displayEdge(x._2)
//    })
    //循环遍历
//    print("vk: ")
//    displayVertex(vk)
    for (edge <- allEdgeMap) {
      val vjId = edge.edgeID
      val vjInfo: VertexClass = getVertexInfo(vjId, vertexArray)
      //第二条边
//      测试输出
//      println(vjId+": ")
//      displayEdgeMap(vjInfo.edgeList)
      val vjkEdge = vjInfo.edgeList(vkId)
      //去除三种类型的两条结点：1）两跳邻居是否是自身？2）边属性是否一样？3）边类型是否一样？4）邻居结点是否被摘要？5）结点是否属性相等？
      if (vjId != viId && (vikEdge.edgeRelation == vjkEdge.edgeRelation)
        && vikEdge.edgeType == vjkEdge.edgeType && vi.nodeAttr == vjInfo.nodeAttr) {
        vjArray += vjInfo
      }
    }
    vjArray.toArray
  }

  def getConnOfVij(vi: VertexClass, vj: VertexClass): List[String] = {
//    displayVertex(vi)
//    displayVertex(vj)
    if(vi.edgeList.contains(vj.nodeID)){
      vi.edgeList(vj.nodeID).edgeConn
    }else {
      List()
    }
  }

  /*def findCandidate(v: String, vertexArray: Array[String]): String = { //随机选择一个二跳候选节点
    val neighbor: Int = getRandomNeighbor(v)

    if (neighbor != -1) {
      //过滤掉自身，防止寻找二跳邻居又找到自身
      val twoHopNeighbor: Array[Int] = getAllNeighborsId(getVertexInfo(neighbor, vertexArray)).filter(_ != getNodeID(v)).toArray

      if (twoHopNeighbor.size > 0) {
        val candidateID: Int = twoHopNeighbor.apply(scala.util.Random.nextInt(twoHopNeighbor.size)) //随机选择一个候选节点nodeID

        val candidate: String = getVertexInfo(candidateID, vertexArray)

        candidate
      }
      else {
        ""
      }
    }
    else {
      ""
    }
  }

  def findCandidates(v: String, vertexArray: Array[String]): Array[String] = {
    val neighbor: Int = getRandomNeighbor(v)

    if (neighbor != -1) {
      //防止寻找二跳邻居又找到自身
      val twoHopNeighborsID: Set[Int] = getAllNeighborsId(getVertexInfo(neighbor, vertexArray)).filter(_ != getNodeID(v))

      //选择所有二跳邻居
      val twoHopNeighbors: Array[String] = twoHopNeighborsID.map(x => getVertexInfo(x, vertexArray)).toArray

      twoHopNeighbors
    }
    else {
      Array[String]()
    }

  }*/

  def findBestCandidate(v: VertexClass, vertexArray: Array[VertexClass], iteration: Double): VertexClass = {
//    val edgeList = getEdgeList(v)
    var candidate: VertexClass = null
    var similarity = 0.0f
    var commonSize = 0
//    val randomEdge = getRandomNeighbor(v)
    val randomAllEdge = getAllRandomNeighbor(v)
//    println(s"randomEdge: $randomEdge")
//    if (randomEdge != null) {
    for (randomEdge <- randomAllEdge) {
      val twoHopNeighbors: Array[VertexClass] = getCandidateNeigNodeId(v, randomEdge, vertexArray)
//      println(s"twoHopNeighbors: ${twoHopNeighbors.length}")
      //        if (iteration >= 1) {
      //          similarity = 0f
      //        }
      for (neighbor <- twoHopNeighbors) {
        val inteSize = (getAllNeighborsId(v) intersect getAllNeighborsId(neighbor)).size //交集
        val uniSize = (getAllNeighborsId(v) union getAllNeighborsId(neighbor)).size //并集
//        println(s"inteSize: $inteSize, uniSize: $uniSize")
        val s: Float = inteSize.toFloat / uniSize.toFloat
        //        val s:Double = 2 * inte.size - uni.size
        if (s > similarity || (s == similarity && commonSize > inteSize)) {
          candidate = neighbor
          similarity = s
          commonSize = inteSize
        }
      }
    }
//    println("candidate: "+candidate)
    //    }
    if (similarity == 0) {
      null
    } else {
      candidate
    }
  }

  def getClassifiedNeighbors(vi: VertexClass, vj: VertexClass): Array[Array[Long]] = {

    //测试
    val viId = vi.nodeID
    val vjId = vj.nodeID
    val neighborsOfVi: Set[Long] = getAllNeighborsId(vi)
    val neighborsOfVj: Set[Long] = getAllNeighborsId(vj)
    val commonNeighbors: Set[Long] = neighborsOfVi intersect neighborsOfVj
    //测试输出
    /*for(cn <- commonNeighbors){
      val cnNeighborId: Array[Int] = getEdgeList(getVertexInfo(cn, vertexArray)).map(getNeighborID(_))
      if(!cnNeighborId.contains(viId) || !cnNeighborId.contains(vjId)){
        commonNeighbors -= cn
        print(s"getClassifiedNeighbor, unsatisfied neighbor node id: ${getVertexInfo(cn, vertexArray)}\t")
      }
    }*/

    //注意vi、vj为直接邻居的情况，特有邻居集中需要除去彼此
    val uniqueNeighborsOfVi: Set[Long] = neighborsOfVi diff commonNeighbors
    val uniqueNeighborsOfVj: Set[Long] = neighborsOfVj diff commonNeighbors
    /*val uniqueNeighborsOfVi: Set[Int] = (neighborsOfVi diff commonNeighbors) - vjId
    val uniqueNeighborsOfVj: Set[Int] = (neighborsOfVj diff commonNeighbors) - viId*/

    /*//测试代码
    for(uniVi <- uniqueNeighborsOfVi){
      val uniqueViId: Array[Int] = getEdgeList(getVertexInfo(uniVi, vertexArray)).map(getNeighborID(_))
      if(!uniqueViId.contains(viId)){
        uniqueNeighborsOfVi -= uniVi
        print(s"getClassifiedNeighbor, unsatisfied neighbor node id: ${getVertexInfo(uniVi, vertexArray)}\t")
      }
    }
    for(uniVj <- uniqueNeighborsOfVj){
      val uniqueVjId: Array[Int] = getEdgeList(getVertexInfo(uniVj, vertexArray)).map(getNeighborID(_))
      if(!uniqueVjId.contains(vjId)){
        uniqueNeighborsOfVj -= uniVj
        print(s"getClassifiedNeighbor, unsatisfied neighbor node id: ${getVertexInfo(uniVj, vertexArray)}\t")
      }
    }*/

    Array(commonNeighbors.toArray, uniqueNeighborsOfVi.toArray, uniqueNeighborsOfVj.toArray)
  }

  def calculateErrorIncrease(vi: VertexClass, vj: VertexClass, vertexArray: Array[VertexClass]): Double = {
    val errorIncrease: Double = calculateTopologyError(vi: VertexClass, vj: VertexClass, vertexArray)
    //    val errorIncrease: Double = COEFFICIENT * calculateTopologyError(vi: String, vj: String)
    //    + (1 - COEFFICIENT) * calculateRelationError(vi: String, vj: String)

    //    println("the error increase of this merge is " + errorIncrease)
    errorIncrease
  }
//  //关系误差计算主要考虑实际关系的增加、删除、更改
//  def calculateRelationError(vi: String, vj: String, vertexArray: Array[String]): Int = {
//    val Array(commonNeighbors, uniqueNeighborsOfVi, uniqueNeighborsOfVj) = getClassifiedNeighbors(vi, vj)
//
//    val sizeOfVi: Int = getSize(vi)
//    val sizeOfVj: Int = getSize(vj)
//
//    var relationError: Int = 0
//    for (cn <- commonNeighbors) {
//      val edgeOfVi: String = getEdgeList(vi).filter(getNeighborID(_) == cn)(0)
//      val relationOfVi = getNeighborRelation(edgeOfVi)
//
//      val edgeOfVj: String = getEdgeList(vj).filter(getNeighborID(_) == cn)(0)
//      val relationOfVj = getNeighborRelation(edgeOfVj)
//
//      val size: Int = {
//        if (getNeighborSize(edgeOfVi) == getNeighborSize(edgeOfVj))
//          getNeighborSize(edgeOfVi)
//        else
//          getNeighborSize(edgeOfVj) //并发时可能存在共有邻居大小不一样的情况！！！！！
//      }
//
//      if (relationOfVi != relationOfVj) {
//        if (sizeOfVi >= sizeOfVj) {
//          relationError += 1
//        }
//        else {
//          relationError += 1
//        }
//      }
//      else {
//        relationError += 0
//      }
//    }
//
//    for (uni <- uniqueNeighborsOfVi) {
//      val size: Int = getNeighborSize(vi, uni)
//      relationError += 1
//    }
//
//    for (unj <- uniqueNeighborsOfVj) {
//      val size: Int = getNeighborSize(vj, unj)
//      relationError += 1
//    }
//
//    relationError
//  }

  def calculateTopologyError(vi: VertexClass, vj: VertexClass, vertexArray: Array[VertexClass]): Int = {
    val Array(commonNeighbors, uniqueNeighborsOfVi, uniqueNeighborsOfVj) = getClassifiedNeighbors(vi, vj)

    var commonTopologyError = 0
//    for (cn <- commonNeighbors) {
//      commonTopologyError += calculateCommonError(vi, vj, cn)
//    }
//  println(s"commonNeighbors: $commonNeighbors")
    var uniqueTopologyErrorOfVi = 0
    for (uni <- uniqueNeighborsOfVi) {
      uniqueTopologyErrorOfVi += calculateUniqueError(vi, vj, uni, vertexArray)
    }
//    println(s"uniqueNeighborsOfVi: $uniqueNeighborsOfVi")

    var uniqueTopologyErrorOfVj = 0
    for (unj <- uniqueNeighborsOfVj) {
      uniqueTopologyErrorOfVj += calculateUniqueError(vj, vi, unj, vertexArray)
    }
//    println(s"uniqueNeighborsOfVj: $uniqueNeighborsOfVj")

    val selfTopologyError: Int = calculateSelfError(vi, vj)

    val topologyError: Int = commonTopologyError + uniqueTopologyErrorOfVi + uniqueTopologyErrorOfVj + selfTopologyError

//    println(s"commonTopologyError: $commonTopologyError, uniqueTopologyErrorOfVi: $uniqueTopologyErrorOfVi" +
//      s", uniqueTopologyErrorOfVj: $uniqueTopologyErrorOfVj, topologyError: $topologyError")
    topologyError
  }

  def calculateCommonError(vi: VertexClass, vj: VertexClass, pID: Long): Int = { //commonError = em,p - ei,p - ej,p

    val edgeOfVip = vi.edgeList(pID)
    val connOfVip = edgeOfVip.edgeConn.size
    val viNodeSize= vi.supernode.size

    val edgeOfVjp = vj.edgeList(pID)
    val connOfVjp = edgeOfVjp.edgeConn.size
    val vjNodeSize = vj.supernode.size

    val sizeOfVp = edgeOfVip.edgeConn.size
    /*val sizeOfVp: Int = {
      if (edgeOfVip.edgeSize == edgeOfVjp.edgeSize)
      edgeOfVip.edgeSize
      else
        edgeOfVjp.edgeSize
    }*/

    val emp: Int = (viNodeSize + vjNodeSize) * sizeOfVp - (connOfVip + connOfVjp)
    val eip: Int = viNodeSize * sizeOfVp - connOfVip
    val ejp: Int = vjNodeSize * sizeOfVp - connOfVjp

    val commonError: Int = emp - eip - ejp

    commonError
  }

  def calculateUniqueError(vi: VertexClass, vj: VertexClass, qID: Long, vertexArray: Array[VertexClass]): Int = { //uniqueError = em,q - ei,q
    val edgeOfViq = vi.edgeList(qID)
    val sizeOfVq = vertexArray(qID.toInt).supernode.size
    val connOfViq = edgeOfViq.edgeConn.size
//相当于在vj和vq之间添加边
//    val emq: Int = (vi.supernode.size + vj.supernode.size) * sizeOfVq - connOfViq
//    val eiq: Int = vi.nodeSize * sizeOfVq - connOfViq

    val uniqueError: Int = vj.supernode.size * sizeOfVq - connOfViq

    uniqueError
  }

  def calculateSelfError(vi: VertexClass, vj: VertexClass): Int = { //selfError = em,m - ei,i - ej,j - ei,j
    val size: Int = vi.supernode.size + vj.supernode.size
    val connOfVij: Int = getConnOfVij(vi, vj).size

    val viSelfConn = getSelfConn(vi)
    val vjSelfConn = getSelfConn(vj)

    val selfConnOfVm: Int = viSelfConn + vjSelfConn + connOfVij

    val f = (size: Int, selfConn: Int) => size * (size - 1) - selfConn //定义函数，计算自连接误差

    val eii: Int = f(getSize(vi), viSelfConn)
    val ejj: Int = f(getSize(vj), vjSelfConn)
    val emm: Int = f(size, selfConnOfVm)
    //    val eij: Int = min(getSize(vi) * getSize(vj) - connOfVij, connOfVij) //算法原始定义误差ei,j计算方式
//    val eij: Int = getSize(vi) * getSize(vj) - connOfVij //假设节点合并，引入误差计算方式

    val selfError: Int = emm - eii - ejj - connOfVij

    //    println(s"selferror: emm:${emm}  eii:${eii}  ejj:${ejj}  eij:${eij}")
    selfError
  }

  def mergeVertex(vi: VertexClass, vj: VertexClass, vertexArray: Array[VertexClass]): Unit = { //假设vi.nodeID < vj.nodeID
    val viID = vi.nodeID
    val vjID = vj.nodeID
    vertexArray(vjID.toInt) = null
    val supernode = vi.supernode ++ vj.supernode
//    val connOfVij: Int = getConnOfVij(vi, vj)
    val vijConn = vi.selfConn ++ vj.selfConn ++ getConnOfVij(vi, vj)

    //    //更新vi邻居集
    //    val neighborsOfVi: Set[Int] = getAllNeighbors(vi)
    //    val neighborsOfVj: Set[Int] = getAllNeighbors(vj)
    //
    //    val commonNeighbors: Set[Int] = neighborsOfVi intersect neighborsOfVj
    //    //注意vi、vj为直接邻居的情况，特有邻居集中需要除去彼此
    //    val uniqueNeighborsOfVi: Set[Int] = (neighborsOfVi diff commonNeighbors).filter(_ != getNodeID(vj))
    //    val uniqueNeighborsOfVj: Set[Int] = (neighborsOfVj diff commonNeighbors).filter(_ != getNodeID(vi))

    val Array(commonNeighbors, uniqueNeighborsOfVi, uniqueNeighborsOfVj) = getClassifiedNeighbors(vi, vj)

    var viEdgeMap = vi.edgeList
    commonNeighbors.filter(_ != vjID).foreach(edgeID => {
      viEdgeMap(edgeID).edgeConn = vj.edgeList(edgeID).edgeConn ++ viEdgeMap(edgeID).edgeConn
    })

    uniqueNeighborsOfVj.foreach(edgeID => {
      viEdgeMap += (edgeID -> vj.edgeList(edgeID))
//      viEdgeMap.put(edgeID, vj.edgeList(edgeID))
    } )
//    val vertex = new VertexClass(viID, supernode, vi.nodeAttr, vijConn, viEdgeMap)
    vertexArray(viID.toInt) = new VertexClass(viID, supernode, vi.nodeAttr, vijConn, viEdgeMap)
  }

  //假设vi.nodeID < vj.nodeID
  def updateMerge(vi: VertexClass, vj: VertexClass, vertexArray: Array[VertexClass]): Unit = {
    val Array(commonNeighbors, uniqueNeighborsOfVi, uniqueNeighborsOfVj) = getClassifiedNeighbors(vi, vj)

    val viID = getNodeID(vi)
    val vjID: Long = getNodeID(vj)
    val viSize: Int = getSize(vi)
    val vjSize = getSize(vj)
    val vijSize = viSize + vjSize
    //更新共同邻居的边信息
//    if (commonNeighbors.nonEmpty) {
      for (cnIndex <- commonNeighbors.map(_.toInt)) {
        //并发
        val cnInfo = getVertexInfo(cnIndex, vertexArray)
        val vjEdge = removeEdge(cnInfo, vjID)
        val viEdge = cnInfo.edgeList(viID)
//        val cnEdge = s"${viID}\t${viEdge.edgeRelation}\t${viEdge.edgeSize + vjEdge.edgeSize}\t${viEdge.edgeConn + vjEdge.edgeConn}\t${viEdge.edgeType}"
        val cnEdge = new EdgeClass(viID, viEdge.edgeRelation, viEdge.edgeConn ++ vjEdge.edgeConn, viEdge.edgeType)
//        vertexArray(cnIndex).edgeList.put(viID, cnEdge)
        vertexArray(cnIndex).edgeList += (viID -> cnEdge)

      }
//    }

    //更新节点vi独有邻居的边信息
//    if (!uniqueNeighborsOfVi.isEmpty) {
//      for (uniIndex <- uniqueNeighborsOfVi if uniIndex != vjID) { //不需要处理vi和vj存在边的情况
//          getVertexInfo(uniIndex, vertexArray).edgeList(viID).edgeSize = vijSize
//      }
//    }

    if (!uniqueNeighborsOfVj.isEmpty) {
      for (unjIndex <- uniqueNeighborsOfVj if unjIndex != viID) {
        /*val unjEdgeIndex = vertexArray(unjIndex).split(";")(5).split("\t,\t").indexWhere(getNeighborID(_) == getNodeID(vj))
      val unjEdge: Array[String] = vertexArray(unjIndex).split(";")(5).split("\t,\t")(unjEdgeIndex).split("\t")*/
//        val unjInfo = getVertexInfo(unjIndex, vertexArray)
        val vjEdge = removeEdge(getVertexInfo(unjIndex, vertexArray), vjID)
        val unjEdge = new EdgeClass(viID, vjEdge.edgeRelation, vjEdge.edgeConn, vjEdge.edgeType)
        getVertexInfo(unjIndex, vertexArray).edgeList += (viID -> unjEdge)
      }
    }
  }
  def removeEdge(vertex: VertexClass, id: Long): EdgeClass = {
    if(vertex.edgeList.contains(id)) {
      val edge = vertex.edgeList(id)
      vertex.edgeList -= id
      edge
    } else {
      null
    }

    /*vertex.edgeList.remove(id) match {
      case Some(edge) =>
        edge
      case None =>
        null
    }*/
  }

  def mergeTypeNode(vertexArray: Array[String], superNodes: HashMap[Int, Set[Int]]) = {
    val groupMap: Map[Predef.Set[String], Array[String]] = vertexArray.filter(_.endsWith(";")).groupBy(x=>x.split(";")(2).split(" , ").toSet)
    groupMap.foreach(x => {
      x._2.map(_.split("\t")(0).toInt).toList.sorted
    })
  }

//  def mergeOneDegreeNode(vertexArray: Array[String], superNodes: HashMap[Int, Set[Int]]) = {
//    //筛选出edgeList中的size等于1的vertex
//    val oneDegreeArray = vertexArray.filter(_.split(";")(5).split("\t,\t").size == 1)
//    val oneDegreeMap: HashMap[String, Set[Int]] = HashMap()
//
//    for (v <- oneDegreeArray) {
//      val vId = getNodeID(v) //一度节点的节点id
//      val hubId: Int = getNeighborID(v.split(";")(5))    //一度节点的邻居节点id
//      //邻居节点id（非一度），节点属性（一度），边属性（一度节点->非一度节点）
//      val s = s"$hubId\t${getIdAttribute(vId, vertexArray)}\t${getEdgeAttr(v,0)}\t${getEdgeType(v.split(";")(5).split("\t,\t")(0))}"
//      if (oneDegreeMap.contains(s)) {
//        //加入
//        //        if(getAttribute(getVertexInfo(oneDereeMap(hubId).head,vertexArray)) == getAttribute(getVertexInfo(vId,vertexArray)))
//        //        if(getIdAttribute(oneDegreeMap(hubId).head))
//        oneDegreeMap(s) += vId //oneDegreeMap的保存的数据为：hubId -> Set(Vid,vId) ,其中Vid是Set集合里已经存在的Id；其实就是将vId添加到hubId的集合中
//      }
//      else {
//        oneDegreeMap += (s -> Set(vId)) // 将数据以hubId -> Set(vId)的形式添加到oneDegreeMap中
//      }
//    }
//
//    //合并一度节点
//    for ((s, vs) <- oneDegreeMap) {
//      if (vs.size > 1) {
//        val sortedList = vs.toList.sorted
//        val size = sortedList.size
//        val vId: Int = sortedList(0)
//        val vIndex = vId
//        val v: Array[String] = vertexArray(vIndex).split(";")
//        vertexArray(vIndex) = s"${vId};${vId};${v(2)};${size};${v(4)};${v(5)}"
//
//        for (i <- 1 until size) {
//          val index = sortedList(i)
//          val vSplit: Array[String] = vertexArray(index).split(";")
//          vertexArray(index) = s"${vSplit(0)};${vId};${vSplit(2)};${vSplit(3)};${vSplit(4)};${vSplit(5)}"
//          updateSuperNodes(vId, sortedList(i), superNodes)
//        }
//
//        //更新hub邻居域
//        //ys:主要是找到edgeList里面的所有不属于vs集合的邻居边信息添加到temp（新的edgeList）里，然后更新合并后节点的信息，再加入到temp里
////        val hubIndex: Int = vertexArray.indexWhere(getNodeID(_) == s.split("\t")(0).toInt)
//        val hubIndex: Int = s.split("\t")(0).toInt
//        val hubSplit: Array[String] = vertexArray(hubIndex).split(";")
//        val hubNeighborsArray: Array[String] = hubSplit(5).split("\t,\t")
//
//        var temp: String = hubSplit(0) + ";" + hubSplit(1) + ";" + hubSplit(2) + ";" + hubSplit(3) + ";" + hubSplit(4) + ";"
//        for (nbr <- hubNeighborsArray) {
//          //对1、节点id为超级节点；2、为hub的邻居节点但是不在一度节点集合中的节点，均不做处理（就是删掉被摘要掉的节点边信息）
//          if (getNeighborID(nbr) == vId || !vs.contains(getNeighborID(nbr))) {
//            temp = temp + nbr + "\t,\t"
//          }
//        }
//        vertexArray(hubIndex) = temp //更新所有hub节点在邻居
//      }
//    }
//  }


  def createInitialSupernodes(arrayString: Array[String]): HashMap[Int, Set[Int]] = {
    val supernodes: HashMap[Int, Set[Int]] = HashMap()
    for (i <- 0 until arrayString.length) {
      supernodes += (i -> Set(i))
    }

    supernodes
  }

  def updateSuperNodes(viID: Long, vjID: Long, supernodes: HashMap[Long, scala.collection.mutable.Set[Long]]): Unit = {
    //更新合并一度节点
    supernodes(viID) ++= supernodes(vjID)
    supernodes -= vjID
  }

  //  def updateSuperNodes(vi: String, vj: String, supernodes: HashMap[Int, Array[Int]]) = {
  //    val viID: Int = getNodeID(vi)
  //    val vjID: Int  = getNodeID(vj)
  //
  //    if(supernodes.contains(viID) && supernodes.contains(vjID)){
  //      supernodes(viID) = supernodes(viID) ++ supernodes(vjID)
  //      supernodes -= vjID
  //    }
  //    else if(supernodes.contains(viID)){
  //      supernodes(viID) = supernodes(viID) ++ Array(vjID)
  //    }
  //    else if(supernodes.contains(vjID)){
  //      supernodes(viID) = supernodes(vjID) ++ Array(viID)
  //      supernodes -= vjID
  //    }
  //    else{
  //      supernodes(viID) = Array(viID, vjID)
  //    }
  //  }

  //  def updateSuperNodes(vi: String, vj: String, supernodes: ConcurrentHashMap[Int, Set[Int]]) = {//并发supernodes
  //    val viID: Int = getNodeID(vi)
  //    val vjID: Int  = getNodeID(vj)
  //
  //    if(supernodes.containsKey(viID) && supernodes.containsKey(vjID)){
  //      supernodes.put(viID, supernodes.get(viID) ++ supernodes.get(vjID))
  //      supernodes.remove(vjID)
  //    }
  //    else if(supernodes.containsKey(viID)){
  //      supernodes.put(viID, supernodes.get(viID) + vjID)
  //    }
  //    else if(supernodes.containsKey(vjID)){
  //      supernodes.put(viID, (supernodes.get(vjID) + viID))
  //      supernodes.remove(vjID)
  //    }
  //    else{
  //      supernodes.put(viID, Set(viID,vjID))
  //    }
  //  }



  def saveSupernodesAsFile(nodeNumber: Long, vertexArray: Array[VertexClass],
                           nodeIdArray: Array[String],
                           typeSet: scala.collection.mutable.Set[String],
                           supernodesFilePath: String) = {
    //调试查看vertexArray数组，将该数组保存到文件中
    val writerNodeInfo = new PrintWriter(new File(s"./results/20k/${nodeNumber}-Hashtable.txt"))
    val writerSupernodes = new PrintWriter(new File(supernodesFilePath))
    for(vertex <- vertexArray){
      if(vertex != null) {
        val supernodeID = vertex.nodeID
        //save hashtable
        val edge = vertex.edgeList.values.filter(_.edgeType == 0).map(x => {
          nodeIdArray(x.edgeID.toInt) + "__:" + x.edgeConn.toArray.mkString("__")
        }).toArray.mkString("___")
        if(!edge.equals(""))  writerNodeInfo.println(nodeIdArray(supernodeID.toInt) + "___:" + edge)
        //save supernode
        writerSupernodes.println(nodeIdArray(supernodeID.toInt)+"----"+vertex.supernode.mkString("\t"))
      }
    }
    writerNodeInfo.close()

    /*for ((key, value) <- supernodes) {
      val array = value.toArray.sorted
      writerSupernodes.print(nodeIdArray(key.toInt)+"----")
      for (v <- array) {
        writerSupernodes.print(nodeIdArray(v.toInt) + "\t")
      }
      writerSupernodes.println()
    }*/
    for(v <- typeSet) {
      writerSupernodes.println(v + "----" + v)
    }
    writerSupernodes.close()
  }

  def saveHashtable(vertexArray: Array[VertexClass], nodeIdArray: Array[String], superedgesFilePath: String): Unit = {
    var id = vertexArray.length
    val typeMap: HashMap[String, Int] = HashMap()
    val result1: Array[String] = vertexArray.filter(_ != null)
      .map(v => v.nodeID + "---"
        + v.supernode.mkString("__") + "---"
        + {
        if(v.nodeAttr.nonEmpty) {
          v.nodeAttr.map(tp => {
            if(!typeMap.contains(tp)) {
              typeMap.put(tp, id)
              id+=1
            }
            typeMap(tp) + "--" + v.supernode.map(v => v + "\trdf:type\t" + tp).mkString("__")
          }).mkString("___") + "___"
        } else {
          ""
        }
      }
        + {
        if (v.selfConn.nonEmpty) {
          v.nodeID + "--" + v.selfConn.map(_.split("\t")).flatMap(x => x(1).split(",").map(p => (x(0)+"\t"+p+"\t"+x(2)))).mkString("__") + "___"
        } else {
          ""
        }
      }
        + v.edgeList.values.filter(_.edgeType == 0).map(e => e.edgeID + "--"
        + e.edgeConn.map(_.split("\t")).flatMap(x => x(1).split(",").map(p => (x(0)+"\t"+p+"\t"+x(2)))).mkString("__")).mkString("___"))
    val result = result1 ++ typeMap.map(k => k._2 + "---" + k._1 + "---")
    val writerSuperedges = new PrintWriter(new File(superedgesFilePath))
    result.foreach(writerSuperedges.println)
    writerSuperedges.close()
    val sparkSession = GraphActor.Main._sparkSession
    import sparkSession.implicits._

    val x = sparkSession.sparkContext.parallelize(result).toDF("edge").toDF()
    x.repartition(2).write.format("parquet").option("compression","snappy").save(superedgesFilePath.replace(".txt", ""))
  }

  def saveSuperedgesAsFile(vertexArray: Array[VertexClass], nodeIdArray: Array[String], superedgesFilePath: String): mutable.Set[String] = {
    //vertexArray.foreach(println)
    var typeSet: mutable.Set[String] = mutable.Set()
    val writerSuperedges = new PrintWriter(new File(superedgesFilePath))
    for(vertex <- vertexArray) {
      if(vertex != null) {
        val src = vertex.nodeID
        val eTypeSet = vertex.nodeAttr
        typeSet = typeSet.union(eTypeSet)
        if(eTypeSet.nonEmpty) {
          for (t <- eTypeSet) {
            writerSuperedges.println(nodeIdArray(src.toInt) + s"\t${ReadFile.typeString}\t" + t)
          }
        }
        if (vertex.edgeList.nonEmpty) {
          //          val edgeList = vertex(5).split("\t,\t").filter(getEdgeType(_) == 0).sortBy(e => e.split("\t")(0).toInt)(Ordering.Int)
          val edgeList = vertex.edgeList.values.toArray.filter(_.edgeType == 0).sortBy(_.edgeID)
          for (edge <- edgeList) {
            edge.edgeRelation.foreach(x => {
              writerSuperedges.println(nodeIdArray(src.toInt) + "\t" + x + "\t" + nodeIdArray(edge.edgeID.toInt))
            })
          }
        }
      }
    }
    writerSuperedges.close()
    typeSet
  }

  /*//保存超级结点去查询
  def saveSupernodesForQuery(supernodes: HashMap[Int, Set[Int]], nodeIdArray: Array[String], filePath: String):Unit = {//调试查看vertexArray数组，将该数组保存到文件中
    val supernodePath = filePath + "supernodes.txt"
    val writerSupernodes = new PrintWriter(new File(supernodePath))
    for ((key, value) <- supernodes) {
      //      println("vertexArray(0):"+vertexArray(0))
      //      println("key:"+key)
      val array = value.toArray.sorted
      writerSupernodes.print(nodeIdArray(key))
      for (v <- array) {
        writerSupernodes.print("\t" + nodeIdArray(v))
      }
      writerSupernodes.println()
    }
    writerSupernodes.close()

  }*/

//  def saveSuperedgesForQuery(vertexArray: Array[String], nodeIdArray: Array[String], FilePath: String):Unit = {
////    val vertexArraysorted = vertexArray.sortBy(v => v.split(";")(0).toInt)(Ordering.Int)
//    val typeWriter = new PrintWriter(new File(FilePath + "rdf-type.txt"))
//    val classSet: HashMap[String, Set[String]] = HashMap()
//    for (v <- vertexArray) {
//      if (getNodeID(v) == getOwnerID(v)) {
//        val vertex = v.split(";")
//        val src = vertex(0).toInt
//        if(vertex(2).length > 0){
//          for(typePerporty <- vertex(2).split(" , ")){
//            typeWriter.println(nodeIdArray(src) + "\t" + typePerporty)
//          }
//        }
//        val edgeList = vertex(5).split(",").filter(getEdgeType(_) == 0).sortBy(e => e.split("\t")(0).toInt)(Ordering.Int)
//        for (edge <- edgeList) {
//          val e = edge.split("\t")
//          if(classSet.contains(e(1))){
//            classSet(e(1)) += nodeIdArray(src) + "\t" + nodeIdArray(e(0).toInt)
//          }
//          else {
//            classSet.put(e(1), Set(nodeIdArray(src) + "\t" + nodeIdArray(e(0).toInt)))
//          }
//        }
//      }
//    }
//    typeWriter.close()
//    //写入文件
//    for((predicate, soSet) <- classSet){
//      val printUrl = {
//        if (predicate.substring(0, 1).equals("<")) {
//          s"$FilePath${predicate.substring(1, predicate.length - 1).replace(":","-")}.txt"
//        } else {
//          s"$FilePath${predicate.replace(":","-")}.txt"
//        }
//      }
////      val printUrl = s"$FilePath${predicate.replace("<","").replace(">","").replace(":","-")}.txt"
//      val printWriter = new PrintWriter(new File(printUrl))
//      val soList = soSet.toList
//      for(line <- soList){
//        printWriter.println(line)
//      }
//      printWriter.close()
//    }
//  }

  def saveInitialInfoToFile(initialGraph: Array[VertexClass], nodeIdArray: Array[String], path: String): Unit ={
    val graphPrintWriter = new PrintWriter(new File(path + "initialGraph.txt"))
    for(vertex <- initialGraph){
      graphPrintWriter.println(vertex)
    }
    graphPrintWriter.close()
    val nodeIdPrintWriter = new PrintWriter(new File(path + "nodeId.txt"))
    for(i <- nodeIdArray.indices){
      nodeIdPrintWriter.println(i + "\t" + nodeIdArray(i))
    }
    nodeIdPrintWriter.close()
  }

  //保存超级结点去查询
  //调试查看vertexArray数组，将该数组保存到文件中
  def saveSupernodesIdForQuery(supernodes: HashMap[Int, Set[Int]], Path: String):Unit = {
    //保存超级结点的id
    val supernodeIdPath = Path + "supernodesId.txt"
    val writerSupernodesId = new PrintWriter(new File(supernodeIdPath))
    for ((key, value) <- supernodes) {
      //      println("vertexArray(0):"+vertexArray(0))
      //      println("key:"+key)
      val array = value.toArray.sorted
//      writerSupernodesId.print(key)
      for (v <- array) {
        writerSupernodesId.print(v + "\t")
      }
      writerSupernodesId.println()
    }
    writerSupernodesId.close()
  }

}
