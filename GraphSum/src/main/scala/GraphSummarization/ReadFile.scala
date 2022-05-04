package GraphSummarization


import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.{ArrayBuffer, HashMap, HashSet, Set}
import scala.io.Source
import java.io.File
import scala.collection.mutable
import GraphSummarization.DataPreprocess.{processLUBM, processWatdiv}

object ReadFile {
  /*
  * Vertex Structure:
  *   node: nodeId,ownerID,attribute,size,selfConn
  *   edgeList: edgeId,relation,size,conn
  *
  * 节点集为Array[String]
  * 每一个节点结构为："nodeID;ownerID;attribute;size;selfConn;edgeList"
  * edgeList中每个边用逗号隔开，同一边中的各属性用空格隔开: "neighborID relation size conn"
  *
  */
  //////////////////////////////////////////*******************//////////////////////////////////////////////////
  /*
  node结构：subjectId;ownId;subjectAttribute;size;selfConn;edgeList,
  * edgeList:objectId property size conn type
  */
  var typeString = "rdf:type"
  val _sparkSession: SparkSession = GraphActor.ActorDriver._sparkSession

  import _sparkSession.implicits._

  def readAdjacentList(url: String): Array[String] = {
    val file = Source.fromFile(url)

    //构建节点结构
    val vertexArray: ArrayBuffer[String] = ArrayBuffer()

    for (line <- file.getLines()) {
      val e: ArrayBuffer[String] = new ArrayBuffer[String] ++ line.split("\t")

      val nodeID: String = e(0)
      val attribute: Int = 0
      //      val attribute = e(1)

      e.remove(0, 2) //从数组索引为0开始删除2个元素, 获取邻居集

      var edgeList: String = ""
      for (x <- e) {
        edgeList += s"${x} 0 1 1," //边属性默认相同，默认值为0
      }

      //      val vertex: String = s"${nodeID};${nodeID};0;1;0;${edgeList}"//节点属性默认相同，默认值为0
      vertexArray += s"${nodeID};${nodeID};${attribute};1;0;${edgeList}"
    }

    //    val vertexArraySorted: ArrayBuffer[String] = vertexArray.sortBy(v => v.split(";")(0).toInt)(Ordering.Int)//按照ID排序
    //    val vertexArraySorted: ArrayBuffer[String] = vertexArray.sortBy(v =>
    //      (v.split(";")(5).split(",").size, v.split(";")(0).toInt))(Ordering.Tuple2(Ordering.Int.reverse, Ordering.Int))//按照degree排序
    vertexArray.toArray
  }

  //将triple:(s, p, o), 变成Array((s, p, o), (o, p, s))，p=type除外
  def doubleEdge(string: String): Array[(String, String)] = {
    val split = string.split("\t")
    if (!split(1).contains("type") && !split(0).equals(split(2))) {
      Array((split(0), string + "\t0"), (split(2), s"${split(2)}\t${split(1)}\t${split(0)}\t1"))
    } else {
      Array((split(0), string + "\t0"))
    }
  }
  //  //获得边的另一个端点
  //  def getEdgePoint(edge: Array[String]): String = {
  //    if(edge(3) == "0") {
  //      edge(2)
  //    } else {
  //      edge(0)
  //    }
  //  }

  def readFile(urlString: String, spark: SparkSession):
  (Array[VertexClass], Array[String]) = {
    import spark.implicits._
    println(urlString)
    val triple = processLUBM(urlString).flatMap(doubleEdge)
    //    val triple = processWatdiv(urlString).flatMap(doubleEdge(_))
    println("finish reading dataset, start construct hashtable...")
    val vertexGroup = triple.groupByKey(_._1)
      .mapGroups { case (k, v) => (k, v.map(x => x._2).toArray) }
      .collect()
    var id = -1L
    val nodeIdArray: ArrayBuffer[String] = ArrayBuffer()
    val nodeIdMap: HashMap[String, Long] = HashMap()
    vertexGroup.foreach(x => {
      id += 1
      nodeIdArray += x._1
      nodeIdMap.put(x._1, id)
    })
    val vertexArray: Array[VertexClass] = vertexGroup.map(vertex => {
      val nodeAttr: Predef.Set[String] = vertex._2.filter(_.split("\t")(1).contains("type")).map(_.split("\t")(2)).toSet
      val self: List[String] = vertex._2.map(_.split("\t")).filter(x => x(0).equals(x(2))).map(_.mkString("\t")).toList
      val edgeList: HashMap[Long, EdgeClass] = HashMap[Long, EdgeClass]()
      vertex._2.filter(!_.split("\t")(1).contains("type"))
        .map(_.split("\t"))
        .foreach(e => {
          edgeList.put(nodeIdMap(e(2)), new EdgeClass(nodeIdMap(e(2)), e(1).split(",").toList, List(getTriple(e)), e(3).toInt))
        })
      new VertexClass(nodeIdMap(vertex._1), List(vertex._1), nodeAttr, self, edgeList)
    })
    (vertexArray, nodeIdArray.toArray)

  }
  /*def readFile(urlString: String, fileType: String, spark: SparkSession):
  (Array[VertexClass], Array[String]) = {
    import spark.implicits._
    println(urlString)
    val triple = processLUBM(urlString).flatMap(doubleEdge(_))
    println("finish reading dataset, start construct hashtable...")
    val vertexGroup = triple.groupByKey(_._1)
      .mapGroups { case (k, v) => (k, v.map(x => x._2).toArray) }
    val nodeIdArray = vertexGroup.map(_._1).collect()
    var nodeIdMap: Map[String, Long] = Map()
    for(id <- nodeIdArray.indices) {
      nodeIdMap += (nodeIdArray(id) -> id)
    }
    val vertexArray: Array[VertexClass] = vertexGroup.map(vertex => {getVertex(vertex._1, vertex._2, nodeIdMap)
    }).collect()
      .map(x => new VertexClass(x._1, x._2, x._3, x._4,
        x._5.map(y => (y._1 -> new EdgeClass(y._2._1, y._2._2, y._2._3, y._2._4)) ).toMap ) )
    display(vertexArray, nodeIdArray)
    (vertexArray, nodeIdArray)
  }*/

  def getTriple(e: Array[String]): String = {
    val triple: String = {
      if (e(3).toInt == 1) {
        e(2) + "\t" + e(1) + "\t" + e(0)
      } else {
        e(0) + "\t" + e(1) + "\t" + e(2)
      }
    }
    triple
  }

  def display(vertexArray: Array[VertexClass], nodeIdArray: Array[String]): Unit = {
    for (i <- vertexArray.indices) {
      val vertex = vertexArray(i)
            print(vertex.nodeID + ", ")
            print(nodeIdArray(vertex.nodeID.toInt) + ", ")
            print(vertex.supernode+"\t, ")
      print(vertex.nodeAttr)
            print(vertex.selfConn+", ")
            print(vertex.edgeList)
      println()
    }
  }

  def readEdge(urlString: String, fileType: String, nodeIdMap: HashMap[String, Int]): Array[Array[String]] = {
    println(urlString)
    //    val urlArray = urlString.split(";")
    val urlArray: Array[File] = {
      if (fileType.equals("file")) {
        Array(new File(urlString))
      } else {
        getFileName(new File(urlString))
      }
    }
    //构建邻接表，保存样式：nodeid characteristic;predicate;nodeid {数据类型：String"\t"inEdge(0)/outEdge(1);String;String}
    val vertexMap: HashMap[String, Set[String]] = HashMap()
    val nodeIdArrayBuffer: ArrayBuffer[String] = ArrayBuffer()
    var edgeLength = 0
    for (url <- urlArray) {
      val file = Source.fromFile(url)
      //从文件按行读入数据
      for (line <- file.getLines()) {
        edgeLength += 1
        /*var subject = ""
        var predicate = ""
        var Object = ""
        if(line.contains("\"@en")){
          val lineSplit: Array[String] = line.split("\t\"")
          val subjectPredicate = lineSplit(0).split("\t")
          subject = subjectPredicate(0)
          predicate = subjectPredicate(1)
          Object = "\"" + lineSplit(1)
        }
        else {
          val lineSplit = line.split("\t")
          subject = lineSplit(0)
          predicate = lineSplit(1)
          Object = lineSplit(2)
        }*/
        //        if(!line.contains("@") && !line.contains("#") && !line.contains("unknown:namespace")) {
        if (!line.contains("unknown:namespace") && line.charAt(0) != '#') {
          //对每行数据进行拆分
          val processedSplit: Array[String] = line.split("\t")
          val subject: String = processedSplit(0)
          val predicate = processedSplit(1)
          /*//BSBM
          val predicate: String = {
            val len = processedSplit(1).length
            if (processedSplit(1).contains("http://purl.org/dc/elements/1.1/")) {
              processedSplit(1).substring("<http://purl.org/dc/elements/1.1/".length, len - 1)
            } else if (processedSplit(1).contains("http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/")) {
              processedSplit(1).substring("<http://www4.wiwiss.fu-berlin.de/bizer/bsbm/v01/vocabulary/".length, len - 1)
            } else if (processedSplit(1).contains("http://xmlns.com/foaf/0.1/")) {
              processedSplit(1).substring("<http://xmlns.com/foaf/0.1/".length, len - 1)
            }
            else {
              processedSplit(1).substring(processedSplit(1).indexOf('#') + 1, len - 1)
            }
          }*/
          val Object: String = {
            if (processedSplit(2).endsWith(" .")) {
              processedSplit(2).substring(0, processedSplit(2).length - 2)
            } else {
              processedSplit(2)
            }
          }

          /*nodeIdSet.add(subject)
          nodeIdSet.add(Object)*/
          if (vertexMap.contains(subject)) {
            vertexMap(subject) += Object + ";" + predicate + ";0"
          }
          else {
            vertexMap.put(subject, Set(Object + ";" + predicate + ";0"))
          }
          //无向图添加另外一条边
          if (vertexMap.contains(Object)) {
            vertexMap(Object) += subject + ";" + predicate + ";1"
          } else {
            vertexMap.put(Object, Set(subject + ";" + predicate + ";1"))
          }
        }
      }
      file.close()
    }
    println(s"initial edge count: $edgeLength")

    //构建节点结构
    val nodeAttributeSet = Set("ub:name", "ub:emailAddress", "ub:telephone")
    val vertexArray: ArrayBuffer[String] = ArrayBuffer()
    for ((key, set) <- vertexMap) {
      val neighborList = set.toList
      var nodeAttribute: String = ""
      var edgeList = ""
      for (neighbor <- neighborList) {
        val edgeData = neighbor.split(";")
        //        if (!edgeData(1).equals("rdf:type")) {
        if (!edgeData(1).contains("type")) {
          edgeList += s"${edgeData(0)}\t${edgeData(1)}\t1\t1\t${edgeData(2)}\t,\t"
        }
        /*else if (edgeData(2).equals("1") && nodeAttributeSet.contains(edgeData(1))) { //为name,email,telephone结点添加属性
          nodeAttribute = edgeData(1) + " , "
        }*/
        else if (edgeData(2).equals("0") && (edgeData(1).contains("type"))) { //剪枝————将边值（predicate）为type的边的谓词（object）放入节点（subject）属性中，边属性为rdf:type且边类型为1的转化为节点属性，不保存边
          typeString = edgeData(1)
          nodeAttribute += edgeData(0) + " , "
        }
      }
      //去除属性为rdf:type且边类型为1的边
      if (edgeList.nonEmpty || !nodeAttribute.equals("")) {
        nodeIdArrayBuffer += key
        nodeIdMap.put(key, nodeIdArrayBuffer.size - 1)
        val vertex: String = s"$key;$key;$nodeAttribute;1;0;$edgeList"
        //        nodeIdArray += key //给节点分配唯一id(标号)
        vertexArray += vertex
      }
    }
    //    val vertexArraySorted: ArrayBuffer[String] = vertexArray.sortBy(v => v.split(";")(0).toInt)(Ordering.Int)//按照ID排序
    //    val vertexArraySorted: ArrayBuffer[String] = vertexArray.sortBy(v =>
    //      (v.split(";")(5).split(",").size, v.split(";")(0).toInt))(Ordering.Tuple2(Ordering.Int, Ordering.Int))//按照degree排序
    Array(vertexArray.toArray, nodeIdArrayBuffer.toArray)
  }

  def getFileName(file: File): Array[File] = {
    //此处读取.nt and .md文件
    //    val files = file.listFiles().filter(! _.isDirectory).filter(t => t.toString.endsWith(".nt") || t.toString.endsWith(".md"))
    val files = file.listFiles().filter(!_.isDirectory).filter(t => t.toString.endsWith(".nt") || t.toString.endsWith(".ttl"))
    //    files ++ file.listFiles().filter(_.isDirectory).flatMap(getFileName)
    files
  }

  def main(args: Array[String]): Unit = {
    //    val vertexArray: Array[String] = readEdge("./datasets/Email-Enron.txt")
    //val vertexArray: Array[String] = readEdge("./edges.txt")
    //    mergeOneDegreeNode(vertexArray, superNodes)
    //    superNodes.foreach(println)
    //    vertexArray.foreach(println)

    //    //文件写入
    //    val writer = new PrintWriter(new File("./datasets/vertex-Enron.txt"))
    //    for(v <- vertexArrayEdge)
    //      writer.println(v)
    //    writer.close()
  }
}
