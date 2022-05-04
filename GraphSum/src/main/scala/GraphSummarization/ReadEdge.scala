package GraphSummarization

import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.io.Source

object ReadEdge {
/*
* Vertex Structure:
* nodeID: ownerID, attribute, size, selfConn
*         edgeList: relation, size, conn
*
* 节点集为Array[String]
* 每一个节点结构为："nodeID;ownerID;attribute;size;selfConn;edgeList"
* edgeList中每个边用逗号隔开，同一边中的各属性用空格隔开: "neighborID relation size conn"
*
*/


  /*ys
  读节点的边，并将边以边的id排序保存
   */

  def readEdge(url: String): Array[String] ={
    val file = Source.fromFile(url)

    //构建节点结构
    val vertexMap: HashMap[Int, String] = HashMap()

    for(line <- file.getLines()){
      if(!line.contains("#")){
        val e: Array[String]  = line.split("\t")

        val nodeID = e(0).toInt
        val attribute = 0 //节点属性默认为0
        val relation = 0 //边关系
        val edge = s"${e(1)} ${relation} 1 1,"

        val vertex: String = s"${nodeID};${nodeID};${attribute};1;0;${edge}"

        if(vertexMap.contains(nodeID)){
          vertexMap(nodeID) += edge
        }
        else{
          vertexMap.put(nodeID, vertex)
        }
      }
    }

    val vertexArray: ArrayBuffer[String] = ArrayBuffer()

    for((_, value) <- vertexMap){
      vertexArray += value
    }

    val vertexArraySorted: ArrayBuffer[String] = vertexArray.sortBy(v => v.split(";")(0).toInt)(Ordering.Int)//排序
    vertexArraySorted.toArray
  }

  def main(args: Array[String]): Unit = {
    println("class: ReadEdge")
    val vertexArraySorted = readEdge("./dataset/Email-Enron.txt")
    vertexArraySorted.foreach(println)
  }
}
