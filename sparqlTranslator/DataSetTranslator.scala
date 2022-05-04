package sparqlTranslator

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import scala.collection.mutable
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._

object DataSetTranslator {

  val _sparkSession: SparkSession = Settings.sparkSession
  private val _sparkContext = _sparkSession.sparkContext
  private val prefixMap = HashMap[String, String]()
  private var variableSet: Set[String] = Set()
//  private var _sqlArray = ArrayBuffer[String]()
  private val rdfsMap = Helper.lubmSubClass()
  //table name map
  private val _tnMap: mutable.HashMap[String, Int] = mutable.HashMap()
  //supernode Map
  private val constantMap: Map[String, Int] = readSupernodesFile()
  //prefix文件
  //  val _prefixes = Helper.broadcastPrefixes(_sparkSession)

  private val statVPMap = readStatisticsFile(Settings.s_VPFile)
  private val statSSMap = readStatisticsFile(Settings.s_SSFile)
  private val statOSMap = readStatisticsFile(Settings.s_OSFile)
  private val statOOMap = readStatisticsFile(Settings.s_OOFile)
  private val statArray = Array(statOSMap, statSSMap, statOOMap, statVPMap)


  def runTranslator(spqFile: String): Unit = {

    //获得sparql文件数组
    val spqFileArray = {
      //判断输入的为文件还是文件夹，获得
      if (Helper.isFile(spqFile)) Array(spqFile)
      else Helper.getSubDirs(spqFile)
    }

    val _sqlArray = ArrayBuffer[String]()

    for (file <- spqFileArray) {
      Settings.setSpqFile(file)
      Settings.setFlag(Settings.oriFlag)

      val Array(sgSortedArray, igSortedArray) = readSPQFile()
      //              println("sgSortedArray")
      //          sgSortedArray.foreach(println)
      //              println("igSortedArray")
      //      igSortedArray.foreach(println)
      if (Settings.flag != -1) {
        val igSqlArray = getSqlArray(igSortedArray)

        val iniSqlString = getIniSqlString(igSqlArray)
        val sgSqlArray = {
          if (Settings.flag == 1) getSqlArray(sgSortedArray)
          else Array[String]()
        }
        val sumSqlString = {
          if (Settings.flag == 1) getSumSqlString(sgSqlArray) + "------\n"
          else ""
        }
        val queryString = sumSqlString + iniSqlString
        //    println(queryString)
        var sqlString = s"<<<<<<${getQueryName()}--"
        sqlString += {
          if (Settings.flag == 1) {
            "summary>>>>>>\n" +
              sgSqlArray.map(getTableName).distinct.map(x => Settings.replaceSymbol(x)).mkString("\t") +
              "\n------\n" + {
              igSqlArray.map(x => x.split("----")(0))
                .map(x => x.split(" "))
                .map(x => getSuperNodeForArray(x(0)) + " " + x(1) + " " + {
                  if (!x(1).endsWith("type") || !rdfsMap.contains(x(2))) {
                    getSuperNodeForArray(x(2))
                  } else {
                    rdfsMap(x(2)).map(getSuperNode).mkString(",")
                  }
                })
                .mkString(", ")
            }

          }
          else {
            "ordinary>>>>>>\n" +
              igSqlArray.map(getTableName).distinct.map(x => Settings.replaceSymbol(x)).mkString("\t")
          }
        }
        //    sqlString += "\n" + sgSqlArray.map(_.split(" ")(1))
        //      .map(x => Settings.replaceSymbol(x)).toSet.mkString("\t")
        //    if (Settings.flag == 1) sqlString += "\n------\n" +
        //      igSqlArray.map(x => x.split("----")(0))
        //      .map(x => x.split(" ")).map(x => x(0)+" "+Settings.replaceSymbol(x(1))+" "+x(2)).mkString(",")
        sqlString += "\n++++++\n" + queryString
//        println(sqlString)
        _sqlArray += sqlString
      }
      else {
        println(s"According to statistics, ${getQueryName()} query value is 0 .")
//        Settings.flag = Settings.oriFlag
      }
    }
    Helper.saveFile(_sqlArray.toArray, Settings.sqlFile)
  }

  def getTableName(string: String): String = {
    val sSplit = string.split("----")
    val stringSplit = sSplit(0).split("<<>>")
    //    println("getTableName: "+string)
    if (sSplit(1).split("\t")(0) == "vp") {
      stringSplit(0).split(" ")(1)
    } else if (sSplit(1).split("\t")(0) == "os") {
      stringSplit(0).split(" ")(1) + "<<>>" + stringSplit(1).split(" ")(1) + "----os"
    } else {
      getMergeString(stringSplit(0).split(" ")(1), stringSplit(1).split(" ")(1), "ss") + "----" + sSplit(1).split("\t")(0)
    }
  }

  def getPartName(v: String): String = {
    v.replaceAll("<", "_L_")
      .replaceAll(">", "_B_")
      .replaceAll(":", "__")
      .replaceAll("~", "W_W") //波浪线(wave)
      .replaceAll("-", "H_H") //水平线
      .replaceAll("/", "D_D") //斜线(diagonal)
      .replaceAll("\\.", "P_P") //点
      .replaceAll("#", "S_S") //符号#
  }

  def getQueryName(): String = {
    Settings.spqFile.split("\\\\").flatMap(_.split("/")).last.split("\\.")(0)
  }

  def isFile(filePath: String): Boolean = {
    filePath.endsWith(".txt") || filePath.endsWith(".rq")
  }

  //read sparql file
  def readSPQFile(): Array[Array[String]] = {
    val file = Helper.readFile(Settings.spqFile)
    val selectLines = file.filter(_.startsWith("SELECT"))
    //解析得到三元组，并根据统计文件statMap进行排序
    val triples = file.filter(_.trim.endsWith(" ."))
      //      .map(x => x.substring(0, x.length-2))
      .map(_.trim.stripSuffix(" ."))
      .map(x => x.trim)
    //      .map(r => Helper.parseDataset(r.mkString, _prefixes))
    //    triples.foreach(println)

    var sortedTriples = triples
      .map(x => {
        val xSplit = x.replaceAll("\t", " ").split(" ").filter(y => y != "")
        val xMK = xSplit.mkString(" ")
        xMK + "----vp\t" + {
          val vL = constantLocation(xMK)
          if(statVPMap.contains(xSplit(1))) {
            statVPMap(xSplit(1)).split(" ")(0).toFloat + "\t" + vL
          } else {
            Settings.setFlag(-1)
            0 + "\t" + vL
          }

          //          if (vL == 1 && rdfsMap.contains(xSplit(2)))
          //            statVPMap(xSplit(1)).split(" ")(vL).toFloat * rdfsMap(xSplit(2)).size +"\t"+ vL
          //          else if (vL == 2 && rdfsMap.contains(x.split(" ")(0)))
          //            statVPMap(xSplit(1)).split(" ")(vL).toFloat * rdfsMap(xSplit(0)).size +"\t"+ vL
          //          else
          //            statVPMap(xSplit(1)).split(" ")(vL) +"\t"+ vL
        }
      })
      .sortBy(x => x.split("\t")(1).toFloat)
      .sortBy(x => x.split("\t")(2) == "0")
//    val t1Sorted: Array[String] = sortedTriples.filter(_.split(" ")(1).contains("type"))
//    val t2Sorted = sortedTriples.filter(!_.split(" ")(1).contains("type")).map(x => x.split("----")(0))
    val igSortedArray = sortedTriples.map(_.split("\t")).map(x => Array(x(0), x(2)).mkString("\t"))
    var t1Sorted: Array[String] = Array[String]()
    if (Settings.dataType == 0) {
      t1Sorted = sortedTriples.filter(_.split(" ")(1).contains("type"))
      sortedTriples = sortedTriples.filter(!_.split(" ")(1).contains("type")).map(x => x.split("----")(0))
    }


    //    println("igSortedArray: ")
    //    igSortedArray.foreach(println)
    variableSet = getVariableArray(selectLines)

    val sgSortedArray = if (igSortedArray.length > 2 && sortedTriples.length > 1 && Settings.flag == 1) {
      //    if(Settings.flag == 1) {
      //      if(t2Sorted.length > 1) {
      val varGroup = sortedTriples.map(_.split(" "))
        .flatMap(x => Array(x(0), x(2)))
        .map(x => (x, sortedTriples.filter(y => {
          val ySplit = y.split(" ")
          ySplit(0) == x || ySplit(2) == x
        }).toSet))
        .toMap

      val tripleGroup = sortedTriples.map(_.split(" "))
        .map(x => (s"${x(0)} ${x(1)} ${x(2)}", varGroup(x(0)) ++ varGroup(x(2)) - s"${x(0)} ${x(1)} ${x(2)}"))
        .map(x => (x._1, x._2.map(y => getValuedTriple(x._1, y)).toList
          .sortBy(_.split("\t")(1).toFloat)
          .sortBy(_.split("\t")(2) == "0")
        ))
//      println("tripleGroup")
//      tripleGroup.foreach(println)
      var tripleSet: Set[String] = Set[String]()
      val triple: Array[String] = tripleGroup.map(x => {
        if (!tripleSet.contains(x._1)) {
          val tri = x._2.filter(y => !tripleSet.contains(y.split("----")(0)))
          if (tri.isEmpty) {
            //          val tSplit = x._2(0).split("----")(0)
            tripleSet += x._1
            val x2Split = x._2.head.split("----")
            if (x2Split(1).split("\t")(0) == "so") {
              x2Split(0) + "<<>>" + x._1 + "----" + x2Split(1).replace("so", "os")
            } else {
              x._1 + "<<>>" + x._2.head
            }
          } else {
            //          val tSplit = tri.head.split("----")
            tripleSet ++= Set(x._1, tri.head.split("----")(0))
            val triSplit = tri.head.split("----")
            if (triSplit(1).split("\t")(0) == "so") {
              triSplit(0) + "<<>>" + x._1 + "----" + triSplit(1).replace("so", "os")
            } else {
              x._1 + "<<>>" + tri.head
            }
          }
        }
        else {
          ""
        }
      })
        .filter(_ != "")
      (t1Sorted ++ triple).sortBy(_.split("\t")(1).toFloat).sortBy(x => x.split("\t")(2) == "0")
        .map(_.split("\t"))
        .map(x => x(0) + "\t" + x(2))
      //      println("测试输出： sgSortedArray: ")
      //      sgSortedArray.foreach(println)
      //      println("测试输出： igSortedArray: ")
      //      igSortedArray.foreach(println)
    } else {
      //      if(Settings.flag == 1 && igSortedArray.filter())
      //      Settings.setFlag(0)
      igSortedArray
      //      Array(igSortedArray, igSortedArray)
    }
    val igArray = {
      val tmpArray = igSortedArray.filter(x => !(x.split(" ")(1).contains("type") && x.split("\t")(1) != "0"))
      if (Settings.flag == 1 && tmpArray.length > 0) {
        tmpArray
      } else {
        igSortedArray
      }
    }
    Array(sgSortedArray, igArray)
    //    val tripleLines = Test.RemovePrefix.removePrefix(tripleLinesTemp, " ")
    /*//测试输出
    println("selectLine: ")
    selectLines.foreach(println)
    println("tripleLine: ")
    tripleLines.foreach(println)
    println("variableSet: ")
    variableSet.foreach(x => print(x + "\t"))
    println()*/
    //    val sqlSet = getSqlArray(tripleLines)
    /*val prefixRDD: RDD[String] = file.filter(_.contains("PREFIX"))
      .map(_.substring(7))
    val queryStat = file.filter(!_.contains("PREFIX"))
    getPrefixArray(prefixRDD)
    queryStat.foreach(println)*/
  }

  def getValuedTriple(str1: String, str2: String): String = {
    val str1Split = str1.split(" ")
    val str2Split = str2.split(" ")
    val x = getSameLocation(str1, str2)
    //    println("x(0): "+x(0))
    var valuedString = str2 + "----"
    valuedString += {
      if (x(0) % 2 == 0) {
        if (x(0) == 2) {
          //          "ss\t" + statArray(x(0)/2)(getMergeString(str1Split(1), str2Split(1), "ss") ).split(" ")(0) +"\t"+ x(1)
          "ss\t" + getStatValue(x(0) / 2, getMergeString(str1Split(1), str2Split(1), "ss")) + "\t" + x(1)
        } else {
          //          "oo\t" + statArray(x(0)/2)(getMergeString(str1Split(1), str2Split(1), "ss") ).split(" ")(0) +"\t"+ x(1)
          "oo\t" + getStatValue(x(0) / 2, getMergeString(str1Split(1), str2Split(1), "ss")) + "\t" + x(1)
        }
      }
      else if (x(0) == 1) {
        //        "os\t"+ statArray(0)(str1Split(1)+"<<>>"+str2Split(1)).split(" ")(0) +"\t"+ x(1)
        "os\t" + getStatValue(0, str1Split(1) + "<<>>" + str2Split(1)) + "\t" + x(1)
      } else {
        "so\t" + getStatValue(0, str2Split(1) + "<<>>" + str1Split(1)) + "\t" + x(1)
      }
    }
    //    println(s"str1: $str1, str2: $str2, valueString: $valuedString")
    valuedString

  }

  def readStatisticsFile(filePath: String): Map[String, String] = {
    _sparkContext.textFile(filePath)
      .collect()
      .filter(!_.startsWith("#"))
      .map(_.split("\t"))
      .map(x => (x(0), x(1)))
      .toMap
  }

  def getMergeString(str1: String, str2: String, sign: String): String = {
    if (str1.compareTo(str2) < 1 || sign == "os") {
      str1 + "<<>>" + str2
    } else {
      str2 + "<<>>" + str1
    }
  }

  def getStatValue(index: Int, typeString: String): String = {
    //原
//    statArray(index)(typeString).split(" ")(0)
    if (statArray(index).contains(typeString)) {
      statArray(index)(typeString).split(" ")(0)
    } else {
      println("The statistic value of typeString: " + typeString + " is 0. ")
      //根据统计值为0，所以直接返回查询结果为0
      Settings.setFlag(-1)
      "0"
    }
  }

  //  def readSPQFile (): Unit ={
  //    val file = _sparkSession.read.text(Settings.spqFile).collect().map(_.get(0).toString)
  ////    file.foreach(println)
  //    val statMap = _sparkSession.read.text(Settings.sgsFile)
  //      .collect()
  //      .map(_.get(0).toString)
  //      .filter(!_.startsWith("#"))
  //      .map(_.split("\t"))
  //      .map(x => (x(0),x(1)+" "+x(2)+" "+x(3)))
  //      .toMap
  //    val selectLines = file.filter(_.startsWith("SELECT"))
  //    //解析得到三元组，并根据统计文件statMap进行排序
  //    val triples = file.filter(_.endsWith(" ."))
  //      .map(x => x.substring(0, x.length-2))
  //      .map(x => x.split("\t")(1))
  //      .map(x => {
  //        x + "\t" + {
  //          val vL = varLocation(x)
  //          val xSplit = x.split(" ")
  //          if (vL == 1 && rdfsMap.contains(xSplit(2)))
  //            statMap(xSplit(1)).split(" ")(vL).toFloat * rdfsMap(xSplit(2)).size
  //          else if (vL == 2 && rdfsMap.contains(x.split(" ")(0)))
  //            statMap(xSplit(1)).split(" ")(vL).toFloat * rdfsMap(xSplit(0)).size
  //          else
  //            statMap(xSplit(1)).split(" ")(vL)
  //        }
  //      })
  ////      .sortBy(x => x.split("\t")(1).toFloat)
  ////    triples.foreach(println)
  //      tripleLines = triples.map(x => x.split("\t")(0))
  //    variableSet = getVariableArray(selectLines)
  ////    val tripleLines = Test.RemovePrefix.removePrefix(tripleLinesTemp, " ")
  //    /*//测试输出
  //    println("selectLine: ")
  //    selectLines.foreach(println)
  //    println("tripleLine: ")
  //    tripleLines.foreach(println)
  //    println("variableSet: ")
  //    variableSet.foreach(x => print(x + "\t"))
  //    println()*/
  ////    val sqlSet = getSqlArray(tripleLines)
  //    /*val prefixRDD: RDD[String] = file.filter(_.contains("PREFIX"))
  //      .map(_.substring(7))
  //    val queryStat = file.filter(!_.contains("PREFIX"))
  //    getPrefixArray(prefixRDD)
  //    queryStat.foreach(println)*/
  //  }
  /**
   * t1: type(os: 1, so: 3, They are odd numbers(OS); ss: 2; oo: 4)
   * t2: constantLocation(If contains no constant: 0; If contains constant: 1, 2 and 3 denote on X, Y and Z respectively)
   *
   * os(1): s1 (o1) (s2) o2
   * 1   2        3
   * ss(2): (s1) o1 (s2) o2
   * 1   2       3
   * so(3): (s1) o1 s2 (o2)
   * 3  1   2
   * oo(4): s1 (o1) s2 (o2)
   * 1   2   3
   */
  def getSameLocation(str1: String, str2: String): Array[Int] = {
    val str1Split = str1.split(" ")
    val str2Split = str2.split(" ")
    val loc1 = constantLocation(str1)
    val loc2 = constantLocation(str2)
    val t1 = if (str1Split(2) == str2Split(0)) 1
    else if (str1Split(0) == str2Split(0)) 2
    else if (str1Split(0) == str2Split(2)) 3
    else if (str1Split(2) == str2Split(2)) 4
    else {
      println(s"$str1, $str2, getSameLocation: -1")
      -1
    }
    val t2 = {
      //two case:(1) os, ss(pre>0), oo(pre<0); (2) so, ss(pre<0), oo(pre<0)
      if (t1 == 1 || (t1 % 2 == 0 && str1Split(1).compareTo(str2Split(1)) < 0)) {
        if (loc1 != 0) {
          loc1
        } else if (loc2 != 0) {
          3
        } else {
          0
        }
      } else {
        if (loc2 != 0) {
          loc2
        } else if (loc1 != 0) {
          3
        } else {
          0
        }
      }
    }
    Array(t1, t2)
  }

  //return constant location
  def constantLocation(triple: String): Int = {
    val tSplit = triple.split(" ")
    if (!tSplit(0).startsWith("?")) 1
    else if (!tSplit(2).startsWith("?")) 2
    else 0
  }


  def getPrefixArray(prefixRDD: RDD[String]): Unit = {
    for (pf <- prefixRDD) {
      val pfSplit = pf.split(": ")
      //      pfSplit.foreach(println)
      prefixMap.put(pfSplit(0), pfSplit(1))
    }
  }

  def getVariableArray(sLArray: Array[String]): Set[String] = {
    sLArray.flatMap(_.split(", ").flatMap(_.split(" ")).filter(_.charAt(0) == '?')).toSet
  }

  def getSqlArray(tripleArray: Array[String]): Array[String] = {
    //    val sqlMap: mutable.HashMap[String, List[String]] = mutable.HashMap()
    var sqlArray: ArrayBuffer[String] = ArrayBuffer()
    var varSets: Set[String] = Set()
    varSets = varSets ++ getTripleVarSet(tripleArray(0))
    sqlArray += tripleArray(0)
    val length = tripleArray.length
    for (i <- 1 until length) {
      breakable(
        for (j <- i until length) {
          val s = getTripleVarSet(tripleArray(j))
          if (s.intersect(varSets).nonEmpty) {
            sqlArray += tripleArray(j)
            varSets = varSets.union(s)
            for (k <- j until i by -1) {
              tripleArray(k) = tripleArray(k - 1)
            }
            break()
          }
        }
      )
    }
    //    println("sqlArray: ")
    //    sqlArray.foreach(println)
    val tem = sqlArray.toArray.map(_.split("----"))
      .map(x => (x(0).split("<<>>"), x(1)))
      .map(x => {
        val str1 = if (x._1.size > 1) {
          if (x._2.split("\t")(0) != "os" && x._1(0).split(" ")(1).compareTo(x._1(1).split(" ")(1)) > 0) {
            x._1(1) + "<<>>" + x._1(0)
          } else {
            x._1(0) + "<<>>" + x._1(1)
          }
        } else {
          x._1(0)
        }
        str1 + "----" + x._2
      })
    //    println("tem: ")
    //    tem.foreach(println)
    tem
    //    val tranSql = sqlArray.toArray.map(x => )
    //    tranSql
  }

  def getTripleVarSet(triple: String): Set[String] = {
    triple.split("----")(0).split("<<>>").flatMap(_.split(" ")).filter(_.startsWith("?")).toSet
  }

  def readSupernodesFile(): Map[String, Int] = {
    import _sparkSession.implicits._
    if (Settings.flag == 1) {
      _sparkSession.read.parquet(Settings.nodeIdMap)
        .map(_.mkString.split("---"))
        .flatMap(node => node(1).split("__").map(sn => (sn -> node(0).toInt)))
        .collect()
        .toMap
//      val nodeIdMap = _sparkContext.textFile(Settings.nodeIdMap).map(_.split("\t"))
//      val maxValue = nodeIdMap.map(_(0).toInt).max()
//      val snArray: Array[Int] = new Array[Int](maxValue + 1)
//      var sn: Int = 0
//      val snFile = _sparkContext.textFile(Settings.snFile).map(_.split("____"))
//      for(node <- snFile.collect()) {
//        sn = node(0).toInt
//        node(1).split(",").foreach(n => snArray(n.toInt) = sn)
//      }
//      nodeIdMap.map(x => x(1) -> snArray(x(0).toInt))
//        .collect().toMap
    } else {
      Map[String, Int]()
    }
  }

  def getSumSqlString(triple: Array[String]): String = {
    //    val constantMap: mutable.HashMap[String, String] = mutable.HashMap()
    var summarySql = ""
    val varMap: mutable.HashMap[String, String] = mutable.HashMap()
    for (index <- triple.indices) {
      if (index != 0) {
        summarySql += "JOIN \n"
      }
      val temMap: mutable.HashMap[String, String] = mutable.HashMap()
      val tripleSplit = triple(index).split("----")
      val ts1 = tripleSplit(0).split("<<>>").map(_.split(" "))
      //      println(triple(index))
      summarySql += getSqlString(triple(index), 1) + " tab" + index + "\n"
      var joinSet: Set[String] = Set()
      for (i <- ts1.indices) {
        for (j <- Array(0, 2) if ts1(i)(j).startsWith("?")) {
          if (varMap.contains(ts1(i)(j))) {
            joinSet += s"${varMap(ts1(i)(j))}=tab$index.${ts1(i)(j).substring(1)}"
          } else {
            temMap.put(ts1(i)(j), s"tab$index.${ts1(i)(j).substring(1)}")
          }
        }
        //        if(ts1(i)(0).startsWith("?") && varMap.contains(ts1(i)(0))) {
        //          joinSet += s"${varMap(ts1(i)(0))}=tab$index.${ts1(i)(0).substring(1)}"
        //        }else {
        //          temMap.put(ts1(i)(0), s"tab$index.${ts1(i)(0).substring(1)}")
        //        }
        //        if(ts1(i)(2).startsWith("?") && varMap.contains(ts1(i)(2))){
        //          joinSet += s"${varMap(ts1(i)(2))}=tab$index.${ts1(i)(2).substring(1)}"
        //        } else {
        //          temMap.put(ts1(i)(2), s"tab$index.${ts1(i)(2).substring(1)}")
        //        }
      }
      if (joinSet.nonEmpty) {
        summarySql += " ON(" + joinSet.mkString(" AND ") + ")\n"
      }
      varMap ++= temMap

      //      if (tripleSplit(0).startsWith("?") && tripleSplit(2).startsWith("?")) {
      //        if (index != 0) {
      //          summarySql += " JOIN "
      //          initialSql += " JOIN "
      //        }
      //        val sql =  "(SELECT sub AS " + tripleSplit(0).substring(1) +
      //          ", obj AS " + tripleSplit(2).substring(1) +
      //          " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
      //          ") tab" + index + "\n"
      //        summarySql += sql
      //        initialSql += sql
      //      }
      //      else {
      //        if (index != 0) {
      //          summarySql += " LEFT SEMI JOIN "
      //          initialSql += " LEFT SEMI JOIN "
      //        }
      //        if (tripleSplit(0).startsWith("?")) {
      //          val constant = {
      //            if (Settings.flag == 1 && !tripleSplit(1).contains("type")) "obj='"+constantMap(tripleSplit(2))+"'"
      //            else if (rdfsMap.contains(tripleSplit(2))) s"obj IN ('${rdfsMap(tripleSplit(2)).mkString("','")}')"
      //            else "obj='"+tripleSplit(2)+"'"
      //          }
      //          summarySql += "(SELECT sub AS " + tripleSplit(0).substring(1) +
      //            " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
      //            " WHERE " + constant +
      //            ") tab" + index + "\n"
      //          initialSql += "(SELECT sub AS " + tripleSplit(0).substring(1) +
      //            " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
      //            ") tab" + index + "\n"
      //        }
      //        else {
      //          val constant = {
      //            if (Settings.flag == 1) constantMap(tripleSplit(0))
      //            else tripleSplit(0)
      //          }
      //          summarySql += "(SELECT obj AS " + tripleSplit(2).substring(1) +
      //            " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
      //            " WHERE sub='" + constant +
      //            "') tab" + index + "\n"
      //          initialSql += "(SELECT obj AS " + tripleSplit(2).substring(1) +
      //            " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
      //            ") tab" + index + "\n"
      //        }
      //      }
      //      val js = if (varMap.contains(tripleSplit(0)) && varMap.contains(tripleSplit(2))) {
      //        s" ON (${varMap(tripleSplit(0))}=tab${index}.${tripleSplit(0).substring(1)}" +
      //          s" AND ${varMap(tripleSplit(2))}=tab$index.${tripleSplit(2).substring(1)})\n"
      //      } else if (varMap.contains(tripleSplit(0))) {
      //        varMap.put(tripleSplit(2), s"tab$index.${tripleSplit(2).substring(1)}")
      //        s" ON (${varMap(tripleSplit(0))}=tab${index}.${tripleSplit(0).substring(1)})\n"
      //      } else if (varMap.contains(tripleSplit(2))) {
      //        varMap.put(tripleSplit(0), s"tab$index.${tripleSplit(0).substring(1)}")
      //        s" ON (${varMap(tripleSplit(2))}=tab$index.${tripleSplit(2).substring(1)})\n"
      //      } else {
      //        varMap.put(tripleSplit(2), s"tab$index.${tripleSplit(2).substring(1)}")
      //        varMap.put(tripleSplit(0), s"tab$index.${tripleSplit(0).substring(1)}")
      //        ""
      //      }
      //      summarySql += js
      //      initialSql += js
    }
    summarySql = "SELECT " + varMap.values.mkString(", ") + " FROM\n" + summarySql
    //    println("summarySql: "+summarySql)
    summarySql
  }

  def getIniSqlString(triple: Array[String]): String = {
    //    val constantMap: mutable.HashMap[String, String] = mutable.HashMap()
    var initialSql = ""
    val varMap: mutable.HashMap[String, String] = mutable.HashMap()
    //清空table name map
    _tnMap.clear()
    for (index <- triple.indices) {
      //判断是不是第一条语句，第一条语句不需要加JOIN
      if (index != 0) {
        initialSql += "JOIN \n"
      }
      val temMap: mutable.HashMap[String, String] = mutable.HashMap()
      val tripleSplit = triple(index).split("----")
      val ts1 = tripleSplit(0).split("<<>>").map(_.split(" "))
      val f = {
        if (Settings.flag == 1) 2
        else 0
      }
      initialSql += getSqlString(triple(index), f) + " tab" + index + "\n"
      var joinSet: Set[String] = Set()
      for (i <- ts1.indices) {
        if (varMap.contains(ts1(i)(0))) {
          joinSet += s"${varMap(ts1(i)(0))}=tab$index.${ts1(i)(0).substring(1)}"
        } else {
          temMap.put(ts1(i)(0), s"tab$index.${ts1(i)(0).substring(1)}")
        }
        if (varMap.contains(ts1(i)(2))) {
          joinSet += s"${varMap(ts1(i)(2))}=tab$index.${ts1(i)(2).substring(1)}"
        } else {
          temMap.put(ts1(i)(2), s"tab$index.${ts1(i)(2).substring(1)}")
        }
      }
      if (joinSet.nonEmpty) {
        initialSql += " ON(" + joinSet.mkString(" AND ") + ")\n"
      }
      varMap ++= temMap

      //      if (tripleSplit(0).startsWith("?") && tripleSplit(2).startsWith("?")) {
      //        if (index != 0) {
      //          summarySql += " JOIN "
      //          initialSql += " JOIN "
      //        }
      //        val sql =  "(SELECT sub AS " + tripleSplit(0).substring(1) +
      //          ", obj AS " + tripleSplit(2).substring(1) +
      //          " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
      //          ") tab" + index + "\n"
      //        summarySql += sql
      //        initialSql += sql
      //      }
      //      else {
      //        if (index != 0) {
      //          summarySql += " LEFT SEMI JOIN "
      //          initialSql += " LEFT SEMI JOIN "
      //        }
      //        if (tripleSplit(0).startsWith("?")) {
      //          val constant = {
      //            if (Settings.flag == 1 && !tripleSplit(1).contains("type")) "obj='"+constantMap(tripleSplit(2))+"'"
      //            else if (rdfsMap.contains(tripleSplit(2))) s"obj IN ('${rdfsMap(tripleSplit(2)).mkString("','")}')"
      //            else "obj='"+tripleSplit(2)+"'"
      //          }
      //          summarySql += "(SELECT sub AS " + tripleSplit(0).substring(1) +
      //            " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
      //            " WHERE " + constant +
      //            ") tab" + index + "\n"
      //          initialSql += "(SELECT sub AS " + tripleSplit(0).substring(1) +
      //            " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
      //            ") tab" + index + "\n"
      //        }
      //        else {
      //          val constant = {
      //            if (Settings.flag == 1) constantMap(tripleSplit(0))
      //            else tripleSplit(0)
      //          }
      //          summarySql += "(SELECT obj AS " + tripleSplit(2).substring(1) +
      //            " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
      //            " WHERE sub='" + constant +
      //            "') tab" + index + "\n"
      //          initialSql += "(SELECT obj AS " + tripleSplit(2).substring(1) +
      //            " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
      //            ") tab" + index + "\n"
      //        }
      //      }
      //      val js = if (varMap.contains(tripleSplit(0)) && varMap.contains(tripleSplit(2))) {
      //        s" ON (${varMap(tripleSplit(0))}=tab${index}.${tripleSplit(0).substring(1)}" +
      //          s" AND ${varMap(tripleSplit(2))}=tab$index.${tripleSplit(2).substring(1)})\n"
      //      } else if (varMap.contains(tripleSplit(0))) {
      //        varMap.put(tripleSplit(2), s"tab$index.${tripleSplit(2).substring(1)}")
      //        s" ON (${varMap(tripleSplit(0))}=tab${index}.${tripleSplit(0).substring(1)})\n"
      //      } else if (varMap.contains(tripleSplit(2))) {
      //        varMap.put(tripleSplit(0), s"tab$index.${tripleSplit(0).substring(1)}")
      //        s" ON (${varMap(tripleSplit(2))}=tab$index.${tripleSplit(2).substring(1)})\n"
      //      } else {
      //        varMap.put(tripleSplit(2), s"tab$index.${tripleSplit(2).substring(1)}")
      //        varMap.put(tripleSplit(0), s"tab$index.${tripleSplit(0).substring(1)}")
      //        ""
      //      }
      //      summarySql += js
      //      initialSql += js
    }
    val varString = "SELECT DISTINCT " + variableSet.map(varMap(_)).mkString(", ") + " FROM\n"
    initialSql = varString + initialSql
    initialSql
    //    if (Settings.flag == 1) {
    //      initialSql + "------\n" + initialSql
    //    } else {
    //      initialSql
    //    }
  }

  /**
   * f = 0 : flag = 0
   * f = 1: flag = 1 and (father function is getSumSqlString)
   * f = 2: flag = 1 and (father function is getIniSqlString)
   * */
  def getSqlString(string: String, f: Int): String = {
    val tripleSplit = string.split("----")
    val ts1 = tripleSplit(0).split("<<>>").map(_.split(" "))
    val ts2 = tripleSplit(1).split("\t")
    val sql = "(SELECT " + {
      if (ts2(0) == "vp") {
        if (ts2(1) == "0") {
          "sub AS " + ts1(0)(0).substring(1) +
            ", obj AS " + ts1(0)(2).substring(1) +
            " FROM " + getTableName(Settings.replaceSymbol(ts1(0)(1)), f) +
            ")"
        } else if (ts2(1) == "1") {
          "obj AS " + ts1(0)(2).substring(1) +
            " FROM " + getTableName(Settings.replaceSymbol(ts1(0)(1)), f) + {
            if (f == 2) {
              //原先的
//              ")"
              s" WHERE sub =' ${ts1(0)(0)} ')"
            } else {
              s" WHERE sub =' ${getSuperNode(ts1(0)(0))} ')"
            }
          }
        } else {
          "sub AS " + ts1(0)(0).substring(1) +
            " FROM " + getTableName(Settings.replaceSymbol(ts1(0)(1)), f) + {
            if (f == 2) {
              //原先的
//              " )"
              if(ts1(0)(1).endsWith("type")) {
                " )"
              } else {
                " WHERE obj='" + ts1(0)(2) + "')"
              }
            } else {
              if (!ts1(0)(1).contains("type")) {
                if (f == 0) " WHERE obj='" + ts1(0)(2) + "')"
                else " WHERE obj='" + getSuperNode(ts1(0)(2)) + "')"
              }
              else {
                if (rdfsMap.contains(ts1(0)(2))) s" WHERE obj IN ('${rdfsMap(ts1(0)(2)).map(getSuperNode).mkString("','")}') )"
                else " WHERE obj='" + getSuperNode(ts1(0)(2)) + "')"
              }
            }
          }
          //          "sub AS " + ts1(0)(0).substring(1) +
          //            " FROM " + Settings.replaceSymbol(ts1(0)(1)) + {
          //            if (f == 0 && !ts1(0)(1).contains("type")) " WHERE obj='" + constantMap(ts1(0)(2)) + "')"
          //            else if (rdfsMap.contains(ts1(0)(2))) {
          //              if(f != 1) s" WHERE obj IN ('${rdfsMap(ts1(0)(2)).mkString("','")}') )"
          //              else ")"
          //            }
          //            else " WHERE obj='" + ts1(0)(2) + "')"
          //          }
        }
      } else if (ts2(0) == "oo") {
        if (ts2(1) == "0") {
          "X AS " + ts1(0)(0).substring(1) +
            ", Y AS " + ts1(0)(2).substring(1) +
            ", Z AS " + ts1(1)(0).substring(1) +
            " FROM " + Settings.replaceSymbol(getMergeString(ts1(0)(1), ts1(1)(1), ts2(0))) +
            ")"
        } else if (ts2(1) == "1") {
          "Y AS " + ts1(0)(2).substring(1) +
            ", Z AS " + ts1(1)(0).substring(1) +
            " FROM " + Settings.replaceSymbol(getMergeString(ts1(0)(1), ts1(1)(1), ts2(0))) +
            " WHERE X ='" + getSuperNode(ts1(0)(0)) +
            "')"
        } else if (ts2(1) == "2") {
          "X AS " + ts1(0)(0).substring(1) +
            ", Z AS " + ts1(1)(0).substring(1) +
            " FROM " + Settings.replaceSymbol(getMergeString(ts1(0)(1), ts1(1)(1), ts2(0))) +
            " WHERE Y ='" + getSuperNode(ts1(0)(2)) +
            "')"
        } else {
          "X AS " + ts1(0)(0).substring(1) +
            ", Y AS " + ts1(0)(2).substring(1) +
            " FROM " + Settings.replaceSymbol(getMergeString(ts1(0)(1), ts1(1)(1), ts2(0))) +
            " WHERE Z ='" + getSuperNode(ts1(1)(0)) +
            "')"
        }
      } else if (ts2(0) == "ss") {
        if (ts2(1) == "0") {
          "X AS " + ts1(0)(0).substring(1) +
            ", Y AS " + ts1(0)(2).substring(1) +
            ", Z AS " + ts1(1)(2).substring(1) +
            " FROM " + Settings.replaceSymbol(getMergeString(ts1(0)(1), ts1(1)(1), ts2(0))) +
            ")"
        } else if (ts2(1) == "1") {
          "Y AS " + ts1(0)(2).substring(1) +
            ", Z AS " + ts1(1)(2).substring(1) +
            " FROM " + Settings.replaceSymbol(getMergeString(ts1(0)(1), ts1(1)(1), ts2(0))) +
            " WHERE X ='" + getSuperNode(ts1(0)(0)) +
            "')"
        } else if (ts2(1) == "2") {
          "X AS " + ts1(0)(0).substring(1) +
            ", Z AS " + ts1(1)(2).substring(1) +
            " FROM " + Settings.replaceSymbol(getMergeString(ts1(0)(1), ts1(1)(1), ts2(0))) +
            " WHERE Y ='" + getSuperNode(ts1(0)(2)) +
            "')"
        } else {
          "X AS " + ts1(0)(0).substring(1) +
            ", Y AS " + ts1(0)(2).substring(1) +
            " FROM " + Settings.replaceSymbol(getMergeString(ts1(0)(1), ts1(1)(1), ts2(0))) +
            " WHERE Z ='" + getSuperNode(ts1(1)(2)) +
            "')"
        }
      } else {
        if (ts2(1) == "0") {
          "X AS " + ts1(0)(0).substring(1) +
            ", Y AS " + ts1(0)(2).substring(1) +
            ", Z AS " + ts1(1)(2).substring(1) +
            " FROM " + Settings.replaceSymbol(getMergeString(ts1(0)(1), ts1(1)(1), ts2(0))) +
            ")"
        } else if (ts2(1) == "1") {
          "Y AS " + ts1(0)(2).substring(1) +
            ", Z AS " + ts1(1)(2).substring(1) +
            " FROM " + Settings.replaceSymbol(getMergeString(ts1(0)(1), ts1(1)(1), ts2(0))) +
            " WHERE X ='" + getSuperNode(ts1(0)(0)) +
            "')"
        } else if (ts2(1) == "2") {
          "X AS " + ts1(0)(0).substring(1) +
            ", Z AS " + ts1(1)(2).substring(1) +
            " FROM " + Settings.replaceSymbol(getMergeString(ts1(0)(1), ts1(1)(1), ts2(0))) +
            " WHERE Y ='" + getSuperNode(ts1(0)(2)) +
            "')"
        } else {
          "X AS " + ts1(0)(0).substring(1) +
            ", Y AS " + ts1(0)(2).substring(1) +
            " FROM " + Settings.replaceSymbol(getMergeString(ts1(0)(1), ts1(1)(1), ts2(0))) +
            " WHERE Z ='" + getSuperNode(ts1(1)(2)) +
            "')"
        }
      }
    }
    sql
  }

  def getSuperNodeForArray(string: String): String = {
    getSuperNode(string) + {
      if(string.startsWith("?")) {
        ""
      } else {
        "____" + string
      }
    }
  }
  def getSuperNode(string: String): String = {
    if (Settings.flag == 1 && constantMap.contains(string)) {
      constantMap(string).toString
    } else {
      string
    }
  }

  def getTableName(tableName: String, flag: Int): String = {
    if (flag != 2) {
      tableName
    } else if (_tnMap.contains(tableName)) {
      _tnMap(tableName) = _tnMap(tableName) + 1
      tableName + "_" + (_tnMap(tableName) - 1)
    } else {
      _tnMap.put(tableName, 1)
      tableName
    }
  }

  //  def getSummaryString(triple: Array[String]): String = {
  //    //    val constantMap: mutable.HashMap[String, String] = mutable.HashMap()
  //    val constantMap: Map[String, String] = {
  //      if (Settings.flag == 1) {
  //        _sparkSession.read.text(Settings.snFile)
  //          .collect()
  //          .map(x => x.get(0).toString.split("----")(1).split("\t"))
  //          .flatMap(x => x.map(
  //            y => {
  //              if (y.endsWith(" .")) (y.substring(0, y.length-2),x(0).substring(0, x(0).length-2))
  //              else (y, x(0))
  //            }))
  //          .toMap
  //      } else {
  //        Map[String, String]()
  //      }
  //    }
  //    var summarySql = ""
  //    var initialSql = ""
  //    val varMap: mutable.HashMap[String, String] = mutable.HashMap()
  //    for (index <- triple.indices){
  //      val tripleSplit = triple(index).split("----")
  //      if(tripleSplit(1) == "vp") {
  //
  //      }
  //      if (tripleSplit(0).startsWith("?") && tripleSplit(2).startsWith("?")) {
  //        if (index != 0) {
  //          summarySql += " JOIN "
  //          initialSql += " JOIN "
  //        }
  //        val sql =  "(SELECT sub AS " + tripleSplit(0).substring(1) +
  //          ", obj AS " + tripleSplit(2).substring(1) +
  //          " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
  //          ") tab" + index + "\n"
  //        summarySql += sql
  //        initialSql += sql
  //      }
  //      else {
  //        if (index != 0) {
  //          summarySql += " LEFT SEMI JOIN "
  //          initialSql += " LEFT SEMI JOIN "
  //        }
  //        if (tripleSplit(0).startsWith("?")) {
  //          val constant = {
  //            if (Settings.flag == 1 && !tripleSplit(1).contains("type")) "obj='"+constantMap(tripleSplit(2))+"'"
  //            else if (rdfsMap.contains(tripleSplit(2))) s"obj IN ('${rdfsMap(tripleSplit(2)).mkString("','")}')"
  //            else "obj='"+tripleSplit(2)+"'"
  //          }
  //          summarySql += "(SELECT sub AS " + tripleSplit(0).substring(1) +
  //            " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
  //            " WHERE " + constant +
  //            ") tab" + index + "\n"
  //          initialSql += "(SELECT sub AS " + tripleSplit(0).substring(1) +
  //            " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
  //            ") tab" + index + "\n"
  //        }
  //        else {
  //          val constant = {
  //            if (Settings.flag == 1) constantMap(tripleSplit(0))
  //            else tripleSplit(0)
  //          }
  //          summarySql += "(SELECT obj AS " + tripleSplit(2).substring(1) +
  //            " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
  //            " WHERE sub='" + constant +
  //            "') tab" + index + "\n"
  //          initialSql += "(SELECT obj AS " + tripleSplit(2).substring(1) +
  //            " FROM " + Settings.replaceSymbol(tripleSplit(1)) +
  //            ") tab" + index + "\n"
  //        }
  //      }
  //      val joinString = if (varMap.contains(tripleSplit(0)) && varMap.contains(tripleSplit(2))) {
  //        s" ON (${varMap(tripleSplit(0))}=tab${index}.${tripleSplit(0).substring(1)}" +
  //          s" AND ${varMap(tripleSplit(2))}=tab$index.${tripleSplit(2).substring(1)})\n"
  //      } else if (varMap.contains(tripleSplit(0))) {
  //        varMap.put(tripleSplit(2), s"tab$index.${tripleSplit(2).substring(1)}")
  //        s" ON (${varMap(tripleSplit(0))}=tab${index}.${tripleSplit(0).substring(1)})\n"
  //      } else if (varMap.contains(tripleSplit(2))) {
  //        varMap.put(tripleSplit(0), s"tab$index.${tripleSplit(0).substring(1)}")
  //        s" ON (${varMap(tripleSplit(2))}=tab$index.${tripleSplit(2).substring(1)})\n"
  //      } else {
  //        varMap.put(tripleSplit(2), s"tab$index.${tripleSplit(2).substring(1)}")
  //        varMap.put(tripleSplit(0), s"tab$index.${tripleSplit(0).substring(1)}")
  //        ""
  //      }
  //      summarySql += joinString
  //      initialSql += joinString
  //    }
  //    val varString = "SELECT " + variableSet.map(varMap(_)).mkString(", ") + " FROM\n"
  //    initialSql = varString + initialSql
  //    summarySql = varString + summarySql
  //    if (Settings.flag == 1) {
  //      summarySql + "------\n" + initialSql
  //    } else {
  //      summarySql
  //    }
  //  }


}
