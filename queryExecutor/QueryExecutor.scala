/* Copyright Simon Skilevic
 * Master Thesis for Chair of Databases and Information Systems
 * Uni Freiburg
 */

package queryExecutor


import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.catalyst.InternalRow

import collection.mutable.{ArrayBuffer, HashMap, ListBuffer}
import scala.collection.mutable

object QueryExecutor {


  private var _cachedTables = HashMap[String, String]();
  /*private val _sc = Settings.sparkContext
  private val _sqlContext = Settings.sqlContext*/
  private val _sparkSession = Settings.sparkSession
  private val _sparkContext = _sparkSession.sparkContext
  private var _queryList: ListBuffer[Query] = null
  var _igTime = 0L
  var _sgTime = 0L
  private var _viewSet: Set[String] = Set[String]()

  //  var _supernodeMap: Broadcast[Predef.Map[String, Array[String]]]

  import _sparkSession.implicits._


  /**
   * Parse input compositeQueryFile (set of queries)
   * TODO: make support for single queries
   */
  def parseQueryFile() : Unit = {
    var resQueries = ListBuffer[Query]()
    val fileSource = scala.io.Source.fromFile(Settings.queryFile)
    val lines = fileSource.getLines().toArray.mkString("\n").replaceAll("\r",  "").split("<<<<<<")
    val queries: Array[String] = lines.slice(1, lines.length).sortBy(_.split(">>>>>>")(0))
    fileSource.close()
    //    queries.foreach(println)
    for (query <- queries if query.nonEmpty) {
      val parts = query.split("\\+\\+\\+\\+\\+\\+")
      val prePart = parts(0).split(">>>>>>")
      val queryName = prePart(0)
      val table = prePart(1).replaceAll("\n", "").split("------")
      //      println(s"queryName: $queryType")
      val tableName = table(0)
      //获得sql查询语句
      val sqlQuery = parts(1).split("------")
      val sgSqlQuery = sqlQuery(0) //摘要图sql查询语句
      var sparqlTriple = ""
      var igSqlQuery = "" //原始图sql查询语句
      if (queryName.endsWith("summary")) {
        sparqlTriple = table(1)
        igSqlQuery = sqlQuery(1)
      }
      //      println(s"tableName: $tableName, sparqlTriple: $sparqlTriple, \nsgSqlQuery: $sgSqlQuery, \nigSqlQuery: $igSqlQuery")
      var resQuery = new Query(queryName, tableName, sparqlTriple, sgSqlQuery, igSqlQuery)
      resQueries += resQuery;
    }
    _queryList = resQueries;
  }
  //  def parseQueryFile() = {
  //    var resQueries = ListBuffer[Query]()
  //    val fileSource = scala.io.Source.fromFile(Settings.queryFile)
  //    val queries = fileSource.mkString
  //      .replaceAll("\r", "")
  //      .split("<<<<<<")
  //    fileSource.close()
  //    queries.foreach(println)
  //    for (query <- queries if query.nonEmpty) {
  //      val parts = query.split("\\+\\+\\+\\+\\+\\+")
  //      val index = parts(0).indexOf("\n")
  //      //获得查询类型
  //      val queryType = parts(0).substring(0, index)
  //      val prePart = parts(0).substring(index + 1).split("------").map(_.replaceAll("\n", ""))
  //      //获得sql查询语句
  //      val sqlQuery = parts(1).split("------")
  ////      println(s"queryName: $queryType")
  //      val tableName = prePart(0)
  //      val sgSqlQuery = sqlQuery(0) //摘要图sql查询语句
  //      var sparqlTriple = ""
  //      var igSqlQuery = "" //原始图sql查询语句
  //      if (queryType.split("--").last.equals("summary")) {
  //        sparqlTriple = prePart(1)
  //        igSqlQuery = sqlQuery(1)
  //      }
  //      println(s"tableName: $tableName, sparqlTriple: $sparqlTriple, \nsgSqlQuery: $sgSqlQuery, \nigSqlQuery: $igSqlQuery")
  //      /*var tableStats = qStat.split("------\n")
  //      println("tableStats: ")
  //      tableStats.foreach(println)
  //      //Tables Statistic in the first line
  //      var tables = HashMap[String, Table]();
  //
  //      for (tableStat <- tableStats if tableStat.length() > 0) {
  //        var bestTable = tableStat.substring(0, tableStat
  //          .indexOf("\n")).replaceAll("\r", "")
  //          .split("\t");
  //        var tableName = bestTable(0);
  //        var bestId = bestTable(1);
  //        var tableType = bestTable(2);
  //        var tablePath = bestTable(3);
  //        var table = new Table(tableName, tableType, tablePath);
  //        tables(tableName) = table;
  //      }*/
  //      var resQuery = new Query(queryType, tableName, sparqlTriple, sgSqlQuery, igSqlQuery)
  //      resQueries += resQuery;
  //    }
  //    _queryList = resQueries;
  //  }

  /**
   * Generate path to VP/ExtVP tabel in HDFS
   */
  //  private def getPath(tType: String, tPath: String): String = {
  //    var res: String = "";
  //    println("tPath: " + tPath)
  //
  //    if (tType == "VP") {
  //      //      res = Settings.databaseDir+"VP/"+tPath.replace("/","")+".parquet";
  //      res = {
  //        if (localRun) {
  //          Settings.superEdgesDir + "VP\\"
  //        } else {
  //          Settings.superEdgesDir + "VP/"
  //        }
  //      }
  //      res += tPath.replace("gr__", "_L_http__--purl.org-goodrelations-")
  //        .replace("wsdbm__", "_L_http__--db.uwaterloo.ca-~galuc-wsdbm-")
  //        .replace("sorg__", "_L_http__--schema.org-")
  //        .replace("gr__", "_L_http__--purl.org-goodrelations-")
  //        .replace("/", "") + ".parquet";
  //    } else if (tType == "SO" || tType == "OS" || tType == "SS") {
  //      res = Settings.superEdgesDir + "ExtVP/" + tType + "/" + tPath + ".parquet";
  //    }
  //    res
  //  }

  /**
   * Get a list of query names corresponding to all input queries
   */
  private def extructQueryNames(queries: ListBuffer[Query])
  : ListBuffer[String] = {
    var resNames = ListBuffer[String]();

    for (query <- queries) {
      //remove \r
      resNames += query.queryName
    }

    resNames;
  }

  /**
   * Returns query objects corresponded to given query-name
   */
  private def getQueryByName(name: String, queries: ListBuffer[Query]): Query = {
    var res: Query = null

    for (query <- queries) {
      if (query.queryName == name) {

        res = query;
      }
    }

    res;
  }

  /**
   * Just adds given line to given file
   */
  private def writeLineToFile(line: String, fileName: String) = {
    val fw = new java.io.FileWriter(fileName, true)
    try {
      fw.write(line + "\n")
    }
    finally fw.close()
  }

  /**
   * removes all information from result line except pure time information
   */
  private def toJustTimesLine(line: String): String = {
    var res = line;

    while (res.contains("ms")) {
      res = (res.substring(0, res.indexOf("ms"))
        + res.substring(res.indexOf(")") + 1));
    }

    res
  }

  /**
   * This function prints obtained results into two csv files
   * One file contains results as [time]ms([resultSize])
   * the other contains results as [time] (better for reuse in excell)
   * Every row in files correponds to one query
   * Every column in files correponds to one test case
   */
  //  def printResults(results: HashMap[String, String]) = {
  //    var resMap = HashMap[String, HashMap[String, String]]();
  //
  //    // Map different testcases for same query together
  //    for (res <- results.keySet) {
  //      var distName = res.split("--")
  //      if (resMap contains distName(0)) {
  //        resMap(distName(0))(distName(1)) = results(res);
  //      } else {
  //        resMap(distName(0)) = HashMap[String, String]()
  //        resMap(distName(0))(distName(1)) = results(res);
  //      }
  //    }
  //
  //    // print results as CSV
  //
  //    // print date
  //    var date = "" + Calendar.getInstance().getTime()
  //    writeLineToFile(date, Settings.resultFile)
  //    writeLineToFile(date, Settings.resultFileTimes)
  //
  //    // every line corresponds to one query
  //    // every column corresponds to one test case
  //    for (res <- resMap.keySet.toList.sorted) {
  //      var lineToPrint = res;
  //      for (testCase <- resMap(res).keySet.toList.sorted) {
  //        lineToPrint = (lineToPrint
  //          + "\t" + resMap(res)(testCase)
  //          + " [" + testCase + "]")
  //      }
  //      // print to result file
  //      writeLineToFile(lineToPrint, Settings.resultFile)
  //      // print to result file containing only times
  //      writeLineToFile(toJustTimesLine(lineToPrint),
  //        Settings.resultFileTimes)
  //    }
  //  }

  /**
   * Extract all names of the tests, which use ExtVP for execution
   */
  private def extractAllExtVPTestNames(qNames: ListBuffer[String])
  : ListBuffer[String] = {
    var queryNames = ListBuffer[String]();
    for (qr <- qNames) if (qr.contains("SO")
      || qr.contains("OS")
      || qr.contains("SS")) {
      queryNames += qr
    }
    queryNames
  }

  /**
   * Extract all names of the tests, wich use only VP for execution
   */
  private def extractAllVPTestNames(qNames: ListBuffer[String])
  : ListBuffer[String] = {
    var queryNames = ListBuffer[String]();
    for (qr <- qNames) if (!(qr.contains("SO")
      || qr.contains("OS")
      || qr.contains("SS"))) {
      queryNames += qr
    }
    queryNames
  }

  /**
   * Preload tables containing in query to main memory
   */
  /*private def preloadTables(qr: Query, actualPostfix: String) = {
    // Cache Tables
    println("size: " + qr.tables.size)
    for (tableStat <- qr.tables.values) {
      var fileName = getPath(tableStat.tType, tableStat.tPath);
      println(s"fileName: $fileName")
      if (_cachedTables contains fileName) {
        qr.query = qr.query.replaceAll(tableStat.tName,
          _cachedTables(fileName))
      } else {
        println("\tLoad Table " + tableStat.tName + " from " + fileName + "-> ")
        var table: org.apache.spark.sql.DataFrame = null;

        // repartition the tables in query
        if (actualPostfix.contains("SO") && Settings.partNumberExtVP != 200) {
          /*table = _sqlContext.read.parquet(fileName)
                  .repartition(Settings.partNumberExtVP)*/
          table = _sparkSession.read.parquet(fileName).repartition(Settings.partNumberExtVP)
        }
        else if (Settings.partNumberVP != 200) {
          /*table = _sqlContext.read.parquet(fileName)
                  .repartition(Settings.partNumberVP)*/
          table = _sparkSession.read.parquet(fileName).repartition(Settings.partNumberVP)
        }
        else {
          //          table = _sqlContext.read.parquet(fileName)
          table = _sparkSession.read.parquet(fileName)
        }
        table.createOrReplaceTempView(tableStat.tName);
        _sqlContext.cacheTable(tableStat.tName)
        //        _sqlContext.cacheTable(tableStat.tName)

        var start = System.currentTimeMillis;
        var size = table.count();
        var time = System.currentTimeMillis - start;
        println("\t\tCached " + size + " Elements in " + time + "ms");
        println(s"fileName: $fileName\t\ttableStat.tName: ${tableStat.tName}")

        _cachedTables(fileName) = tableStat.tName;
      }
    }
  }*/

  /**
   * Remove odl tables of old queries from main memory
   */
  private def unCacheTables(): Unit = {
    for (tN <- _viewSet) {
      //      _sparkSession.sqlContext.uncacheTable(tN)
      //      _sparkSession.catalog.uncacheTable(tN)
      _sparkSession.sqlContext.dropTempTable(tN)
    }
    _viewSet = Set()
    _sparkSession.sqlContext.clearCache()
  }

  /*private def avoidDoubleTableReferencing(qr: Query) = {
    for (tableStat <- qr.tables.values) {
      var fileName = getPath(tableStat.tType, tableStat.tPath);
      if (_cachedTables contains fileName) {
        qr.query = qr.query.replaceAll(tableStat.tName,
          _cachedTables(fileName))
      }
    }
  }*/

  /**
   * Execute a single test
   */
  /*private def executeQuery(qr: Query): String = {
    println()
    print("\t Run test-> ")
    var resString = ""
    var resSize: Long = 0
    /*val q = _sparkSession.sql("SELECT obj AS v1 \n\t FROM  gr__offers__1__\n\tWHERE sub = '<http://db.uwaterloo.ca/~galuc/wsdbm/Retailer23>'")
    q.show(100,false)*/
    println("Settings.testLoop: " + Settings.testLoop)
    for (tId <- 1 to Settings.testLoop) {
      var start = System.currentTimeMillis
      //      var temp = _sqlContext.sql(qr.query)
      var summaryMatchDF: DataFrame = _sparkSession.sql(qr.query)
      unloadTables()
      var time = System.currentTimeMillis - start
      summaryMatchDF.show(false)
      summaryMatchDF.createOrReplaceTempView("summaryTable")
      _sqlContext.cacheTable("summaryTable")
      resSize = summaryMatchDF.count()

      //根据原始图信息重新构造查询sql表
      val MIGTime = System.currentTimeMillis()
      val supernodesMap = _sparkContext
        .textFile("E:\\dataset\\LUBM\\supernodes.txt")
        .map(_.split("---"))
        .map(x => (x(0), x(1).split(" ").toList))
        .collectAsMap()
      supernodesMap("<http://www.Department0.University0.edu/GraduateCourse17>").foreach(println)
      /*//获得所属超点
      val id = supernodesMap.filter(x => x._2.contains("<http://www.Department0.University0.edu/GraduateCourse170>"))
        .map(_._1)
      println("id: "+id.max)*/

      val initialEdgeSet = _sparkContext
        .textFile("E:\\dataset\\LUBM\\initialGraph.txt")
        .collect()
        .toSet
      val initialGraphEdgeMap: HashMap[String, Set[String]] = HashMap()
      initialGraphEdgeMap.put("ub:takesCourse", initialEdgeSet)

      //      superNodesMap.put("<http://www.Department0.University0.edu/GraduateStudent92>",List)
      rebuildQueryTable(summaryMatchDF, supernodesMap, initialGraphEdgeMap)
      /*val matchEdgeMap = getMatchEdge(summaryMatchDF, initialGraphMap)
      matchEdgeMap("?X rdf:type ub:Student").foreach(println)*/
      println(s"matchInitialGraph: ${System.currentTimeMillis() - MIGTime} ms")
      summaryMatchDF = null
      if (tId > 1) resString += "/"
      resString += time;
    }
    resString + "ms (" + resSize + ")"
  }*/

  def executeQuery(edgeMap: Broadcast[Map[String, Array[(String, String)]]]): Unit = {
    var sgCount = 0l
    for (query <- _queryList) {
      val qName = query.queryName
      val nameString = query.tNameString
      cacheTableToMemory(nameString, qName.split("--").last)
      _sparkSession.sql(query.sgSQString)
        .queryExecution
        .executedPlan
      val time1 = System.currentTimeMillis()
      val igRes: RDD[InternalRow] = _sparkSession.sql(query.sgSQString)
        .queryExecution
        .executedPlan.execute()

      val igTime = System.currentTimeMillis() - time1
      unCacheTables()

      if(qName.split("--").last.equals("summary") && !igRes.isEmpty()) {
        val time3 = System.currentTimeMillis()
        rebuildQueryTable(igRes, query.sgSQString, query.sTripleString, edgeMap)
        val edgeTime = System.currentTimeMillis() - time3
          _sparkSession.sql(query.igSQString)
            .queryExecution.executedPlan
          val time5 = System.currentTimeMillis()
          val sgQueryPlan = _sparkSession.sql(query.igSQString)
            .queryExecution
            .executedPlan
          val sgRes: RDD[InternalRow] = sgQueryPlan.execute()
          val time6 = System.currentTimeMillis()
          sgCount = sgRes.count()
          val sgTime = time6 - time5
          Settings.addQueryResult(s"$qName: ${igTime + edgeTime +  sgTime}(${sgCount})")

      }else {
        Settings.addQueryResult(s"$qName: $igTime(0)")
      }
    //}
    unCacheTables()
    }
    Settings.addQueryResult("")

  }

  private def cacheTableToMemory(tableNameString: String, qType: String): Unit = {
    val seDir = Settings.superEdgesDir
    /*{
      if (qType.equals("summary")) {
        Settings.superEdgesDir
      } else {
        Settings.initialGraphDir
      }
    }*/
    val tableArray = tableNameString.split("\t").filter(_ != "")
    for (tableName <- tableArray) {
      val table = _sparkSession.read.parquet(seDir + getTablePath(tableName, qType) + ".parquet")
        .repartition(1)
      //      println(tableName+".parquet count: "+table.count())
      val viewString = tableName.split("H_HH_HH_HH_H")(0)
      table.createOrReplaceTempView(viewString)
      _sparkSession.sqlContext.cacheTable(viewString)
      _viewSet += viewString
      //table.count()
    }
  }

  def getTablePath(table: String, qType: String): String = {
    if (table.contains("_L__L__B__B_")) {
      val temp = table.split("H_HH_HH_HH_H")
      "CONN/" + temp(1) + "/" + temp(0).replaceAll("_L__L__B__B_", "/")
    } else {
      val s = {
        if (qType == "summary") "VP/"
        else "IGVP/"
      }
      s + table
    }
  }

  def readSupernodeFile(): Broadcast[Map[String, Array[(String, String)]]] = {

    val y: Broadcast[Map[String, Array[(String, String)]]] = _sparkContext.broadcast(
        _sparkSession.read.parquet(Settings.hashtablePath)
        .rdd
          .map(_.mkString.split("---"))
          .filter(_.length > 2)
        .flatMap(ss => ss(2).split("___")
          .map(so => so.split("--"))
          .flatMap(so => so(1).split("__").map(_.split("\t")).groupBy(_(1))
          .map(p => (ss(0) + " " + p._1 + " " + so(0)) -> p._2.map(t => (t(0), t(2))))
        ))
          .collect().toMap
      )

//    val x: Broadcast[Map[String, Map[String, Map[String, Array[(String, String)]]]]] =
//      _sparkContext.broadcast(
//      _sparkSession.read.parquet(Settings.hashtablePath)
//      .map(_.mkString.split("---"))
//      .filter(_.length > 2)
//      .map(v => v(0) -> v(2).split("___").map(e => e.split("--"))
//        .map(e => e(0) -> e(1).split("__")
//          .groupBy(_.split("\t")(1))
//          .map(p => (p._1 -> p._2.map(_.split("\t")).map(t => (t(0), t(2)))))
//        ).toMap
//      ).collect().toMap
//    )

//    _sparkContext.broadcast(
//      _sparkSession.read.parquet(Settings.hashtablePath)
//        .map(_ (0).toString.split("----"))
//        .map(x => x(0) -> x(1).split("____").map(_.split("---"))
//          .map(y => y(0) -> y(1).split("___").map(_.split("--"))
//            .map(z => z(0) -> z(1).split("__"))
//            .toMap).toMap)
//        .collect().toMap
//    )

    y

    //    val supernodesMap: Broadcast[Predef.Map[String, Map[String, Map[String, Array[String]]]]] = _sparkContext.broadcast(
    //      _sparkSession.read.parquet(Settings.hashtablePath)
    //        .map(_ (0).toString.split("----"))
    //        .collect().map(x => x(0) -> x(1).split("____").map(_.split("---"))
    //        .map(y => y(0) -> y(1).split("___").map(_.split("--"))
    //          .map(z => z(0) -> z(1).split("__"))
    //          .toMap).toMap).toMap
    //    )

  }

  /*def runTests() = {
    var temp = extructQueryNames(_queryList).sorted;
    var queryNames = ListBuffer[String]()
    queryNames ++= extractAllExtVPTestNames(temp)
    queryNames ++= extractAllVPTestNames(temp)

    var results = HashMap[String, String]()
    var testSet = ""
    //    for (queryN <- queryNames) if (queryN.contains("SO")){
    for (queryN <- queryNames) {

      var query = getQueryByName(queryN, _queryList)

      // Example queryName IL5-1-U-1--SO-OS-SS-VP__WatDiv1M
      var actualPrefix = queryN.substring(0, queryN.indexOf("--"))
      // = IL5-1-U-1
      var actualPostfix = queryN.substring(queryN.indexOf("--") + 2)
      // = SO-OS-SS-VP__WatDiv1M
      var actualTestSet = queryN.substring(0, queryN.indexOf("-", queryN.indexOf("-") + 1))
      // = IL5-1

      // HACK for Selectivity Testing-Queries
      if (queryN.indexOf("-") == queryN.indexOf("--"))
        actualTestSet = actualPrefix

      // HACK for yago
      if (actualPostfix.contains("yago")) {
        actualTestSet = queryN.substring(0)
      }

      println("pr-" + actualPrefix + ", pf-" + actualPostfix + ", at-" + actualTestSet)
      println("Test " + query.queryName + ":");

      // move to next query, Uncache Tables
      if (testSet.length > 0 && actualTestSet != testSet) {
        unloadTables()
      }

      // cache tables
      if (testSet.length == 0 || actualTestSet != testSet) {
        testSet = actualTestSet;
        preloadTables(query, actualPostfix)
      }

      avoidDoubleTableReferencing(query)

      println("_cachedTables: ")
      _cachedTables.foreach(println)
      // Print query plane
      /*println("HaLLO")
      /*println(_sqlContext.sql("""""" + query.query + """""")
                          .queryExecution
                          .executedPlan)*/
      println("sql: "+"""""" + query.query + """""")
      println(_sparkSession.sql("""""" + query.query + """""")
                          .queryExecution
                          .executedPlan)
      println("HaLL1")*/
      // Run Tests
      // Execute a dummy execution, since all first execution is always slower than the following executions
      /*if (allFirstExecution){
        var temp = executeQuery(query)
        allFirstExecution = false
      }*/
      results(query.queryName) = executeQuery(query)
      println("result: ")
      println(results(query.queryName))

      unloadTables()
    }
    //print results
    printResults(results)
  }*/

  def rebuildQueryTable(queryRes: RDD[InternalRow], sgSql: String, STString: String, edgeMap: Broadcast[Map[String, Array[(String, String)]]]): Unit = {
    val qEdgeArray = STString.split(", ")
    val variArray = sgSql.split("FROM")(0).split("SELECT")(1).split(",").map(_.trim).map(_.split("\\.").last)
    //    variArray.foreach(println)
    val viewMap: mutable.HashMap[String, Int] = mutable.HashMap()
    //    val queryMap = getMatchMap(queryRes, sgSql)
    for(qEdge <- qEdgeArray) {
      val qEdgeSplit = qEdge.split(" ")
      val tableDF = {
        //判断是不是type类型，是的话不需要匹配
        //          println("predicate: "+qEdgeSplit(1))
        if (!qEdgeSplit(1).contains("type") || qEdgeSplit(2).startsWith("?")) {
          //获得边的笛卡尔积
          val cartRDD: RDD[(String, String)] = {
            if (qEdgeSplit(0).startsWith("?") && qEdgeSplit(2).startsWith("?")) {
              val subID = variArray.indexOf(qEdgeSplit(0).substring(1))
              val objID = variArray.indexOf(qEdgeSplit(2).substring(1))
              queryRes.map(x => (x.getString(subID), x.getString(objID))).distinct()
                .flatMap(x => edgeMap.value(x._1 + " " + qEdgeSplit(1)+ " " + x._2))
//                .map(_.split("\t"))
              //                  .flatMap(x => _supernodeMap.value(x._1).flatMap(s1 => _supernodeMap.value(x._2).map(s2 => s"$s1\t$s2"))
              //                    .filter(y => IGEdgeMap(qEdgeSplit(1).split("____")(0)).contains(y)))
            } else if (qEdgeSplit(0).startsWith("?")) {
              val subID = variArray.indexOf(qEdgeSplit(0).substring(1))
              val objSplit = qEdgeSplit(2).split("____")
              queryRes.map(_.getString(subID)).distinct()
                .flatMap(s => edgeMap.value(s + " " + qEdgeSplit(1) + " " +objSplit(0)))
//                .map(_.split("\t"))
                .filter(_._2.equals(objSplit(1)))
              //                  queArray.flatMap(x => _supernodeMap.value(x).map(s1 => s"$s1\t${qEdgeSplit(2)}")
              //                                  .filter(y => IGEdgeMap(qEdgeSplit(1).split("____")(0) ).contains(y)) )
            } else {
              val objID = variArray.indexOf(qEdgeSplit(2).substring(1))
              val subSplit = qEdgeSplit(0).split("____")
              queryRes.map(_.getString(objID)).distinct()
                .flatMap(o => edgeMap.value(subSplit(0)+" " +qEdgeSplit(1)+" "+ o))
//                .map(_.split("\t"))
                .filter(_._1.equals(subSplit(1)))
              //                  .flatMap(x => _supernodeMap.value(x).map(s1 => s"${qEdgeSplit(0)}\t$s1")
              //                  .filter(y => IGEdgeMap(qEdgeSplit(1).split("____")(0) ).contains(y)) )
            }
          }
          val table = cartRDD.map(vp => VPSchema(vp._1, vp._2))
          //              .coalesce(1, shuffle = true)
          //            println("1: "+table.partitions.length)
          //            println("cartesian time: " + (System.currentTimeMillis() - cartainTime))
          table.toDF().repartition(1)
        } else {
          val subID = variArray.indexOf(qEdgeSplit(0).substring(1))
          val table = queryRes.map(_.getString(subID)).distinct().flatMap(sub => qEdgeSplit(2).split("____")(0).split(",")
            .flatMap(obj => {
              if (edgeMap.value.contains(sub + " " + qEdgeSplit(1) + " "+obj)) {
                edgeMap.value(sub + " " + qEdgeSplit(1) + " "+obj)
              } else {
                Array[(String,String)]()
              }
            }))
//            .map(_.split("\t"))
            .map(vp => VPSchema(vp._1, vp._2))
          //            println("2: "+table.partitions.length)
          table.toDF().repartition(1)
        }
      }
      val tableName = if (viewMap.contains(qEdgeSplit(1))) {
        viewMap(qEdgeSplit(1)) = viewMap(qEdgeSplit(1)) + 1
        qEdgeSplit(1) +"_"+ (viewMap(qEdgeSplit(1))-1)
      } else {
        viewMap.put(qEdgeSplit(1), 1)
        qEdgeSplit(1)
      }
      val view = tableName.replace(":", "__")
      tableDF.createOrReplaceTempView(view)
      //        _sparkSession.catalog.cacheTable(tableName)
      _sparkSession.sqlContext.cacheTable(view)
      _viewSet += view
      //        tableDF.show(100, false)
      //        tableDF.write.parquet("E:\\temporaryFolder\\query\\tmp\\"+tableName)
      //        println(s"$tableName count: $edgeCount")
    }
  }

  //  def rebuildQueryTable(SGMatchDF: DataFrame, IGEdgeMap: HashMap[String, Set[String]], STString: String,
  //                        _supernodeMap: Broadcast[Predef.Map[String, Array[String]]], count: Int): Unit = {
  //    val qEdgeArray = STString.split(", ")
  //    val viewMap: mutable.HashMap[String, Int] = mutable.HashMap()
  //    qEdgeArray.foreach(
  //      (qEdge: String) => {
  //        val qEdgeSplit = qEdge.split(" ")
  //        val tableDF = {
  //          //判断是不是type类型，是的话不需要匹配
  //          if (!qEdgeSplit(1).contains("type") || qEdgeSplit(2).startsWith("?")) {
  //            val cartainTime = System.currentTimeMillis()
  //            val cartArray = {
  //              if (qEdgeSplit(0).startsWith("?") && qEdgeSplit(2).startsWith("?")) {
  //                //                _sparkSession.sql(s"select ${qEdgeSplit(0).substring(1)}, ${qEdgeSplit(2).substring(1)} from summaryMatch")
  //                //                  .distinct()
  //                //                  .collect()
  //                //                  .flatMap(x => _sparkContext.parallelize(_supernodeMap(x.get(0).toString))
  //                //                    .cartesian(_sparkContext.parallelize(_supernodeMap(x.get(1).toString))).collect())
  //
  //                SGMatchDF.select(qEdgeSplit(0).substring(1), qEdgeSplit(2).substring(1))
  //                  .distinct()
  //                  .flatMap(x => _supernodeMap.value(x.get(0).toString).flatMap(s1 => _supernodeMap.value(x.get(1).toString).map(s2 => s"$s1\t$s2"))
  //                    .filter(y => IGEdgeMap(qEdgeSplit(1).split("____")(0)).contains(y)) )
  //
  //              } else if (qEdgeSplit(0).startsWith("?")) {
  //                //                val s = SGMatchDF.select(qEdgeSplit(0).substring(1))
  //                //                  .distinct()
  //                //                  .collect()
  //                //                  .flatMap(x => _sparkContext.parallelize(_supernodeMap(x.get(0).toString))
  //                //                    .cartesian(_sparkContext.parallelize(List(qEdgeSplit(2)))).collect())
  //                //                    s
  //                val s = SGMatchDF.select(qEdgeSplit(0).substring(1))
  //                  .distinct()
  ////                  .collect()
  //                  .flatMap(x => _supernodeMap.value(x.get(0).toString).map(s1 => s"$s1\t${qEdgeSplit(2)}")
  //                    .filter(y => IGEdgeMap(qEdgeSplit(1).split("____")(0)).contains(y)))
  //                s
  //              } else {
  //                //                SGMatchDF.select(qEdgeSplit(2).substring(1))
  //                //                  .distinct()
  //                //                  .collect()
  //                //                  .flatMap(x => _sparkContext.parallelize(List(qEdgeSplit(0)))
  //                //                    .cartesian(_sparkContext.parallelize(_supernodeMap(x.get(0).toString))).collect())
  //                SGMatchDF.select(qEdgeSplit(2).substring(1))
  //                  .distinct()
  ////                  .map(x => VPSchema(qEdgeSplit(0), _supernodeMap(x.get(0).toString).mkString(",\t")))
  //                  .flatMap(x => _supernodeMap.value(x.get(0).toString).map(s1 => s"${qEdgeSplit(0)}\t$s1")
  //                    .filter(IGEdgeMap(qEdgeSplit(1).split("____")(0)).contains(_)))
  //              }
  //            }
  //            //            val table = cartArray.map(x => x._1+"\t"+x._2)
  //            //              .filter(x => IGEdgeMap(qEdgeSplit(1).split("____")(0)).contains(x))
  //            //              .map(_.split("\t"))
  //            //              .map(vp => VPSchema(vp(0), vp(1)))
  ////            val df = cartArray.withColumn("sub", functions.explode(functions.split($"sub", ",\t")))
  ////              .withColumn("obj", functions.explode(functions.split($"obj", ",\t")))
  //            val table = cartArray.map(_.split("\t"))
  //                          .map(vp => VPSchema(vp(0), vp(1))).toDF()
  ////            println("cartesian time: " + (System.currentTimeMillis() - cartainTime))
  //            table.repartition(count)
  //            //            _sparkSession.createDataFrame(table)
  //          } else {
  //            val table = _sparkSession.sql(s"select ${qEdgeSplit(0).substring(1)} from summaryMatch")
  //              .distinct()
  //              .flatMap(x => _supernodeMap.value(x.get(0).toString))
  //              .map(sub => subSchema(sub))
  //              .toDF()
  //            //            val table = SGMatchDF.select(qEdgeSplit(0).substring(1))
  //            //              .collect()
  //            //              .distinct
  //            //              .flatMap(x => _supernodeMap(x.get(0).toString))
  //            //              .map(sub => subSchema(sub))
  //            table.repartition(count)
  //          }
  //        }
  //        val tableName = if(viewMap.contains(qEdgeSplit(1))){
  //          viewMap(qEdgeSplit(1)) = viewMap(qEdgeSplit(1)) + 1
  //          qEdgeSplit(1) + (viewMap(qEdgeSplit(1))-1)
  //        }else {
  //          viewMap.put(qEdgeSplit(1), 1)
  //          qEdgeSplit(1)
  //        }
  //        tableDF.createOrReplaceTempView(tableName)
  ////        _sparkSession.catalog.cacheTable(tableName)
  //        _sparkSession.sqlContext.cacheTable(tableName)
  //        _viewSet += tableName
  //        tableDF.count()
  ////        tableDF.show(100, false)
  ////        tableDF.write.parquet("E:\\temporaryFolder\\query\\tmp\\"+tableName)
  ////        println(s"$tableName count: $edgeCount")
  //      }
  //    )
  //  }

  def getMatchMap(matchRes: RDD[InternalRow], sqlString: String): mutable.HashMap[String, RDD[String]] = {
    val variArray = sqlString.split("FROM")(0).split("SELECT")(1).split(",").map(_.trim)
    val resMap: mutable.HashMap[String, RDD[String]] = mutable.HashMap()
    for(index <- variArray.indices) {
      //      val t = sgRes.flatMap(x => Array(x.getString(0), x.getString(1)))
      resMap.put(variArray(index), matchRes.map(x => x.getString(index)).distinct())
    }
    resMap.foreach(x => x._2.foreach(println))
    resMap
  }
  def getCartesian(list1: Array[String], list2: Array[String], string: String): Array[String] = {
    list1.flatMap(l1 => {
      list2.map(l2 => l1 + "\t" + l2)
    })
  }

  /*def rebuildQueryTable(SGMatchDF: DataFrame, IGEdgeMap: HashMap[String, Set[String]], STString: String): Unit = {
    //    val qEdgeArray = Array("?X rdf:type ub:Student", "?Y rdf:type ub:Course", "?X ub:takesCourse ?Y", "<http://www.Department0.University0.edu/AssociateProfessor0> ub:teacherOf ?Y")
    //    val qEdgeArray = "?X ub:takesCourse ?Y".split(",")
    val qEdgeArray = STString.split(",")
    qEdgeArray.foreach(
      (qEdge: String) => {
        val qEdgeSplit = qEdge.split(" ")
        //        if (!qEdgeSplit(2).endsWith("type")) {
        val cartArray = {
          if (qEdgeSplit(0).charAt(0) == '?' && qEdgeSplit(2).charAt(0) == '?') {
            SGMatchDF.select(qEdgeSplit(0).substring(1), qEdgeSplit(2).substring(1))
              .collect()
              .flatMap(x => _sparkContext.parallelize(_supernodeMap(x.get(0).toString))
                .cartesian(_sparkContext.parallelize(_supernodeMap(x.get(1).toString))).collect())
              .distinct
            /*SGMatchDF.select(qEdgeSplit(0).substring(1), qEdgeSplit(2).substring(1))
              .collect()
              .flatMap(x => broadcast(_sparkSession.createDataFrame(_supernodeMap(x.get(0).toString).map(subSchema(_))))
                .join(broadcast(_sparkSession.createDataFrame(_supernodeMap(x.get(1).toString).map(objSchema(_)))))
                .collect())
              .distinct*/
          } else if (qEdgeSplit(0).charAt(0) == '?') {
            val s = SGMatchDF.select(qEdgeSplit(0).substring(1))
              .collect()
              .flatMap(x => _sparkContext.parallelize(_supernodeMap(x.get(0).toString))
                .cartesian(_sparkContext.parallelize(List(qEdgeSplit(2)))).collect())
              .distinct
            /*val s: Array[Row] = SGMatchDF.select(qEdgeSplit(0).substring(1))
              .collect()
              .flatMap(x => broadcast(_sparkSession.createDataFrame(_supernodeMap(x.get(0).toString).map(subSchema)))
                .join(_sparkSession.createDataFrame(Array(subSchema(qEdgeSplit(2)))))
                .collect())
              .distinct*/
            //            println("type")
            //            s.foreach(println)
            s
          } else {
            SGMatchDF.select(qEdgeSplit(2).substring(1))
              .collect()
              .flatMap(x => _sparkContext.parallelize(List(qEdgeSplit(0)))
                .cartesian(_sparkContext.parallelize(_supernodeMap(x.get(0).toString))).collect())
              .distinct
          }
        }
        val table: Array[VPSchema] = cartArray.map(x => x._1 + "\t" + x._2)
          .filter(x => IGEdgeMap(qEdgeSplit(1).split("____")(0)).contains(x))
          .map(_.split("\t"))
          .map(vp => VPSchema(vp(0), vp(1)))
        val tableDF = _sparkSession.createDataFrame(table)
        //        val tableDF = _sparkContext.parallelize(table).toDF()
        tableDF.createOrReplaceTempView(qEdgeSplit(1))
        _sparkSession.catalog.cacheTable(qEdgeSplit(1))
        //        tableDF.show(100,false)
        //        println(s"qEdge: $qEdge, cartesian count: ${tableDF.count()}")
      }
    )
    /*val table = SGMatchDF.select(qEdgeSplit(0).substring(1), qEdgeSplit(2).substring(1))
      .collect()
      .flatMap(x => _sparkSession.sparkContext.parallelize(snMap(x.get(0).toString))
        .cartesian(_sparkSession.sparkContext.parallelize(snMap(x.get(1).toString))).collect())
      .map(x => x._1 + "\t" + x._2)
      .filter(x => IGEdgeMap(qEdgeSplit(1)).contains(x)).map(_.split("\t"))
      .map(vp => VPPartition(vp(0), vp(1)))
    val tableDF = _sparkSession.sparkContext.parallelize(table).toDF()
    table.foreach(println)
    tableDF.createOrReplaceTempView(qEdgeSplit(1).replace(":", "__"))
    _sparkSession.sqlContext.cacheTable(qEdgeSplit(1).replace(":", "__"))*/
    //    table.show(false)
    //    table.createOrReplaceTempView(qEdgeSplit(1).replace(":","__"))
    //    _sparkSession.sqlContext.cacheTable(qEdgeSplit(1).replace(":","__"))

    /*for (qEdge <- qEdgeArray){
      val qEdgeSplit = qEdge.split(" ")
      val table = SGMatchDF.select(qEdgeSplit(0).substring(1), qEdgeSplit(2).substring(1))
        .rdd
        .flatMap(x=> _sparkSession.sparkContext.parallelize(List(x.get(0).toString))
        .cartesian(_sparkSession.sparkContext.parallelize(qEdgeMap(x.get(1).toString))).collect()).map(x => x._1+"\t"+x._2)
        .filter(x => initialGM("type").contains(x)).map(_.split("\t"))
        .map(vp => VPPartition(vp(0),vp(1)))
        .toDF()
      table.createOrReplaceTempView(qEdgeSplit(1))
      _sparkSession.sqlContext.cacheTable(qEdgeSplit(1))

      if (qEdgeSplit(0).charAt(0) == '?' && qEdgeSplit(2).charAt(0) == '?') {
        qEdgeMap.put(qEdge, SGMatchDF.select(qEdgeSplit(0).substring(1), qEdgeSplit(2).substring(1))
          .distinct().collect().map(x => s"${x.get(0)}\t${qEdgeSplit(1)}\t${x.get(1)}"))
      } else if (qEdgeSplit(0).charAt(0) == '?') {
        qEdgeMap.put(qEdge, SGMatchDF.select(qEdgeSplit(0).substring(1))
          .distinct().collect().map(_(0) + s"\t${qEdgeSplit(1)}\t${qEdgeSplit(2)}"))
      } else {
        qEdgeMap.put(qEdge, SGMatchDF.select(qEdgeSplit(2).substring(1))
          .distinct().collect().map(x => s"${qEdgeSplit(0)}\t${qEdgeSplit(1)}\t${x.get(0)}"))
      }
    }*/
    //    qEdgeMap("?X rdf:type ub:Student").foreach(println)

  }*/


  //VP Table Schema
  case class VPSchema(sub: String, obj: String)

  case class subSchema(sub: String)

  case class objSchema(obj: String)

}
