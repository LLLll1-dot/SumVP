package dataCreator

import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions.count

import collection.mutable.HashMap

object DataSetGenerator {

  // Spark initialization
  //  private val _sc = Settings.sparkContext
  //  private val _sqlContext = Settings.sqlContext
  private val _sparkSession = Settings.sparkSession

  import _sparkSession.implicits._
  //  import _sqlContext.implicits._

  // number of triples in input dataset
  private var _inputSize = 0: Long
  // number of triples for every VP table  
  private var _vpTableSizes = new HashMap[String, Long]()
  // set of unique predicates from input RDF dataset
  private var _uPredicates = null: Array[String]

  def generateDataSet() = {

    // create or load TripleTable if already created
    val datasetType = Settings.datasetType
    val time1 = System.currentTimeMillis()
    createTT()
    // create directory for all vp and pre-join tables
    Helper.removeDirInHDFS(_sparkSession, Settings.vpDir)
    Helper.createDirInHDFS(_sparkSession, Settings.vpDir)
    Helper.removeDirInHDFS(_sparkSession, Settings.extVpDir)
    Helper.createDirInHDFS(_sparkSession, Settings.extVpDir)
//    _sparkSession.catalog.uncacheTable("triples")
    createVP()
    val time2 = System.currentTimeMillis()
    createCONN()
    val time3 = System.currentTimeMillis()
    val vpTime = (time2 - time1) / 1000.0
    val ttTime = (time3 - time2) / 1000.0
//    println(s"ttTime: $ttTime s")
    //测试输出
//    _uPredicates.foreach(println)
//    println(_inputSize)
    // extarct all unique predicates from TripleTable
    // necessary for VP/ExtVP generation
//    _uPredicates = _sparkSession.sql("select distinct pred from triples")
//      .map(t => t(0).toString)
//      .collect()
//      .sorted
//    _uPredicates = df.select("pred").distinct().collect().map(_.get(0).toString).sorted
//    _sparkSession.catalog.uncacheTable("triples")
//    StatisticWriter.init(_uPredicates.length, _inputSize)

//    // create or load Vertical Partitioning if already exists
//    val time2 = System.currentTimeMillis()
//    if (datasetType.equals("VP")) createVP()
//    else loadVP()
//    val vpTime = (System.currentTimeMillis() - time2) / 1000.0
////    println(s"vpTime: $vpTime s")
//
//    // if we create/recreate VP than we gonna later probably create/recreate
//    // ExtVP. Out of this reason we remove ExtVP directory containing old tables
//    // and create it empty again
//    /*if (datasetType == "VP") {
//      Helper.removeDirInHDFS(Settings.extVpDir)
//      Helper.createDirInHDFS(Settings.extVpDir)
//    }*/
//    // create Extended Vertical Partitioning table set definded by datasetType
//
//    val time3 = System.currentTimeMillis()
//    if (datasetType.equals("CONN")) {
//      Helper.removeDirInHDFS(_sparkSession, Settings.extVpDir)
//      Helper.createDirInHDFS(_sparkSession, Settings.extVpDir)
//      createCONN()
//    }
////    createSGVP()
//    val connTime = (System.currentTimeMillis() - time3) / 1000.0
//
//    val time = s"create triple table time: $ttTime s, " +
//      s"create vp table time: $vpTime s, " +
//      s"create conn table time: $connTime s \n" +
//    s"total time: ${(System.currentTimeMillis()-_startTime)/1000.0} s "
    println(s"vp time: $vpTime, prejoin time: $ttTime, totalTime: ${vpTime + ttTime} s")
    Helper.writeLineToFile(s"//////////////////////////$datasetType////////////////////////////\n"
      + s"vp time: $vpTime, prejoin time: $ttTime, totalTime: ${vpTime + ttTime} s", "dataTime.txt")
//    StatisticWriter.closeStatisticFile()
    _sparkSession.catalog.clearCache()
    _sparkSession.close()

  }

  private def readHashTable(): Unit = {

//    val hashtable = _sparkSession.read.text(Settings.inputRDFSet)
//      .map(_.get(0).toString.split("\t"))
//      //      .filter(!_(0).startsWith("<unknown"))
//      .map(p => Triple(p(0), p(1), p(2)))
//      .toDF

    val hashtable = _sparkSession.read.parquet(Settings.inputRDFSet)
      .map(_.get(0).toString.split("---"))
      .filter(_.length > 2)
      .flatMap(v => v(2).split("___").map(e => e.split("--"))
        .flatMap(e => e(1).split("__").map(_.split("\t")(1)).distinct.map(p => (p, e(0)))
        ).map(x => Triple(v(0), x._1, x._2))
      )

    val vp = hashtable.map(t => (t.pred, vpSchema(t.sub, t.obj)))
    vp.toDF("key", "value").write.partitionBy().parquet(Settings.igVpDir)
    vp.rdd.map(x => (x._1, 1)).reduceByKey(_ + _)
    val df =  hashtable.toDF
    df.createOrReplaceTempView("triples")
    _sparkSession.catalog.cacheTable("triples")
    df.write.parquet(Settings.tripleTable)
    _inputSize = df.count()
  }


  /**
   * Generate TripleTable and save it to Parquet file in HDFS.
   * The table has to be cached, since it is used for generation of VP and ExtVP
   */
  private def createTT(): Unit = {

//    val x = _sparkSession.read.parquet(Settings.inputRDFSet)
//      //    _sparkSession.read.text("E:\\ysData\\code\\rdfGSforGit\\results\\query\\superedges-21138.txt")
//      .map(_.get(0).toString.split("---"))
//      .filter(_.length > 2)
//      .flatMap(v => v(2).split("___").map(e => e.split("--"))
//        .flatMap(e => e(1).split("__").map(_.split("\t")).filter(_.length < 1))
//        ).show(100)

    val df =
    _sparkSession.read.parquet(Settings.inputRDFSet)
//    _sparkSession.read.text("E:\\ysData\\code\\rdfGSforGit\\results\\query\\superedges-21138.txt")
      .map(_.get(0).toString.split("---"))
      .filter(_.length > 2)
      .flatMap(v => v(2).split("___").map(e => e.split("--"))
        .flatMap(e => e(1).split("__").map(_.split("\t")(1)).distinct.map(p => (p, e(0)))
        ).map(x => Triple(v(0), x._1, x._2))
      ).toDF()

    df.createOrReplaceTempView("triples")
    _sparkSession.catalog.cacheTable("triples")
    _inputSize = df.count()

    val statisticArray = df.groupBy("pred")
      .agg(count("sub"))
      .collect()
    _uPredicates = statisticArray.map(_.get(0).toString)
    StatisticWriter.init(_uPredicates.length, _inputSize)
    StatisticWriter.initNewStatisticFile(Settings.datasetType)
    val fw = new java.io.FileWriter(StatisticWriter._statisticFileName, true)
    try {
      statisticArray.map (
        array => {
          StatisticWriter.getVPStatisticString(array(0).toString, array(1).toString.toLong)
        }
      ).foreach(x => fw.write( x + "\n") )
    }
    finally fw.close()

//    Helper.removeDirInHDFS(_sparkSession, Settings.vpDir)
//    Helper.createDirInHDFS(_sparkSession, Settings.vpDir)
//    for(predicate<-_uPredicates) {
//      val cleanPredicate = Helper.getPartName(predicate)
//      val table = df.filter($"pred".===(predicate)).select("sub","obj")
//      table.show(5,false)
//      table.write.parquet(Settings.vpDir + cleanPredicate + ".parquet")
//      StatisticWriter.incSavedTables()
//    }

  }


  /**
   * Loads TT table and caches it to main memory.
   * TT table is used for generation of ExtVP and VP tables
   */
  private def loadTT(): DataFrame = {
    val df = _sparkSession.read.parquet(Settings.tripleTable)
    df.createOrReplaceTempView("triples")
    _sparkSession.catalog.cacheTable("triples")
//    _inputSize = df.count()
    val statisticArray = df.groupBy("pred")
      .agg(count("sub"))
      .collect()
    _uPredicates = statisticArray.map(_.getString(0))
    _inputSize = statisticArray.map(_.getLong(1)).sum
//    _uPredicates = _sparkSession.sql("select distinct pred from triples").collect().map(_.mkString)
//    _uPredicates = df.select("pred").distinct().collect().map(_.mkString)
    df
  }

  /**
   * Generates VP table for each unique predicate in input RDF dataset.
   * All tables have to be cached, since they are used for generation of ExtVP 
   * tables.
   */
  private def createVP() = {
////     create directory for all vp tables
//    Helper.removeDirInHDFS(_sparkSession, Settings.vpDir)
//    Helper.createDirInHDFS(_sparkSession, Settings.vpDir)
//    StatisticWriter.initNewStatisticFile(Settings.datasetType)

    // create and cache vpTables for all predicates in input RDF dataset
    _uPredicates.foreach(
      predicate => {
        val vpTable = _sparkSession.sql("select sub, obj "
          + "from triples where pred='" + predicate + "'")

        val cleanPredicate = Helper.getPartName(predicate)
        vpTable.createOrReplaceTempView(cleanPredicate)
        _sparkSession.catalog.cacheTable(cleanPredicate)

        vpTable.write.parquet(Settings.vpDir + cleanPredicate + ".parquet")
//        print(vpTable.count())
//        statistic file
        //        val sizeTime = System.currentTimeMillis()
//        val sizeTable = vpTable.agg(countDistinct("sub"), countDistinct("obj")).collect()
//        println("sizeTime: "+(System.currentTimeMillis() - sizeTime)/1000.0)
//        sizeTable.foreach(
//          t => {
//            subSize = t.get(0).toString.toLong
//            objSize = t.get(1).toString.toLong
//          })

        //      _sparkSession.catalog.uncacheTable(cleanPredicate)

        // print statistic line
        StatisticWriter.incSavedTables()

      }
    )
  }

  /**
   * Loads VP tables and caches them to main memory.
   * VP tables are used for generation of ExtVP tables
   */
  private def loadVP() = {
    for (predicate <- _uPredicates) {
      val cleanPredicate = Helper.getPartName(predicate)
      val vpTable = _sparkSession.read.parquet(Settings.igVpDir
        + cleanPredicate
        + ".parquet")

      vpTable.createOrReplaceTempView(cleanPredicate)
      _sparkSession.catalog.cacheTable(cleanPredicate)
//      _vpTableSizes(predicate) = vpTable.count()
//      _sparkSession.catalog.uncacheTable(cleanPredicate)
    }
  }

  /**
   * Generates ExtVP tables for all (relType(SO/OS/SS))-relations of all 
   * VP tables to the other VP tables 
   */
  private def createCONN() = {
    // create directory for all ExtVp tables of given relType (SO/OS/SS)
    val typeArray = Array("ss", "os", "oo")
//    val constantMap: Broadcast[Map[String, String]] = {
//      _sparkSession.sparkContext.broadcast(
//        _sparkSession.read.text(Settings.inputRDFSet)
//          .collect()
//          .map(x => x.get(0).toString.split("----")(1).split("\t"))
//          .flatMap(x => x.map((_, x(0))))
//          .toMap
//      )
//    }
    Helper.writeLineToFile("//////////////////////////////////////////////////////\n", "dataTime.txt")
    for (relType <- typeArray) {
//      println(s"start: $relType !")
//      val startTime = System.currentTimeMillis()
      Helper.createDirInHDFS(_sparkSession, Settings.extVpDir + relType + "/")
      StatisticWriter.initNewStatisticFile(relType)

      // for every VP table generate a set of ExtVP tables, which represent its
      // (relType)-relations to the other VP tables
      for (pred1 <- _uPredicates) {
        Helper.createDirInHDFS(_sparkSession, Settings.extVpDir
          + relType + "/"
          + Helper.getPartName(pred1))

        // get all predicates, whose TPs are in (relType)-relation with TP
        // (?x, pred1, ?y)
//        val getRelTime = System.currentTimeMillis()
        val relatedPredicates = getRelatedPredicates(pred1, relType)
//        println("getRelTime: "+(System.currentTimeMillis()-getRelTime)/1000.0)

        for (pred2 <- relatedPredicates) {
//        for (pred2 <- _uPredicates
//             if !pred2.contains("type") && (relType=="os" || pred1 < pred2)) {
          var extVpTableSize = -1: Long
//          var xSize = 0: Long
//          var ySize = 0: Long
//          var zSize = 0: Long

          // we avoid generation of ExtVP tables corresponding to subject-subject
          // relation to it self, since such tables are always equal to the
          // corresponding VP tables
          if (!((relType == "ss" || relType == "oo") && pred1 == pred2)) {
            val sqlCommand = getExtVpSQLcommand(pred1, pred2, relType)
//            println(s"sqlCommand: $sqlCommand")
//            val extVpTable = _sparkSession.sql(sqlCommand).queryExecution.executedPlan.execute()
//              .map(x => (x.getString(0), x.getString(1), x.getString(2)))
//              .map(x => connSchema(constantMap.value(x._1), constantMap.value(x._2), constantMap.value(x._3)))
//              .distinct()
//              .repartition(1)
//              .toDF()
            val extVpTable = _sparkSession.sql(sqlCommand)
            extVpTableSize = extVpTable.count()
//            val extVpTableArray = _sparkSession.sql(sqlCommand)
//              .collect()
//            extVpTableSize = extVpTableArray.length

//            println("connTime: "+(tempViewTime-connTime)/1000.0)
//            println("tempViewTime: "+(System.currentTimeMillis()-tempViewTime)/1000.0)
            if (extVpTableSize > 0) {
//              val tempViewTime = System.currentTimeMillis()
//               save ExtVP table
//              val writeTime = System.currentTimeMillis()
              extVpTable.write.parquet(Settings.extVpDir
                + relType + "/"
                + Helper.getPartName(pred1) + "/"
                + Helper.getPartName(pred2)
                + ".parquet")
              //测试输出
              StatisticWriter.incSavedTables()
            } else {
              println(s"$pred1 $pred2 $relType")
              StatisticWriter.incUnsavedNonEmptyTables()
            }
            //测试输出
            //            println("ppTime: "+(System.currentTimeMillis()-ppTime)/1000.0)
            //            _sparkSession.catalog.uncacheTable("extvp_table")
          }
          //          else {
          //            extVpTableSize = _vpTableSizes(pred1)
          //          }

          // print statistic line
          // save statistics about all ExtVP tables > 0, even about those, which
          // > then ScaleUB.
          // We need statistics about all non-empty tables
          // for the Empty Table Optimization (avoiding query execution for
          // the queries having triple pattern relations, which lead to empty
          // result)
          StatisticWriter.addCONNTableStatistic(pred1 + "<<>>" + pred2,
            extVpTableSize)
        }

      }

      StatisticWriter.closeStatisticFile()
//      val time = (System.currentTimeMillis() - startTime) / 1000.0
//      println(relType + " has finished!, total time: " + time + "s")
//      Helper.writeLineToFile(s"$relType: $time s", "dataTime.txt")
    }

  }

//  def getCONN(sr1: String, sr2: String): Unit ={
//    val rdfDF = spark.read.text(_dataset).map(r => r.mks)
//    val arrayCols = rdfDF.groupBy("s", "p").agg(count("*").as("rowCount"))
//      .where($"rowCount" > 1).select("p").distinct.collect.map(row => row.getString(0))
//    val aggDF = rdfDF.groupBy("s").pivot("p").agg(collect_list("o"))
//    val stringCols = aggDF.columns.filter(x => x != "s" && !arrayCols.contains(x))
//    val ptDF = stringCols.foldLeft(aggDF)((df, x) => df.withColumn(x, col(x)(0)))
//    val finalPT = arrayCols.foldLeft(ptDF)((df, x) => df.withColumn(x, when(size(col(x)) > 0, col(x)).otherwise(lit(null))))
//    finalPT.write.mode(SaveMode.Overwrite).parquet(Settings.ptDir)
//  }
//  private def createExtVP() = {
//
//    // create directory for all ExtVp tables of given relType (SO/OS/SS)
//    val typeArray = Array("ss", "os", "oo")
//    val constantMap = {
//      _sparkSession.sparkContext.broadcast(
//        _sparkSession.read.text(Settings.inputRDFSet)
//          .collect()
//          .map(x => x.get(0).toString.split("----")(1).split("\t"))
//          .flatMap(x => x.map((_, x(0))))
//          .toMap
//      )
//    }
//
//    Helper.writeLineToFile("//////////////////////////////////////////////////////\n", "dataTime.txt")
//    for (relType <- typeArray) {
//      //      println(s"start: $relType !")
//      val startTime = System.currentTimeMillis()
//      Helper.createDirInHDFS(_sparkSession, Settings.extVpDir + relType + "/")
//      StatisticWriter.initNewStatisticFile(relType)
//
//      // for every VP table generate a set of ExtVP tables, which represent its
//      // (relType)-relations to the other VP tables
//      for (pred1 <- _uPredicates if !pred1.contains("type")) {
//        Helper.createDirInHDFS(_sparkSession, Settings.extVpDir
//          + relType + "/"
//          + Helper.getPartName(pred1))
//
//        // get all predicates, whose TPs are in (relType)-relation with TP
//        // (?x, pred1, ?y)
//        val relatedPredicates = getRelatedPredicates(pred1, relType)
//
//        for (pred2 <- relatedPredicates if !pred2.contains("type")) {
//          var extVpTableSize = -1: Long
//          var xSize = 0: Long
//          var ySize = 0: Long
//          var zSize = 0: Long
//
//          // we avoid generation of ExtVP tables corresponding to subject-subject
//          // relation to it self, since such tables are always equal to the
//          // corresponding VP tables
//          if (!(relType == "ss" && pred1 == pred2)) {
//            val sqlCommand = getExtVpSQLcommand(pred1, pred2, relType)
//            //            println(s"sqlCommand: $sqlCommand")
//            val extVpTable = _sparkSession.sql(sqlCommand)
//              .map(x => (constantMap.value(x.get(0).toString), constantMap.value(x.get(1).toString), constantMap.value(x.get(2).toString)))
//              .map(x => connTable(x._1, x._2, x._3))
//              .distinct()
//              .toDF()
//            extVpTable.createOrReplaceTempView("extvp_table")
//            //            // cache table to avoid recomputation of DF by storage to HDFS
//            _sparkSession.catalog.cacheTable("extvp_table")
//            extVpTableSize = extVpTable.count()
//            //            println(s"extVpTableSize: $extVpTableSize, xSize: $xSize, ySize: $ySize, zSize: $zSize")
//            //            extVpTable.show(20, false)
//
//            // save ExtVP table in case if its size smaller than
//            // ScaleUB*size(corresponding VPTable)
//            //            if (extVpTableSize < _vpTableSizes(pred1) * Settings.ScaleUB) {
//            if (extVpTableSize > 0) {
//
//              // create directory extVP/relType/pred1 if not exists
//              /* if (!createdDirs.contains(pred1)) {
//                 createdDirs = pred1 :: createdDirs
//                 Helper.createDirInHDFS(Settings.extVpDir
//                   + relType + "/"
//                   + Helper.getPartName(pred1))
//               }*/
//
//              // save ExtVP table
//              extVpTable.repartition(1).write.parquet(Settings.extVpDir
//                + relType + "/"
//                + Helper.getPartName(pred1) + "/"
//                + Helper.getPartName(pred2)
//                + ".parquet")
//
//              val sizeTable =  extVpTable.agg(countDistinct("X"), countDistinct("Y"), countDistinct("Z")).collect()
//              //              val exprs = extVpTable.columns.map(_ -> "approx_count_distinct").toMap
//              sizeTable.foreach(
//                t => {
//                  xSize = t.get(0).toString.toLong
//                  ySize = t.get(1).toString.toLong
//                  zSize = t.get(2).toString.toLong
//                })
//
//              StatisticWriter.incSavedTables()
//            } else {
//              StatisticWriter.incUnsavedNonEmptyTables()
//            }
//            _sparkSession.catalog.uncacheTable("extvp_table")
//          } else {
//            extVpTableSize = _vpTableSizes(pred1)
//          }
//
//          // print statistic line
//          // save statistics about all ExtVP tables > 0, even about those, which
//          // > then ScaleUB.
//          // We need statistics about all non-empty tables
//          // for the Empty Table Optimization (avoiding query execution for
//          // the queries having triple pattern relations, which lead to empty
//          // result)
//          StatisticWriter.addCONNTableStatistic(pred1 +"<<>>"+ pred2,
//            extVpTableSize, xSize, ySize, zSize,
//            _vpTableSizes(pred1), _vpTableSizes(pred2))
//        }
//
//      }
//
//      StatisticWriter.closeStatisticFile()
//      val time = (System.currentTimeMillis()-startTime)/1000.0
//      println(relType + " has finished!, total time: "+ time+"s")
//      Helper.writeLineToFile(s"$relType: $time s", "dataTime.txt")
//    }
//
//  }

  /**
   * Returns all predicates, whose triple patterns are in (relType)-relation 
   * with TP of predicate pred.
   */
  private def getRelatedPredicates(pred: String, relType: String)
  : Array[String] = {
    var sqlRelPreds = ("select distinct pred "
      + "from triples t1 "
      + "left semi join ")

//    "select distinct pred from tt1 t1 left semi join "
    if (relType == "ss") {
      sqlRelPreds += "(select sub from "+
        Helper.getPartName(pred)+") t2 on (t1.sub=t2.sub)"
    } else if (relType == "os") {
      sqlRelPreds += "(select obj from "+
        Helper.getPartName(pred)+") t2 on (t1.sub=t2.obj)"
    } else if (relType == "oo") {
      sqlRelPreds += "(select obj from "+
        Helper.getPartName(pred)+") t2 on (t1.obj=t2.obj)"
    }
//    println("sqlRelPreds: "+sqlRelPreds)
    val preRDD = _sparkSession.sql(sqlRelPreds).queryExecution.executedPlan.execute().map(_.getString(0)).distinct().collect()
    if(relType.equals("os")) {
      preRDD
    } else {
      preRDD.filter(_ > pred)
    }
  }

  /**
   * Generates SQL query to obtain ExtVP_(relType)pred1|pred2 table containing
   * all triples(pairs) from VPpred1, which are linked by (relType)-relation
   * with some other pair in VPpred2
   */
  private def getExtVpSQLcommand(pred1: String,
                                 pred2: String,
                                 relType: String): String = {
    //    var command = ("select t1.sub as sub, t1.obj as obj "
    //                    + "from " + Helper.getPartName(pred1) + " t1 "
    //                    + "left semi join " + Helper.getPartName(pred2) + " t2 "
    //                    + "on ")
    var command = ("select t1.sub as X, t1.obj as Y, t2.obj as Z "
      + "from " + Helper.getPartName(pred1) + " t1 "
      + " join " + Helper.getPartName(pred2) + " t2 "
      + "on ")

    if (relType == "ss") {
      command += "(t1.sub=t2.sub)"
    } else if (relType == "os") {
      command += "(t1.obj=t2.sub)"
    } else if (relType == "oo") {
      command = ("select t1.sub as X, t1.obj as Y, t2.sub as Z "
        + "from " + Helper.getPartName(pred1) + " t1 "
        + " join " + Helper.getPartName(pred2) + " t2 "
        + "on (t1.obj=t2.obj)")
    }

    command
  }

  // Triple Table schema
  case class Triple(sub: String, pred: String, obj: String)
  case class vpSchema(sub: String, obj: String)

  //Conn table schema
  case class connSchema(X: String, Y: String, Z: String)
}
