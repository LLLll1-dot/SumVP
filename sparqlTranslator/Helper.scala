package sparqlTranslator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import java.io.{File, FileNotFoundException, IOException, PrintWriter}
import scala.collection.mutable.HashMap
import scala.io.Source

object Helper {
  private var prefixes = scala.collection.mutable.Map[String, String]()

  //获得子类
  def lubmSubClass(): HashMap[String, Set[String]] ={
    val rdfsSet: HashMap[String, Set[String]] = HashMap()
    rdfsSet.put("ub:Student", Set("ub:GraduateStudent", "ub:UndergraduateStudent"))
    rdfsSet.put("ub:Professor", Set("ub:FullProfessor", "ub:AssociateProfessor", "ub:AssistantProfessor"))
    rdfsSet.put("ub:Person", Set("ub:GraduateStudent", "ub:UndergraduateStudent","ub:FullProfessor", "ub:AssociateProfessor", "ub:AssistantProfessor", "ub:TeachingAssistant", "ub:ResearchAssistant", "ub:Lecturer"))
    rdfsSet.put("ub:Course", Set("ub:Course", "ub:GraduateCourse"))
    rdfsSet.put("ub:Faculty", Set("ub:FullProfessor", "ub:AssociateProfessor", "ub:AssistantProfessor", "ub:TeachingAssistant", "ub:Lecturer", "ub:ResearchAssistant"))
    rdfsSet
  }

  def isFile(filePath: String): Boolean ={
    filePath.endsWith(".txt") || filePath.endsWith(".rq") || filePath.endsWith(".q")
  }

  def getSubDirs(dir: String): Array[String] = {
    val file = new File(dir)
    file.listFiles.filter(_.isFile).map(_.toString)
  }

  def readFile(filePath: String): Array[String] = {
    val file = scala.io.Source.fromFile(filePath)
    val lines = file.getLines().toArray
    file.close()
    lines
  }

  def saveFile(sqlArray: Array[String], fileString: String): Unit ={
    val printWriter = new PrintWriter(new File(fileString))
    sqlArray.foreach(sql => printWriter.println(sql))
    printWriter.close()
  }

  def prefixArray(array: Array[String], spark: SparkSession): Array[String] = {
    val prefixes = Helper.broadcastPrefixes(spark)
    array.map(r => Helper.parseDataset(r.mkString, prefixes))
  }

  def broadcastPrefixes(spark: SparkSession): Broadcast[scala.collection.mutable.Map[String, String]] = {
    try {
      val file = Source.fromFile("./file/prefix_file.txt")
      for (line <- file.getLines if !line.equals("") && !line.startsWith("#")) {
        val arr = line.split("&&")
        prefixes += (arr(1).trim() -> arr(0).trim())
      }
      file.close()
    } catch {
      case e: FileNotFoundException => println("Couldn't find that prefix file.")
      case e: IOException           => println("Got an IOException!")
    }
    val _prefixes = spark.sparkContext.broadcast(prefixes)
    _prefixes
  }
  def parseDataset(line: String, prefixes: Broadcast[scala.collection.mutable.Map[String, String]]): String = {

//    val fields = line.split("\\s+")
    val fields = line.split(" ")
    var _subject = ""
    var _predicate = ""
    var _object = ""
    if (fields.length > 2) {
      _subject = getParsedField(fields(0), prefixes.value)
      _predicate = getParsedField(fields(1), prefixes.value)
      _object = getParsedField(fields.drop(2).mkString(" ").stripSuffix(".").trim, prefixes.value)
    }
    _subject+"\t"+_predicate+"\t"+_object+" ."
  }

  def getParsedField(field: String, prefixes: scala.collection.mutable.Map[String, String]): String = {

    var _str = Helper.replaceNamespace(field, prefixes)
    if (_str.endsWith("/")) {
      _str = _str.substring(0, _str.length() - 1).trim()
    }
    _str
  }
  def replaceSpacialChar(p: String): String = {
    p.replaceAll("[<>]", "")
  }
  def replaceNamespace(field: String, prefixes: scala.collection.mutable.Map[String, String]): String = {
    var _field = ""

    if (isURI(field)) {
      prefixes foreach {
        case (key, value) =>
          if (field.contains(key)) {
            _field = replaceSpacialChar(field.replace(key, value))
            return _field
          }
      }
      _field = field
      _field
    } else {
      _field = field
      _field
    }

  }
  def isURI(field: String): Boolean = {
    if (field.contains("<") && field.endsWith(">")) {
      true
    } else {
      false
    }
  }

}
