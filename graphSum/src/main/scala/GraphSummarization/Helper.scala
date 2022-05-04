package GraphSummarization

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.SparkSession

import java.io.{FileNotFoundException, IOException}
import scala.io.Source

object Helper {

  private var prefixes = scala.collection.mutable.Map[String, String]()
  case class Triple(sub: String, pred: String, obj: String)

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

  /*def parseDataset(line: String, prefixes: Broadcast[scala.collection.mutable.Map[String, String]]): Triple = {

    val fields = line.split("\\s+")
    var _subject = ""
    var _predicate = ""
    var _object = ""
    if (fields.length > 2) {
      _subject = getParsedField(fields(0), prefixes.value)
      _predicate = replaceSpacialChar(getParsedField(fields(1), prefixes.value))
      _object = getParsedField(fields.drop(2).mkString(" ").stripSuffix(".").trim, prefixes.value)
    }
    Triple(_subject, _predicate, _object)
  }*/
  def parseDataset(line: String, prefixes: Broadcast[scala.collection.mutable.Map[String, String]]): String = {

    val fields = line.split("\\s+")
    var _subject = ""
    var _predicate = ""
    var _object = ""
    if (fields.length > 2) {
      _subject = getParsedField(fields(0), prefixes.value)
      _predicate = getParsedField(fields(1), prefixes.value)
      _object = getParsedField(fields.drop(2).mkString(" ").stripSuffix(".").trim, prefixes.value)
    }
    _subject+"\t"+_predicate+"\t"+_object
  }

  def replaceSpacialChar(p: String): String = {
    p.replaceAll("<|>", "")
  }

  def getParsedField(field: String, prefixes: scala.collection.mutable.Map[String, String]): String = {

    var _str = Helper.replaceNamespace(field, prefixes)
    if (_str.endsWith("/")) {
      _str = _str.substring(0, _str.length() - 1).trim()
    }
    _str
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
