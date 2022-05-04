package GraphActor

object driver {
  var inputPath = ""
  var outputPath = ""
  def main(args: Array[String]): Unit = {
    inputPath = args(0)
    outputPath = args(1)
    Main.run()
  }

}
