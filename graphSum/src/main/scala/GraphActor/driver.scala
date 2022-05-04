package GraphActor

object driver {
  var inputPath = ""
  var outputPath = ""
  def main(args: Array[String]): Unit = {
//    inputPath = "D:\\ysData\\dataset\\lubm\\nt\\lubm\\lubm1\\lubm1.nt"
//    outputPath = "./results/query/"
    inputPath = args(0)
    outputPath = args(1)
    Main.run()
  }

}
