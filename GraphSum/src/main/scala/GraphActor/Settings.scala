package GraphActor

object Settings {
  var inputPath = ""
  var fileType = ""
  var actorNum = 1
  var snSavePath = ""
  var seSavePath = ""
  def loadSetting(inputPath: String, fileType: String, actorNum: Int, savePath: String): Unit = {
    this.inputPath = inputPath
    this.fileType = fileType
    this.actorNum = actorNum
//    this.snSavePath = savePath + "/supernodes" +
  }
}
