package GraphActor

import java.io.PrintWriter
import java.util.Calendar
import scala.actors.Actor

class Assembler(startTime: Long, consolePrinter: PrintWriter) extends Actor {
  var totalError: Double = 0
  def act() = {
    while(true){
      receive{
        case error: Double =>{
//          println(s"consolePrinter errorIncrease: $error")
          consolePrinter.println("the error increase of this merge is " + error)
          totalError = totalError + error
        }
        case numberOfSupernodes: Int =>{
          consolePrinter.println("supernodes record: " + numberOfSupernodes)
          consolePrinter.println("the total error is " + totalError)
          val endTime: Long = Calendar.getInstance.getTimeInMillis
          consolePrinter.println("程序运行时长为：" + (endTime - startTime)/1000.0 + "s");
        }
        case STOP =>{
          consolePrinter.println("the total error is " + totalError)
          val endTime: Long = Calendar.getInstance.getTimeInMillis
          consolePrinter.println("程序运行时长为：" + (endTime - startTime)/1000.0 + "s")
          consolePrinter.close()

          exit()
        }
      }
    }
  }
}
