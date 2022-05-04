package GraphSummarization
import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql

/**
  * @Author lqs
  * @Date 2022/5/2 23:17
  * @Function 功能简介
  */
object TxtChange {

  implicit class RDDExtensions(val df: sql.DataFrame) extends AnyVal {

    def Txt2Parquet(path: String, name: String, num: Int): Unit = {

      //df转rdd
      val rdd: RDD[String] = df.rdd.map(x=>x.toString())

      //临时文件夹
      val path1 = s"$path/$name";

      //转换
      val hdfs = FileSystem.get(rdd.sparkContext.hadoopConfiguration)
      if(num > 0){
        df.repartition(num).write.option("compression","snappy").parquet(path1)
      }else{
        df.repartition(2).write.format("parquet").option("compression","snappy").save(path1)
      }

      //copy 文件并且临时文件夹
      val pt = new Path(s"$path1/part*.parquet")
      val fileStatus = hdfs.globStatus(pt)
      val files = FileUtil.stat2Paths(fileStatus)
      for(i <- files){
        FileUtil.copy(
          hdfs,i,
          hdfs,new Path(path),
          false,rdd.sparkContext.hadoopConfiguration
        )
        hdfs.delete(new Path(s"$path/."+i.getName+".crc"), true)
      }

      hdfs.delete(new Path(path1), true)
    }
  }

}
