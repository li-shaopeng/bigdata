package com.lisp.user_behavior

import java.net.URI
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.SparkSession
import org.apache.hadoop.fs._

object tes {
  def main(args: Array[String]): Unit = {
  //    classOf["com.mysql.jdbc.Driver"]
  val te = 1594742400L
  println(tranTimeToString(te))
  //idea上传文件到hdfs
    val configuration = new Configuration()
    configuration.set("dfs.replication", "3")

    val fs: FileSystem = FileSystem.get(new URI("hdfs://hadoop102:9000"), configuration, "lisp")
    fs.copyFromLocalFile(new Path("./inputPath/"), new Path("/user/hive/warehouse/ods.db/origin_user_behavior/20200715"))
    fs.close()

  }
  def tranTimeToLong(tm:String) :Long={
    val fm = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val dt = fm.parse(tm)
    val aa = fm.format(dt)
    val tim: Long = dt.getTime()
    tim
  }

  def tranTimeToString(timestamp:Long) :String={
    val fm = new SimpleDateFormat("yyyyMMdd")
    val time = fm.format(new Date(timestamp.toLong * 1000))
    time
  }


}

