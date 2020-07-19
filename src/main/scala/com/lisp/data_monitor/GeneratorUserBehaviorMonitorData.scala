package com.lisp.data_monitor

import java.io.PrintWriter
import java.text.SimpleDateFormat
import java.util.Date

object GeneratorUserBehaviorMonitorData {



  def main(args: Array[String]): Unit = {
    if(args.length != 1){
      println("Please input date like 2020-07-15")
      System.exit(1)
    }
    generatorMonitorData(args(0))
  }


  //想要的数据: 教育视频方面的（）
  // （uid:string, username:string, gender:string, lever:tinyint, is_vip:tinyint, os:string, channel:string,
  // net_config:string, ip:string, phone:string, video_id:int, video_length:int,
  // start_video_time:bigint, end_video_time:bigint, version:string, event_key:string, event_time:bigint）
  def generatorMonitorData(data: String): Unit = {
    //后五位自动补齐
    val initPhoto = 188000
    val sdf = new SimpleDateFormat("yyyy-MM-dd")
    val eventTime = sdf.parse(data)
    val initTimestamp = eventTime.getTime / 1000

    writeMonitorData2File("watchVideo", false, 10000, 10000, initPhoto, initTimestamp)
    writeMonitorData2File("completeVideo", true, 20001, 8000, initPhoto, initTimestamp)
    writeMonitorData2File("startHomework", true, 30001, 7000, initPhoto, initTimestamp)
    writeMonitorData2File("completeHomework", true, 40001, 6000, initPhoto, initTimestamp)
    writeMonitorData2File("enterOrderPage", true, 50001, 4000, initPhoto, initTimestamp)
    writeMonitorData2File("completeOrder", true, 60001, 2000, initPhoto, initTimestamp)




  }

  def writeMonitorData2File(dataType: String, isCompleteVideo: Boolean, initUid: Int, userAccount: Int, initPhoto: Int, initTimestamp: Long): Unit = {
    val writer: PrintWriter = new PrintWriter(s"./inputPath/${dataType}_${initTimestamp}.txt")

    //获取开始看视频事件和结束视频事件和事件发生事件
    val (startVideoTime, endVideoTime, eventTime) = getVideoTimeAndEventTime(initTimestamp, isCompleteVideo)

    for (uid <- initUid until(initUid + userAccount)){
      //手机号拼接
      val phone = initPhoto + "" + uid
      // （uid:string, username:string, gender:string, lever:tinyint, is_vip:tinyint, os:string, channel:string,
      // net_config:string, ip:string, phone:string, video_id:int, video_length:int,
      // start_video_time:bigint, end_video_time:bigint, version:string, event_key:string, event_time:bigint）
      val event = dataType match {
        case "watchVideo" =>
          s"""|$uid\t$uid\tF\t2\t0\tSymbian\tauto\t4G\t27.129.32.0\t$phone\t1\t300\t$startVideoTime\t0\t1.0\tstartVideo\t$eventTime
              |$uid\t$uid\tF\t2\t0\tSymbian\tauto\t4G\t27.129.32.0\t$phone\t1\t300\t$startVideoTime\t$endVideoTime\t1.0\tendVideo\t$eventTime\n""".stripMargin
        case "completeVideo" =>
          s"""|$uid\t$uid\tM\t2\t0\tios\tauto\twifi\t127.129.32.0\t$phone\t0\t0\t0\t0\t1.0\tregisterAccount\t$eventTime
              |$uid\t$uid\tM\t2\t0\tios\tauto\twifi\t127.129.32.0\t$phone\t0\t0\t0\t0\t1.0\tstartApp\t$eventTime
              |$uid\t$uid\tM\t2\t0\tios\tauto\twifi\t127.129.32.0\t$phone\t1\t300\t$startVideoTime\t0\t1.0\tstartVideo\t$eventTime
              |$uid\t$uid\tM\t2\t0\tios\tauto\twifi\t127.129.32.0\t$phone\t1\t300\t$startVideoTime\t$endVideoTime\t1.0\tendVideo\t$eventTime\n""".stripMargin
        case "startHomework" =>
          s"""|$uid\t$uid\tM\t2\t0\tios\thuawei\twifi\t58.129.32.0\t$phone\t0\t0\t0\t0\t1.0\tregisterAccount\t$eventTime
              |$uid\t$uid\tM\t2\t0\tios\thuawei\twifi\t58.129.32.0\t$phone\t0\t0\t0\t0\t1.0\tstartApp\t$eventTime
              |$uid\t$uid\tM\t2\t0\tios\thuawei\twifi\t58.129.32.0\t$phone\t1\t300\t$startVideoTime\t0\t1.0\tstartVideo\t$eventTime
              |$uid\t$uid\tM\t2\t0\tios\thuawei\twifi\t58.129.32.0\t$phone\t1\t300\t$startVideoTime\t$endVideoTime\t1.0\tendVideo\t$eventTime
              |$uid\t$uid\tM\t2\t0\tios\thuawei\twifi\t58.129.32.0\t$phone\t1\t0\t0\t0\t1.0\tstartHomework\t$endVideoTime\n""".stripMargin
        case "completeHomework" =>
                s"""|$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t58.129.32.0\t$phone\t0\t0\t0\t0\t1.0\tregisterAccount\t$eventTime
                    |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t58.129.32.0\t$phone\t0\t0\t0\t0\t1.0\tstartApp\t$eventTime
                    |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t58.129.32.0\t$phone\t1\t300\t$startVideoTime\t0\t1.0\tstartVideo\t$eventTime
                    |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t58.129.32.0\t$phone\t1\t300\t$startVideoTime\t$endVideoTime\t1.0\tendVideo\t$eventTime
                    |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t58.129.32.0\t$phone\t1\t0\t0\t0\t1.0\tstartHomework\t$eventTime
                    |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t58.129.32.0\t$phone\t1\t0\t0\t0\t1.0\tcompleteHomework\t$eventTime\n""".stripMargin
        case "enterOrderPage" =>
                s"""|$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t58.129.32.0\t$phone\t0\t0\t0\t0\t1.0\tregisterAccount\t$eventTime
                    |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t58.129.32.0\t$phone\t0\t0\t0\t0\t1.0\tstartApp\t$eventTime
                    |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t58.129.32.0\t$phone\t1\t300\t$startVideoTime\t0\t1.0\tstartVideo\t$eventTime
                    |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t58.129.32.0\t$phone\t1\t300\t$startVideoTime\t$endVideoTime\t1.0\tendVideo\t$eventTime
                    |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t58.129.32.0\t$phone\t1\t0\t0\t0\t1.0\tstartHomework\t$eventTime
                    |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t58.129.32.0\t$phone\t1\t0\t0\t0\t1.0\tcompleteHomework\t$eventTime
                    |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t58.129.32.0\t$phone\t1\t0\t0\t0\t1.0\tenterOrderPage\t$eventTime\n""".stripMargin
        case "completeOrder" =>
          s"""|$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t48.129.32.0\t$phone\t0\t0\t0\t0\t2.0\tregisterAccount\t$eventTime
              |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t48.129.32.0\t$phone\t0\t0\t0\t0\t2.0\tstartApp\t$eventTime
              |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t48.129.32.0\t$phone\t1\t300\t$startVideoTime\t0\t2.0\tstartVideo\t$eventTime
              |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t48.129.32.0\t$phone\t1\t300\t$startVideoTime\t$endVideoTime\t2.0\tendVideo\t$eventTime
              |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t48.129.32.0\t$phone\t1\t0\t0\t0\t2.0\tstartHomework\t$eventTime
              |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t48.129.32.0\t$phone\t1\t0\t0\t0\t2.0\tcompleteHomework\t$eventTime
              |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t48.129.32.0\t$phone\t1\t0\t0\t0\t2.0\tenterOrderPage\t$eventTime
              |$uid\t$uid\tM\t2\t0\tios\ttoutiao\twifi\t48.129.32.0\t$phone\t1\t0\t0\t0\t2.0\tcompleteOrder\t$eventTime}\n""".stripMargin

      }
      writer.write(event)
    }
    writer.close()
  }

  def getVideoTimeAndEventTime(initTimestamp: Long, isCompleteVideo: Boolean) = {
    val startVideoTime = initTimestamp

    val endVideoTime = if(isCompleteVideo) initTimestamp +300 else initTimestamp +100

    val eventTime = tranTimeToString(initTimestamp)
    (startVideoTime, endVideoTime, eventTime)
  }
  def tranTimeToString(timestamp:Long) :String={
    val fm = new SimpleDateFormat("yyyyMMdd")
    val time = fm.format(new Date(timestamp.toLong * 1000))
    time
  }


}
