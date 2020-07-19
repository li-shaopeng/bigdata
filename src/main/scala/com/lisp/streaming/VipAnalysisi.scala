package com.lisp.streaming

import java.text.SimpleDateFormat
import java.util.Properties

import net.ipip.ipdb.{City, CityInfo}

object VipAnalysisi {
  //提取出公共芰量，转換算子共用
  val sdf = new SimpleDateFormat("yyyy-MM-dd")
  //从properties文件里获取各种参数
  val prop = new Properties()
  prop.load(this.getClass.getClassLoader().getResourceAsStream("VipIncrementAnalysis.properties"))
  //使用静态ip资源库
  val ipdb = new City(this .getClass().getClassLoader().getResource("ipipfree.ipdb").getPath())
  //获取jdbc相关参数
  val driver= prop.getProperty("jdbcDriver")
  val jdbcUrl = prop.getProperty("jdbc.url")
  val jdbcUser = prop.getProperty("jdbc.user")
  val jdbcPassword = prop.getProperty("jdbc.password")
  // 设置;jdbc
  Class.forName(driver)
  // 设 置 连 接 池
  //  ConnectionPool.singleton(jdbcUrl, jdbcUser,jdbcPassword)
  def main(args: Array[String]): Unit = {
    val ip = "112.23.6.6"
    var region = "未知"
    val info: CityInfo = ipdb.findInfo(ip,"CN")
    if(info != null){
      region = info.getRegionName()
    }
    println(region)
  }

}
