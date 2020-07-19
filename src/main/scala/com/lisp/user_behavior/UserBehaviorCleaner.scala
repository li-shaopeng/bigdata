package com.lisp.user_behavior

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object UserBehaviorCleaner {
  def main(args: Array[String]): Unit = {
    if(args.length != 2){
      println("Please input 2args")
      System.exit(1)
    }
    val inputPath = args(0)
    val outputPath = args(1)

    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val sc = new SparkContext(conf)
    //通过输入获取RDD
    val eventRDD: RDD[String] = sc.textFile(inputPath)

    eventRDD.filter(event => checkEventValid(event))
      .map(event => maskPhone(event))
      .map(event => repairUsername(event))
      .coalesce(3)
      .saveAsTextFile(outputPath)

  }

  /**
    * username 用户自定义，可能存在"\n"
    * @param event
    */
  def repairUsername(event:String) ={
    val fields = event.split("\t")
    val username = fields(1)

    if(username != null && !"".equals(username)){
      fields(1) = username.replace("\n", "")
    }
    fields.mkString("\t")
  }

  /**
    * 手机脱敏
    * @param event
    * @return
    */
  def maskPhone(event:String) ={
    var mashPhone = new StringBuilder
    val fields: Array[String] = event.split("\t")

    var phone = fields(9)
    //手机号不为空时做掩码处理
    if(phone != null && !"".equals(phone)){
      mashPhone = mashPhone.append(phone.substring(1,3)).append("XXXX").append(phone.substring(7,11))
      fields(9) = mashPhone.toString()
    }
    fields.mkString("\t")
  }

  def checkEventValid(event:String) ={
    val fields: Array[String] = event.split("\t")
    fields.length == 17
  }
}
