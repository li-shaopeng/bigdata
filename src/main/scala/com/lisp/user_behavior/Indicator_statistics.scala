package com.lisp.user_behavior

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
case class userBehavior (uid:String, username:String, gender:String, lever:Byte, is_vip:Byte, os:String, channel:String,
net_config:String, ip:String, phone:String, video_id:Int, video_length:Int,
start_video_time:Long, end_video_time:Long, version:String, event_key:String, event_time:Long){}
object Indicator_statistics {
  def main(args: Array[String]): Unit = {
    // 获取SparkSession，并支持Hive操
    val conf: SparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    val spark: SparkSession = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._

    //查询hive数据
    val sqlStr:String = " "

    spark.sql(sqlStr).as[userBehavior].rdd

    /*
教育视频指标统计：
1、观看视频人数以及完成观察人数:
select count(1) AS ac, uid, event_time from dwd.user_behavior
where event_key = "startVideo" and event_time ="20200718" group by uid

2、各系统 版本统计  os version acess  _count dt
select count(1) AS ac, os, version from dwd.user_behavior
where event_time ="20200718" group by os,version,

3、渠道新用户
select  channel, count(1) AS ac,"20200718" as event_time from dwd.user_behavior
where event_key = "registerAccount" and event_time ="20200718" group by channel

4、访问次数分布 -session
先建立一个统计表
先creat,table

insert overwrite table dwd.user_behavior_cnt
select  uid,count(1) AS access_cnt,"20200718" as event_time from dwd.user_behavior
where  event_time ="20200718" group by uid

select sum(le_tow) as le_tow,sum(le_four) as le_four,sum(gt_four) as gt_four from(

select  count(1) AS le_tow,0 AS le_four,0 AS gt_four,"20200718" as event_time from dwd.user_behavior_cnt
where access_cnt <= 2 and event_time ="20200718" group by event_time
Union all
select  0 AS le_tow,count(1) AS le_four,0 AS gt_four,"20200718" as event_time from dwd.user_behavior_cnt
where access_cnt <= 4 and access_cnt > 2 and event_time ="20200718" group by event_time
Union all
select  0 AS le_tow,0 AS le_four,count(1) AS gt_four,"20200718" as event_time from dwd.user_behavior_cnt
where access_cnt > 4 and event_time ="20200718" group by event_time
)tmp1  group by event_time

5、漏斗分析
insert overwrite table tmp.app_study_funnel_analysis_${day}
select count(distinct tl.uid) as start_app_cnt,count(distinct t2.uid) as start_video_cnt,count(distinct t3.uid) as complete_video_cnt,count(distinct t4.uid) as start_Homework_cnt,count(distinct t5.uid) as complete_HomeWork_cnt
(select uid,dt from dwd.user_behavior where dt =${day} and event_Jcey = "startApp") tl
left join
(select uid from dwd.user_behavior where dt =${day} and event_key = "startVideo") t2
on tl.uid = t2.uid
left join
(select uid from dwd.user_behavior where dt = ${day} and event_key = "endVideo" and (end_video_time - start_video_time) >= 300
on t2.uid = t3.uid
left join
(select uid from dwd.user_behavior where dt = ${day} and event_key = "startHomework") t4
on t3.uid = t4.uid
left join
(select uid from dwd,user_behavior where dt = ${day} and event_key = "completeHomework") t5
on t4.uid = t5.uid group by tl.dt

6、7日留存分析(概念！)：datediff(from_unixtime( ,"yyyy-MM-dd"),from_unixtime( ,"yyyy-MM-dd"))
次日留存率：（第一天新增用户数，第2天还登录的用户数）/第一天总注册用户数
7日留存率：（第一天新增用户数，第8天还登录的用户数）/第一天总注册用户数
30日留存率：（第一天新增用户数，第31天还登录的用户数）/第一天总注册用户


次日留存：当日登录后，第二天也登录了，比如12.10登录过，12.11登录的算作次日留存
三日留存：当日登录后，第三天也登录了，比如12.10登录过，12.12登录的算作三日留存
七日留存：当日登录后，第七天也登录了，比如12.10登录过，12.16登录的算作三日留存

//获取7日全部注册用户
//近7日每天活跃的用户列表,最近的活跃日期
select  uid,max(event_time) as event_time from dwd.user_behavior_cnt
where event_time between${startDay} and ${endDay} group by uid
     */
  }
}
