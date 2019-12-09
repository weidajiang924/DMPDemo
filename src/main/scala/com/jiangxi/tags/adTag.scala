package com.jiangxi.tags

import com.jiangxi.`trait`.Tags
import com.jiangxi.entry.logBean
import org.apache.spark.sql.Row
/**
  * 1）广告位类型（标签格式：LC03->1或者LC16->1）xx为数字，小于10 补0
  */
object adTag extends Tags{
  override def makeTag(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()
    //在scala中强制转换类型使用asInstanceOf
    val bean: logBean = args(0).asInstanceOf[logBean]
    //adspacetype广告位类型（1：banner 2：插屏 3：全屏）
    if (bean.adspacetype!=0 && bean.adspacetype!=null){
      bean.adspacetype match{
        case x if x < 10 => list :+=("LC0"+x -> 1)
        case x if x > 9  => list :+=("LC"+x -> 1)
      }
    }
    //广告位类型
    if(bean.adspacetypename != null){
      list :+=("LN"+bean.adspacetypename -> 1)
    }
    list
  }
}
/*
val bean: logBean = args(0).asInstanceOf[logBean]
if (bean.adspacetype!=0 && bean.adspacetype!=null){
bean.adspacetype match{
case x if x < 10 => list :+=("LC0"+x -> 1)
case x if x > 9  => list :+=("LC"+x -> 1)
}
}

list*/
