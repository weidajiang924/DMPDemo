package com.jiangxi.tags

import ch.hsr.geohash.GeoHash
import com.jiangxi.`trait`.Tags
import com.jiangxi.entry.logBean
import com.jiangxi.util.{GaoDeUtil, JedisConnectionPool}

object businessTag extends Tags{
  override def makeTag(args: Any*): List[(String, Int)] = {
    var list =List[(String,Int)]()
    if(args.length>0){
     //获取数据
      val log = args(0).asInstanceOf[logBean]
      //解析出字段
      val longtitude :Double = log.longitude.toDouble   //经度
      val lat: Double = log.lat.toDouble          //纬度

      //经纬度转换成geoHash串
      val geoHash: String = GeoHash.geoHashStringWithCharacterPrecision(longtitude,lat,12)
      //做判断，用户的经纬度是否是一个有效范围内的数据
      //  经度范围  73°33′E至135°05′E。纬度范围：3°51′N至53°33′N
      var business =""
      if(longtitude>=73 && longtitude <=135 && lat>=3 && lat<=53){
        business =  getBusiness(longtitude,lat)
      }//根据geoHash串获取商圈信息，如果没有，发http请求=》高德=》商圈信息=》redis

      //打标签
      if(business!=null && business.isEmpty){
        val businessArr: Array[String] = business.split(",")
        for (elem <- businessArr){
          list :+= (elem,1)
        }
      }
    }
    list
  }

  //查看redis中的商圈信息
def getBusiness (longtitude:Double,lat:Double) ={
  val geoHash = GeoHash.geoHashStringWithCharacterPrecision(longtitude,lat,8)
  val jedis = JedisConnectionPool.getConnection()
  //获取经纬度所对应的商圈信息
  var business: String = jedis.get(geoHash)
  if(business!=null || business.length==0){
  //获取商圈信息
   business = GaoDeUtil.getBusiness(longtitude,lat)
    //存到redis
    jedis.set(geoHash,business)
  }
  business
}
}
