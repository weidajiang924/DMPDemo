package com.jiangxi.util

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

import scala.collection.mutable.ListBuffer

object GaoDeUtil {

  def getBusiness(longtitude:Double,lat:Double): String ={
    val buffer = ListBuffer[String]()
    val location= longtitude+","+lat
    val url = "https://restapi.amap.com/v3/geocode/regeo?key=422a9950c65b1fd84c9f762025e94cfe&location="+location
    //发送http请求,返回响应内容，json字符串
    val json: String = HttpUtil.get(url)
    val jSONObject: JSONObject = JSON.parseObject(json)
    val status: Int = jSONObject.getIntValue("status")
    if(status == 0) return null
      else {
        val regecodeJson: JSONObject = jSONObject.getJSONObject("regeocode")  //	逆地理编码列表
        if(regecodeJson != null){
          val addressJson: JSONObject = regecodeJson.getJSONObject("addressComponent")  //地址元素列表
          if(addressJson != null) {
            val busiarray: JSONArray = addressJson.getJSONArray("businessAreas")  //经纬度所属商圈列表
            if(busiarray != null){
              for(item<-busiarray.toArray){
                if(item.isInstanceOf[JSONObject]){
                  val json = item.asInstanceOf[JSONObject]
                  buffer.append(json.getString("name"))
                }
              }
            }
          }
        }
      }
//字段参考    https://lbs.amap.com/api/webservice/guide/api/georegeo
    val str= buffer.mkString(",")
    str
    }
  def main(args: Array[String]): Unit = {
    val str: String = getBusiness(116.310003,39.99195)
    println(str)
  }

}
