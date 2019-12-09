package com.jiangxi.entry


import org.apache.commons.lang.StringUtils
case class logBean(
                    val sessionid: String, //会话标识
                    val advertisersid: Int, //广告主id
                    val adorderid: Int, //广告id
                    val adcreativeid: Int, //广告创意id   ( >= 200000 : dsp ,  < 200000 oss)
                    val adplatformproviderid: Int, //广告平台商id  (>= 100000: rtb  , < 100000 : api )
                    val sdkversionnumber: String, //sdk版本号
                    val adplatformkey: String, //平台商key
                    val putinmodeltype: Int, //针对广告主的投放模式,1：展示量投放 2：点击量投放
                    val requestmode: Int, //数据请求方式（1:请求、2:展示、3:点击）
                    val adprice: Double, //广告价格
                    val adppprice: Double, //平台商价格
                    val requestdate: String, //请求时间,格式为：yyyy-m-dd hh:mm:ss
                    val ip: String, //设备用户的真实ip地址
                    val appid: String, //应用id
                    val appname: String, //应用名称
                    val uuid: String, //设备唯一标识，比如imei或者androidid等
                    val device: String, //设备型号，如htc、iphone
                    val client: Int, //设备类型 （1：android 2：ios 3：wp）
                    val osversion: String, //设备操作系统版本，如4.0
                    val density: String, //备屏幕的密度 android的取值为0.75、1、1.5,ios的取值为：1、2
                    val pw: Int, //设备屏幕宽度
                    val ph: Int, //设备屏幕高度
                    val longitude: String, //设备所在经度
                    val lat: String, //设备所在纬度
                    val provincename: String, //设备所在省份名称
                    val cityname: String, //设备所在城市名称
                    val ispid: Int, //运营商id
                    val ispname: String, //运营商名称
                    val networkmannerid: Int, //联网方式id
                    val networkmannername: String, //联网方式名称
                    val iseffective: Int, //有效标识（有效指可以正常计费的）(0：无效 1：有效)
                    val isbilling: Int, //是否收费（0：未收费 1：已收费）
                    val adspacetype: Int, //广告位类型（1：banner 2：插屏 3：全屏）
                    val adspacetypename: String, //广告位类型名称（banner、插屏、全屏）
                    val devicetype: Int, //设备类型（1：手机 2：平板）
                    val processnode: Int, //流程节点（1：请求量kpi 2：有效请求 3：广告请求）
                    val apptype: Int, //应用类型id
                    val district: String, //设备所在县名称
                    val paymode: Int, //针对平台商的支付模式，1：展示量投放(CPM) 2：点击量投放(CPC)
                    val isbid: Int, //是否rtb
                    val bidprice: Double, //rtb竞价价格
                    val winprice: Double, //rtb竞价成功价格
                    val iswin: Int, //是否竞价成功
                    val cur: String, //values:usd|rmb等
                    val rate: Double, //汇率
                    val cnywinprice: Double, //rtb竞价成功转换成人民币的价格
                    val imei: String, //imei
                    val mac: String, //mac
                    val idfa: String, //idfa
                    val openudid: String, //openudid
                    val androidid: String, //androidid
                    val rtbprovince: String, //rtb 省
                    val rtbcity: String, //rtb 市
                    val rtbdistrict: String, //rtb 区
                    val rtbstreet: String, //rtb 街道
                    val storeurl: String, //app的市场下载地址
                    val realip: String, //真实ip
                    val isqualityapp: Int, //优选标识
                    val bidfloor: Double, //底价
                    val aw: Int, //广告位的宽
                    val ah: Int, //广告位的高
                    val imeimd5: String, //imei_md5
                    val macmd5: String, //mac_md5
                    val idfamd5: String, //idfa_md5
                    val openudidmd5: String, //openudid_md5
                    val androididmd5: String, //androidid_md5
                    val imeisha1: String, //imei_sha1
                    val macsha1: String, //mac_sha1
                    val idfasha1: String, //idfa_sha1
                    val openudidsha1: String, //openudid_sha1
                    val androididsha1: String, //androidid_sha1
                    val uuidunknow: String, //uuid_unknow tanx密文
                    val userid: String, //平台用户id
                    val iptype: Int, //表示ip库类型，1为点媒ip库，2为广告协会的ip地理信息标准库，默认为1
                    val initbidprice: Double, //初始出价
                    val adpayment: Double, //转换后的广告消费（保留小数点后6位）
                    val agentrate: Double, //代理商利润率
                    val lomarkrate: Double, //代理利润率
                    val adxrate: Double, //媒介利润率
                    val title: String, //标题
                    val keywords: String, //关键字
                    val tagid: String, //广告位标识(当视频流量时值为视频ID号)
                    val callbackdate: String, //回调时间 格式为:YYYY/mm/dd hh:mm:ss
                    val channelid: String, //频道ID
                    val mediatype: Int
                   )extends  Serializable {}
/**
  * 只要给我们传过来一条数据，我们就可以通过line2Log转换成一个日志对象
  */
object logBean{

  //创建空对象
  def makeLogs(): logBean = {
   new logBean( "",0,0,0,0,""
   ,"",0,0,0.0,0.0,"","",
   "","","","",0,"","",0,0,"",
   "","","",0,"",0,"",0,
     0,0,"",0,0,0,"",
     0,0,0.0,0.0,0,"",0.0,0.0,"",
   "","","","","","","","","",
   "",0,0.0,0,0,"","","","","","",
   "","","","","","",0,0.0,0.0,
     0.0,0.0,0.0,"","","","","",0)
  }

  def line2log(lines:String) : logBean ={
    if (StringUtils.isNotEmpty(lines)){
      val line = lines.split(",")
      if (line.length==85){
       new logBean(
         if(line(0).isEmpty) "-1" else line(1),
         if(line(1).isEmpty) -1 else line(1).toInt,
         if(line(2).isEmpty) -1 else line(2).toInt,
         if(line(3).isEmpty) -1 else line(3).toInt,
         if(line(4).isEmpty) -1 else line(4).toInt,
         if(line(5).isEmpty) "-1" else line(5),
         if(line(6).isEmpty) "-1" else line(6),
         if(line(7).isEmpty) -1 else line(7).toInt,
         if(line(8).isEmpty) -1 else line(8).toInt,
         if(line(9).isEmpty) -1.0 else line(9).toDouble,
         if(line(10).isEmpty) -1.0 else line(10).toDouble,
         if(line(11).isEmpty) "-1" else line(11),
         if(line(12).isEmpty) "-1" else line(12),
         if(line(13).isEmpty) "-1" else line(13),
         if(line(14).isEmpty) "-1" else line(14),
         if(line(15).isEmpty) "-1" else line(15),
         if(line(16).isEmpty) "-1" else line(16),
         if(line(17).isEmpty) -1 else line(17).toInt,
         if (line(18).isEmpty) "-1" else line(18),
         if (line(19).isEmpty) "-1" else line(19),
         if (line(20).isEmpty) -1 else line(20).toInt,
         if (line(21).isEmpty) -1 else line(21).toInt,
         if (line(22).isEmpty) "-1" else line(22),
         if (line(23).isEmpty) "-1" else line(23),
         if (line(24).isEmpty) "-1" else line(24),
         if (line(25).isEmpty) "-1" else line(25),
         if(line(26).isEmpty) -1 else line(26).toInt,
         if (line(27).isEmpty) "-1" else line(27),
         if(line(28).isEmpty) -1 else line(28).toInt,
         if (line(29).isEmpty) "-1" else line(29),
         if(line(30).isEmpty) -1 else line(30).toInt,
         if(line(31).isEmpty) -1 else line(31).toInt,
         if(line(32).isEmpty) -1 else line(32).toInt,
         if (line(33).isEmpty) "-1" else line(33),
         if(line(34).isEmpty) -1 else line(34).toInt,
         if(line(35).isEmpty) -1 else line(35).toInt,
         if(line(36).isEmpty) -1 else line(36).toInt,
         if (line(37).isEmpty) "-1" else line(37),
         if(line(38).isEmpty) -1 else line(38).toInt,
         if(line(39).isEmpty) -1 else line(39).toInt,
         if(line(40).isEmpty) -1.0 else line(40).toDouble,
         if(line(41).isEmpty) -1.0 else line(41).toDouble,
         if(line(42).isEmpty) -1 else line(42).toInt,
         if (line(43).isEmpty) "-1" else line(43),
         if(line(44).isEmpty) -1.0 else line(44).toDouble,
         if(line(45).isEmpty) -1.0 else line(45).toDouble,
         if (line(46).isEmpty) "-1" else line(46),
         if (line(47).isEmpty) "-1" else line(47),
         if (line(48).isEmpty) "-1" else line(48),
         if (line(49).isEmpty) "-1" else line(49),
         if (line(50).isEmpty) "-1" else line(50),
         if (line(51).isEmpty) "-1" else line(51),
         if (line(52).isEmpty) "-1" else line(52),
         if (line(53).isEmpty) "-1" else line(53),
         if (line(54).isEmpty) "-1" else line(54),
         if (line(55).isEmpty) "-1" else line(55),
         if (line(56).isEmpty) "-1" else line(56),
         if(line(57).isEmpty) -1 else line(57).toInt,
         if(line(58).isEmpty) -1.0 else line(58).toDouble,
         if(line(59).isEmpty) -1 else line(59).toInt,
         if(line(60).isEmpty) -1 else line(60).toInt,
         if (line(61).isEmpty) "-1" else line(61),
         if (line(62).isEmpty) "-1" else line(62),
         if (line(63).isEmpty) "-1" else line(63),
         if (line(64).isEmpty) "-1" else line(64),
         if (line(65).isEmpty) "-1" else line(65),
         if (line(66).isEmpty) "-1" else line(66),
         if (line(67).isEmpty) "-1" else line(67),
         if (line(68).isEmpty) "-1" else line(68),
         if (line(69).isEmpty) "-1" else line(69),
         if (line(70).isEmpty) "-1" else line(70),
         if (line(71).isEmpty) "-1" else line(71),
         if (line(72).isEmpty) "-1" else line(72),
         if(line(73).isEmpty) -1 else line(73).toInt,
         if(line(74).isEmpty) -1.0 else line(74).toDouble,
         if(line(75).isEmpty) -1.0 else line(75).toDouble,
         if(line(76).isEmpty) -1.0 else line(76).toDouble,
         if(line(77).isEmpty) -1.0 else line(77).toDouble,
         if(line(78).isEmpty) -1.0 else line(78).toDouble,
         if (line(79).isEmpty) "-1" else line(79),
         if (line(80).isEmpty) "-1" else line(80),
         if (line(81).isEmpty) "-1" else line(81),
         if (line(82).isEmpty) "-1" else line(82),
         if (line(83).isEmpty) "-1" else line(83),
         if(line(84).isEmpty) -1 else line(84).substring(0,1).toInt
       )
     }else{ makeLogs()
     }
    } else {
      //万一没满足条件，我们后面的代码就无法运行了。所以要创建空对象
      makeLogs()
    }
  }
}