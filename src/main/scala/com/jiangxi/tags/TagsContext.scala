package com.jiangxi.tags

import com.jiangxi.entry.logBean
import com.jiangxi.util.{TagsUtils, rdd2hbase}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapred.{TableInputFormat, TableOutputFormat}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapred.JobConf
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}

object TagsContext {
  def main(args: Array[String]): Unit = {
    /**
      * 第一步判断参数个数
      */
    if(args.length < 2){
      println(
        """
          |com.dmp.total.ProvniceCityAnlyse <inputFilePath><outputFilePath>
          |<inputFilePath> 输入是文件路径
          |<outputFilePath> 输出的文件路径
        """.stripMargin)
      System.exit(0)
    }

    /**
      * 第二步接收参数
      */
    val Array(inputFile,outputFile)=args
    /**
      * 第三步初始化程序入口
      */
    val conf = new SparkConf()
    conf.setAppName(s"${this.getClass.getSimpleName }")
    conf.setMaster("local[4]")
    val spark=SparkSession.builder()
      .config(conf)
      .getOrCreate()
    import org.apache.spark.sql.functions._
    import spark.implicits._
    //app详解数据
    val appDetailData: RDD[String] = spark.sparkContext.textFile("E:\\DMPData\\Inpath\\app_dict.txt")
    //准备广播数据集
    //对数据进行过滤
    val app_id_name: Array[(String, String)] = appDetailData.filter(line => {
      line.split("\t").length >= 5 && line.split("\t")(3).startsWith("A")
    }).map(line => {
      val arr: Array[String] = line.split("\t")
      val appname = arr(1)
      val appid = arr(2)
      (appid, appname)
    }).collect()
    //获取stopword数据（电视剧的种类）
    val stopwordArr: Array[String] = spark.sparkContext.textFile("E:\\DMPData\\Inpath\\stopwords.txt").collect()
    //进行广播
    val app_id_name_Bc: Broadcast[Array[(String, String)]] = spark.sparkContext.broadcast(app_id_name)
    val stopwordBc: Broadcast[Array[String]] = spark.sparkContext.broadcast(stopwordArr)


    //加载需要处理的数据
    val frame: DataFrame = spark.read.parquet(inputFile)
    val value: RDD[(String, List[(String, Int)])] = frame.rdd.map(line => {
      val log: logBean = logBean.line2log(line.toString())
      //广告标签
      val adtag = adTag.makeTag(log)
      //app标签
      val apptag: List[(String, Int)] = appTag.makeTag(log,app_id_name_Bc)
      //渠道标签
      val channeltag: List[(String, Int)] = channelTag.makeTag(log)
      //设备标签
      val devicetag: List[(String, Int)] = deviceTag.makeTag(log)
      //关键字标签
      val keyWordstag: List[(String, Int)] = keyWordsTag.makeTag(log)
      //地域标签
      val areatag: List[(String, Int)] = areaTag.makeTag(log)
      //商圈标签
      val businesstag: List[(String, Int)] = businessTag.makeTag(log)
      //得到userID
     // val userid: String = TagsUtils.getAllUserIds(log)
      val userid: String = TagsUtils.getAllUserId(line).mkString(",")
      val tags = adtag ++ apptag ++ channeltag  ++ devicetag ++ keyWordstag ++ areatag++businesstag
      (userid, tags)
    })
    //value.foreach(println)
  //(IM: -1,List((LC12,1), (LN视频前贴片,1), (APP爱奇艺,1), (CN8,1), (D00010001,1), (D00020005,1), (D00030004,1), (K游戏世界,1), (K单机游戏,1), (ZP浙江省,1), (ZC温州市,1)))
    //首先过滤掉等于空的userid
    val result: RDD[(String, List[(String, Int)])] = value.filter(!_._1.isEmpty).reduceByKey {
      case (list1, list2) => {
        /**
          * var b=Map("aaaa"->1)
          * var a=List("bbbb",1)
          * b++a 后就是下面的形式
          * List((aaaa,1),(bbbb,1))
          */
        //这里的逻辑验证在test里面验证，更加明了
        (list1 ++ list2).groupBy(_._1)
          .map {
            case (key, list) => {
              (key, list.map(x => x._2).sum)
            }
          }.toList
      }
    }
    result.foreach(println)
    //存入Hbase
    val tablename :String ="lhh_test:dmp"
    val hbconf: Configuration = HBaseConfiguration.create()
    hbconf.set("hbase.zookeeper.quorum","node245,node246,node247")
    hbconf.set("hbase.zookeeper.property.clientPort", "2181")
    hbconf.set(TableOutputFormat.OUTPUT_TABLE, tablename)

    //初始化jobconf
    val jobConf = new JobConf(hbconf)
    jobConf.setOutputKeyClass(classOf[ImmutableBytesWritable])
    jobConf.setOutputValueClass(classOf[Put])
    jobConf.setOutputFormat(classOf[TableOutputFormat])
    val hbaseres: RDD[(ImmutableBytesWritable, Put)] = result.map {
      case (userid, tagList) => {
        //rowkey,如果是数值类型，转化成字符串，可能出现乱码
        val put = new Put(Bytes.toBytes(userid.toString))
        val tags = tagList.map(item => item._1 + ":" + item._2).mkString(",")
        put.addImmutable(Bytes.toBytes("cf1"), Bytes.toBytes("day1"), Bytes.toBytes(tags))
        (new ImmutableBytesWritable(), put)
      }
    }
    //hbaseres.saveAsHadoopDataset(jobConf)

    spark.stop()
  }

}
