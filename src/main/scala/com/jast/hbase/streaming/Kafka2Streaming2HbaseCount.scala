package com.jast.hbase.streaming

import java.io.{File, FileInputStream}
import java.util
import java.util.Properties

import com.alibaba.fastjson.JSONObject
import com.jast.hbase.data.convert.HbaseDataProcessor
import com.jast.hbase.utils.HbaseUtil
import org.apache.commons.lang.StringUtils
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}



//    spark-submit --class com.jast.hbase.streaming.Kafka2Streaming2Hbase \
//    --master yarn \
//    --deploy-mode client \
//    --executor-memory 2g \
//    --jars $(echo /home/jast/k2s2h/lib/*.jar | tr ' ' ',') \
//    --executor-cores 2 \
//    --driver-memory 2g \
//    --num-executors 2 \
//    --queue default  \
//    --principal hbase@IZHONGHONG.COM \
//    --keytab /home/jast/k2s2h/conf/nb.keytab \
//    --driver-java-options "-Djava.security.auth.login.config=/home/jast/k2s2h/conf/jaas.conf" \
//    --conf "spark.executor.extraJavaOptions=-Djava.security.auth.login.config=/home/jast/k2s2h/conf/jaas.conf" \
//    hbase-data-save-1.0-SNAPSHOT.jar

object Kafka2Streaming2HbaseCount {


  Logger.getLogger("com").setLevel(Level.ERROR) //设置日志级别

  val isLocal = true
  var confPath: String = System.getProperty("user.dir") + File.separator + "conf"
  val VT_WEIBO: String = "zh_ams_ns:vt_weibo"
  val ARTICLE_OVERSEAS: String = "zh_ams_ns:artcile_overseas"
  val WECHAT_ARTICLE: String = "zh_ams_ns:wechat_article"
  val OTHER_ARTICLE: String = "zh_ams_ns:vt_article"
  val COLUMN_FAMILY: String = "fn"
  def main(args: Array[String]): Unit = {


    println("读取配置文件路径:"+confPath + File.separator + "jast.properties")
    //加载配置文件
    val properties = new Properties()
    val file = new File(confPath + File.separator + "jast.properties")
    if(!file.exists()) {
      val in = Kafka2Streaming2Hbase.getClass.getClassLoader.getResourceAsStream("jast.properties")
      properties.load(in);
    } else {
      properties.load(new FileInputStream(file))
    }

    val brokers = properties.getProperty("kafka.brokers")
    val topics = properties.getProperty("kafka.topics")
    val principal = properties.getProperty("principal.account")
    val hbasePrincipal = properties.getProperty("hbase.principal.account")
    val keytabFilePath = properties.getProperty("keytab.filepath")
    val jaasFilePath = properties.getProperty("jaas.filepath")
    println("kafka.brokers:" + brokers)
    println("kafka.topics:" + topics)

    if(isLocal){
      System.setProperty("java.security.auth.login.config", "C:\\Users\\Administrator\\IdeaProjects\\hbasedatasave\\src\\main\\resources\\jaas.conf")
      System.setProperty("hadoop.home.dir", "C:\\hadoop")
      System.setProperty("java.security.krb5.conf", "C:\\Users\\Administrator\\eclipse-workspace\\kafka\\src\\main\\resources\\krb5.conf")
    }
    else{
      println("jin ru ")
      System.setProperty("java.security.auth.login.config", jaasFilePath)
    }

    if(StringUtils.isEmpty(brokers)|| StringUtils.isEmpty(topics) || StringUtils.isEmpty(principal) || StringUtils.isEmpty(keytabFilePath)) {
      println("未配置Kafka和Kerberos信息")
      System.exit(0)
    }
    val topicsSet = topics.split(",").toSet
    var spark: SparkSession = null
    if(isLocal) {
       spark = SparkSession.builder()
        .master("local[4]")
        .appName("Kafka2Spark2HBase-kerberos").
        config(new SparkConf())
        .getOrCreate()
    }else{
       spark = SparkSession.builder()

        .appName("Kafka2Spark2HBase-kerberos").
        config(new SparkConf())
        .getOrCreate()
    }
    val ssc = new StreamingContext(spark.sparkContext, Seconds(5)) //设置Spark时间窗口，每5s处理一次
    val kafkaParams = Map[String, Object]("bootstrap.servers" -> brokers
      , "auto.offset.reset" -> "earliest"
      , "security.protocol" -> "SASL_PLAINTEXT"
      , "sasl.kerberos.service.name" -> "kafka"
      , "key.deserializer" -> classOf[StringDeserializer]
      , "value.deserializer" -> classOf[StringDeserializer]
      , "group.id" -> "testgroup"
    )

    val dStream = KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))

    dStream.foreachRDD(rdd => {
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition(partitionRecords => {
        val o = offsetRanges(TaskContext.get.partitionId)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
        if(o.topic == "test_topic") {
          println("---"+partitionRecords.length)
          partitionRecords.foreach(msg => {
            println("-----" + msg.value())
          })

//          partitionRecords.foreach(HbaseConvert.topicHbaseData)

        }
        if(o.topic == "test_topic") {
          println("+++"+partitionRecords.length)
          partitionRecords.foreach(msg => {
            println("+++++"+msg.value().substring(0,10))
          })
        }

      })
    })
    ssc.start()
    ssc.awaitTermination()
  }


  import java.text.SimpleDateFormat

  def dataFormate(time: Long): String = {
    val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")
    df.format(time)
  }

}