package com.jast.hbase.data.convert

import java.util

import com.alibaba.fastjson.JSONObject
import com.jast.hbase.streaming.Kafka2Streaming2Hbase.{ARTICLE_OVERSEAS, COLUMN_FAMILY, OTHER_ARTICLE, VT_WEIBO, WECHAT_ARTICLE, dataFormate}
import com.jast.hbase.utils.HbaseUtil
import org.apache.hadoop.hbase.client.Connection
import org.apache.kafka.clients.consumer.ConsumerRecord

object HbaseDataProcessor {

  def topicHbaseData(msg :ConsumerRecord[String,String],connection: Connection): Unit = {
    val jo: JSONObject = com.alibaba.fastjson.JSON.parseObject(msg.value())
    val `type` = jo.getString("type")
    val jsonArray = jo.getJSONArray("data")
    val size = jsonArray.size()
    for (i <- 0 until size) {
      var row = ""
      val map = new util.HashMap[String, String]()
      //  print( "i:"+i + " size:"+size+"\t")
      val dataObject: JSONObject = jsonArray.getJSONObject(i)
      val containsKey = dataObject.containsKey("mid")
      var isWechat = false
      val article_type = dataObject.getString("article_type")
      if (article_type != null && article_type.equals("2")) isWechat = true
      //println(dataObject.getString("mid") +"|"+article_type+ "|" + dataObject)
      if (containsKey) {
        row = dataObject.getString("mid")
        import scala.collection.JavaConversions._
        for (value <- dataObject.keySet) {
          if (!(value == "key")) if (value == "created_at") {
            val str1 = dataFormate(dataObject.getLongValue("created_at"))
            map.put(value, str1)
          }
          else map.put(value, dataObject.getString(value))
        }
      }

      if (`type`.equals("weibo")) {
        HbaseUtil.putData(connection, VT_WEIBO, row, COLUMN_FAMILY, map)
      } else if (`type`.equals("article")) {
        if (article_type == null)
          println(dataObject)
        if (isWechat) {
          HbaseUtil.putData(connection, WECHAT_ARTICLE, row, COLUMN_FAMILY, map)
        } else if (article_type.equals("9") && article_type != null) {
          HbaseUtil.putData(connection, ARTICLE_OVERSEAS, row, COLUMN_FAMILY, map)
        } else {
          HbaseUtil.putData(connection, OTHER_ARTICLE, row, COLUMN_FAMILY, map)
        }
      }
    }
  }
}
