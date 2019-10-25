package com.jast.hbase.utils
import java.io.{File, FileInputStream}
import java.security.PrivilegedAction
import java.util

import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Put}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.security.UserGroupInformation

import scala.util.Try

object HbaseUtil {

  def main(args: Array[String]): Unit = {

  }
  /**
   * HBase 配置文件路径
   * @param confPath
   * @return
   */
  def getHBaseConn(confPath: String, principal: String, keytabPath: String): Connection = {
    println("-----------------------------------------"+principal)
    println("-----------------------------------------"+keytabPath)
    val configuration = HBaseConfiguration.create

    val coreFile = new File(confPath + File.separator + "core-site.xml")
    if(!coreFile.exists()) {
      val in = HbaseUtil.getClass.getClassLoader.getResourceAsStream("hbase-conf/core-site.xml")
      configuration.addResource(in)
    }else{
      configuration.addResource(new FileInputStream(coreFile))
    }
    val hdfsFile = new File(confPath + File.separator + "hdfs-site.xml")
    if(!hdfsFile.exists()) {
      val in = HbaseUtil.getClass.getClassLoader.getResourceAsStream("hbase-conf/hdfs-site.xml")
      configuration.addResource(in)
    }else{
      configuration.addResource(new FileInputStream(hdfsFile))
    }
    val hbaseFile = new File(confPath + File.separator + "hbase-site.xml")
    if(!hbaseFile.exists()) {
      val in = HbaseUtil.getClass.getClassLoader.getResourceAsStream("hbase-conf/hbase-site.xml")
      configuration.addResource(in)
    }else{
      configuration.addResource(new FileInputStream(hbaseFile))
    }

    UserGroupInformation.setConfiguration(configuration)
    UserGroupInformation.loginUserFromKeytab(principal, keytabPath)

    val loginUser = UserGroupInformation.getLoginUser
    loginUser.doAs(new PrivilegedAction[Connection] {
      override def run(): Connection = ConnectionFactory.createConnection(configuration)
    })
  }

  /**
   * @TODO　插入数据
   * @param connection
   * @param tableName
   * @param rowkey
   * @param columnFamily
   * @param columns
   * @return
   */
  def putData(connection:Connection,tableName:String,rowkey:String,columnFamily:String,columns:util.HashMap[String,String]):Boolean={
    val put = new Put(Bytes.toBytes(rowkey))
    val table = connection.getTable(TableName.valueOf(tableName))
    import scala.collection.JavaConversions._
    for (key <- columns.keySet) {
      put.addColumn(Bytes.toBytes(columnFamily), Bytes.toBytes(key), Bytes.toBytes(columns.get(key)))
    }
    Try(table.put(put)).getOrElse(table.close())//将数据写入HBase，若出错关闭table
    table.close()
    true
  }





}
