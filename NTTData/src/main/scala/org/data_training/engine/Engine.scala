package org.data_training.engine

import org.apache.spark.sql.{DataFrame, SparkSession}
import javax.xml.crypto.Data


class Engine {
  def init_spark(): SparkSession ={
    val spark = SparkSession.builder()
      .master("spark://spark-master-0.spark-headless.spark.svc.cluster.local:7077")
      .appName("ETL_Training")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.182.6:8020/hive/warehouse")
      .config("hive.metastore.warehouse.dir", "hdfs://192.168.182.6:8020/hive/warehouse")
      //.config("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
      .config("hive.metastore.uris", "thrift://192.168.219.114:9850")
      .enableHiveSupport()
      .getOrCreate();
    spark
  }



}
