package org.example
import org.apache.spark.sql.{DataFrame, SparkSession}
import java.io.File

object App {

  def main(args: Array[String]) {

    System.setProperty("HADOOP_USER_NAME", "spark")

    val spark = SparkSession.builder()
      .master("spark://spark-master-0.spark-headless.spark.svc.cluster.local:7077")
      .appName("ETL_Training")
      .config("spark.sql.warehouse.dir", "hdfs://192.168.182.17:8020/hive/warehouse")
      .config("hive.metastore.warehouse.dir","hdfs://192.168.182.17:8020/hive/warehouse")
      //.config("spark.sql.hive.hiveserver2.jdbc.url","jdbc:hive2://10.111.86.81:9852/")
      .config("hive.metastore.uris","thrift://192.168.219.111:9850")
      .config("spark.ranger.plugin.authorization.enable","false")
      .enableHiveSupport()
      .getOrCreate()

    println(spark.sparkContext.sparkUser)

   var req = spark.sql("use service")
    //req.show()
     req = spark.sql("create table test(id int , name string)")
    //req = spark.sql("create table table4(id int, name string)")
    //req.show()
    req = spark.sql("show tables")
    req.show()



  }

}