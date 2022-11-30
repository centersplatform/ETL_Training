package org.data_training.engine

trait Constant {
  val JobsList : List[String] = List("Customers","Hdfs_into_postgres","LoadDataToDW")
  val file_format: String = "csv"
  val location_path: String ="hdfs://192.168.182.6:8020/hive/warehouse/processEcomData"
  val number_of_partitions: Int = 1
  val hdfs_host_server: String= "hdfs://192.168.182.6:8020"
  val spark_master: String="spark://spark-master-0.spark-headless.spark.svc.cluster.local:7077"
  val app_name: String= "ETL_Training"
  val spark_warehouse_dir: String= "hdfs://192.168.182.6:8020/hive/warehouse"
  val hive_metastore_dir: String= "hdfs://192.168.182.6:8020/hive/warehouse"
  val hive_metastore_uris: String= "thrift://192.168.219.114:9850"
}
