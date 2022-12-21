package org.data_training.engine

trait Constant {
  val JobsList : List[String] = List("Customers","Hdfs_into_postgres","LoadDataToDW","CustomerMasterData")
  val file_format: String = "csv"
  val location_path: String ="hdfs://192.168.182.6:8020/hive/warehouse/processEcomData"
  val number_of_partitions: Int = 1
  val hdfs_host_server: String= "hdfs://192.168.182.6:8020"
  val spark_master: String="spark://spark-master-0.spark-headless.spark.svc.cluster.local:7077"
  val app_name: String= "ETL_Training"
  val spark_warehouse_dir: String= "hdfs://192.168.182.6:8020/hive/warehouse"
  val hive_metastore_dir: String= "hdfs://192.168.182.6:8020/hive/warehouse"
  val hive_metastore_uris: String= "thrift://192.168.219.114:9850"
  val load_hdfs_to_dw_settings: String= "/jobs_conf/CONFIG-HDFS_To_DW.yaml"
  val jdbc_connection_string: String= "jdbc:postgresql://10.102.86.9:5432/"
  val postgres_driver: String= "org.postgresql.Driver"
  val postgres_user_name: String= "postgres"
  val postgres_password: String= "abJIbg3d53"
}
