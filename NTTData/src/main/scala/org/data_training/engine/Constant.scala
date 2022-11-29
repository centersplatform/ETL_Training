package org.data_training.engine

trait Constant {
  val JobsList : List[String] = List("Customers","Hdfs_into_postgres","MoveHDFSToHive")
  val file_format: String = "csv"
  val location_path: String ="hdfs://192.168.182.6:8020/hive/warehouse/processEcomData"
  val number_of_partitions: Int = 1
  
}
