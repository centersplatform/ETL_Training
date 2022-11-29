package org.data_training.utils
import org.apache.spark.sql.DataFrame


trait WriteDFs {
  def write_df_to_hive(df: DataFrame, database: String, table_name: String, save_mode: String, columns_to_write: Seq[String]=Nil): Unit
  def write_df_to_hdfs(df: DataFrame, file_format: String="csv", location_path: String="hdfs://192.168.182.6:8020/hive/warehouse/processEcomData", number_of_partitions: Int=1, columns_to_write: Seq[String]=Nil, options: Map[String,String]): Unit
}
