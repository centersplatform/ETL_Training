package org.data_training.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.utils.WriteDFs

class WriteDataframes(spark: SparkSession) extends WriteDFs {
  override def write_df_to_hive(spark: SparkSession, df: DataFrame, database: String, table_name: String, save_mode: String, columns_to_write: List[String]=Nil): Unit = {
    if (!columns_to_write.isEmpty){
      df=df.select(columns_to_write.map(col):_*);
    }
    df.write.mode(save_mode).saveAsTable(s"$database.$table_name");
  }

  override def write_df_to_hdfs(spark: SparkSession, df: DataFrame, file_format: String, location_path: String, number_of_partitions: Int, columns_to_write: List[String]=Nil, options: Map[String,String]): Unit = {
    if (!columns_to_write.isEmpty) {
      df = df.select(columns_to_write.map(col): _*);
    }
    df.repartition(number_of_partitions).write.options(options).format(file_format).save(location_path);
  }
}
