package org.data_training.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.data_training.utils.WriteDFs
import org.apache.spark.sql.functions.col


class WriteDataframes(spark: SparkSession) extends WriteDFs {
  var df_to_write: DataFrame=_
  import spark.implicits._
  override def write_df_to_hive(df: DataFrame, database: String, table_name: String, save_mode: String, columns_to_write: Seq[String]=Nil): Unit = {
    df_to_write=df
    if (!columns_to_write.isEmpty){
      //df_to_write=df.select(columns_to_write.map(col):_*);
      println(s"------------- Selecting Columns: Columns to select: $columns_to_write -----------")
      val col_names = columns_to_write.map(name => col(name))
      df_to_write = df.select(col_names: _*)
    }
    println(s"------------- Writing Hive : DataBase= $database, Table Name= $table_name -----------")
    df_to_write.write.mode(save_mode).saveAsTable(s"$database.$table_name");
  }

  override def write_df_to_hdfs(df: DataFrame, file_format: String="csv", location_path: String="hdfs://192.168.182.6:8020/hive/warehouse/processEcomData", number_of_partitions: Int=1, columns_to_write: Seq[String]=Nil, options: Map[String,String]): Unit = {
    df_to_write=df
    if (!columns_to_write.isEmpty) {
      //df_to_write = df.select(columns_to_write.map(col): _*);
      println(s"------------- Selecting Columns: Columns to select: $columns_to_write -----------")
      val col_names = columns_to_write.map(name => col(name))
      df_to_write = df.select(col_names: _*)
    }
    println(s"------------- Writing HDFS : format= $file_format, Number of Partitions= $number_of_partitions -----------")
    df_to_write.repartition(number_of_partitions).write.options(options).format(file_format).save(location_path);
  }
}
