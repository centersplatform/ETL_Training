package org.data_training.engine

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.utils.WriteDFs
import org.apache.spark.sql.functions.col


class WriteDataframes(spark: SparkSession) extends WriteDFs with Constant {
  var df_to_write: DataFrame=_
  import spark.implicits._
  override def write_df_to_hive(df: DataFrame, database: String, table_name: String, save_mode: String, columns_to_write: Seq[String]=Nil): Unit = {
    df_to_write=df
    if (df_to_write.count()!=0) {
      if (!columns_to_write.isEmpty) {
        //df_to_write=df.select(columns_to_write.map(col):_*);
        println(s"------------- Selecting Columns: Columns to select: $columns_to_write -----------")
        val col_names = columns_to_write.map(name => col(name))
        df_to_write = df.select(col_names: _*)
      }
      println(s"------------- Writing Hive : DataBase= $database, Table Name= $table_name, Save Mode: $save_mode, Number of records to write: ${df_to_write.count()} -----------")
      df_to_write.write.mode(save_mode).format("hive").saveAsTable(s"$database.$table_name")
    }
  }

  override def write_df_to_hdfs(df: DataFrame, file_format: String=file_format, location_path: String=location_path, number_of_partitions: Int=number_of_partitions, columns_to_write: Seq[String]=Nil, options: Map[String,String]): Unit = {
    df_to_write=df
    if (df_to_write.count()!=0) {
      if (!columns_to_write.isEmpty) {
        //df_to_write = df.select(columns_to_write.map(col): _*);
        println(s"------------- Selecting Columns: Columns to select: $columns_to_write -----------")
        val col_names = columns_to_write.map(name => col(name))
        df_to_write = df.select(col_names: _*)
      }
      println(s"------------- Writing HDFS : format= $file_format, Number of Partitions= $number_of_partitions, , Number of records to write: ${df_to_write.count()} -----------")
      df_to_write.repartition(number_of_partitions).write.options(options).format(file_format).save(location_path);
    }
  }

  override def write_df_to_postgresql(df: DataFrame, database: String, table_name: String, mode: String="append",max_pool_size: Int=10, batch_size: Int=1000, num_partition: Int=10): Unit = {
    df_to_write=df
    println(s"------------- Writing data to PostgresQl, Database: $database, Table Name: $table_name, Mode: $mode, Number of records to write: ${df_to_write.count()} ------------------")
    /*
    * batchsize option is used to specify the number of rows to be written in each batch.
    This can help reduce the number of round trips to the database and improve the performance of the write operation.
    * maxPoolSize option is used to Enable connection pooling.
    This can help reduce the overhead of creating and closing connections to the database and improve the performance of the write operation.
    * numPartitions option is used to specify the number of partitions to use when writing data to a database.
    */
    val url_postgres= jdbc_connection_string + database
    val connection_properties = new java.util.Properties()
    connection_properties.put("user", postgres_user_name)
    connection_properties.put("password", postgres_password)
    connection_properties.put("driver", postgres_driver)
    connection_properties.put("maxPoolSize", max_pool_size.toString)
    connection_properties.put("batchsize", batch_size.toString)
    connection_properties.put("numPartitions", num_partition.toString)

    df_to_write.write
      .mode(mode)
      .jdbc(url_postgres, table_name, connection_properties)
  }
}
