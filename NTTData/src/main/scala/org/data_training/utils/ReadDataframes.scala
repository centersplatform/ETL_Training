package org.data_training.utils

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, count, when}
import org.apache.spark.sql.types.StructType
import org.data_training.utils.*


class ReadDataframes(spark: SparkSession) extends ReadDFs {

  protected var df: DataFrame
  override def read_hive_df(database: String, table_name: String, clause: String="", columns_to_read: List[String]=Nil): DataFrame = {
    if (columns_to_read.isEmpty) {
      df = spark.sql(s"SELECT * FROM $database.$table_name $clause")
    }
    else {
      val columns_to_read_concat = columns_to_read.mkString(",")
      df = spark.sql(s"SELECT $columns_to_read_concat FROM $database.$table_name $clause")
    }
    // Print Nulls summary
    df.select(df.columns.map(col_name => count(when(col(col_name).isNull, col_name)).alias(col_name)): _*).show()
    // return
    df
  }

  override def read_hdfs_df(file_path: String, file_format: String, schema: StructType,options: Map[String,String]=Nil, clause: String="", columns_to_read: List[String]=Nil): DataFrame = {

    if (options.isEmpty) {
      df = spark.read.format(file_format).schema(schema).load(file_path)
    }
    else {
      df = spark.read.format(file_format).options(options).schema(schema).load(file_path)
    }

    df.createOrReplaceTempView("df_temp_table")
    if (columns_to_read.isEmpty) {
      df = spark.sql(s"SELECT * FROM df_temp_table $clause")
    }
    else {
      val columns_to_read_concat = columns_to_read.mkString(",")
      df = spark.sql(s"SELECT $columns_to_read_concat FROM df_temp_table $clause")
    }
    spark.catalog.dropTempView("df_temp_table")
    /*if (!columns_to_read.isEmpty){
      df=df.select(columns_to_read.map(col):_*)
    }*/
    // Print Nulls summary
    df.select(df.columns.map(col_name => count(when(col(col_name).isNull, col_name)).alias(col_name)): _*).show()
    // return
    df
  }

  override def read_postgresql_df(): Unit = Unit
}
