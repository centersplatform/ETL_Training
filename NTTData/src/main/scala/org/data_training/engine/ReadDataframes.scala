package org.data_training.engine

import org.apache.spark.sql.functions.{col, count, when}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.utils.ReadDFs
import java.nio.file.Paths
//import org.data_training.utils.ReadDFs


class ReadDataframes(spark: SparkSession) extends ReadDFs with Constant {

  protected var df: DataFrame=_
  override def read_hive_df(database: String, table_name: String, clause: String="", columns_to_read: List[String]=Nil): DataFrame = {
    println(s"------------- Reading data from HIVE, Database: $database, Table Name: $table_name ------------------")
    if (columns_to_read.isEmpty) {
      df = spark.sql(s"SELECT * FROM $database.$table_name $clause")
    }
    else {
      val columns_to_read_concat = columns_to_read.mkString(",")
      df = spark.sql(s"SELECT $columns_to_read_concat FROM $database.$table_name $clause")
    }
    // Print Nulls summary
    println(s"-------------- Count of Nulls by column, Number of records ${df.count()} --------------")
    df.select(df.columns.map(col_name => count(when(col(col_name).isNull, col_name)).alias(col_name)): _*).show()
    // return
    df
  }

  override def read_hdfs_df(file_path: String, file_format: String= file_format, schema: StructType= new StructType(),options: Map[String,String]=Map(), clause: String="", columns_to_read: List[String]=Nil): DataFrame = {
    println(s"------------- Reading data from HDFS, File Name: ${Paths.get(file_path).getFileName} ------------------")
    if (options.isEmpty) {
      if(schema.fieldNames.isEmpty) {
        df = spark.read.format(file_format).load(file_path)
      }
      else {
        df = spark.read.format(file_format).schema(schema).load(file_path)
      }
    }
    else {
      if(schema.fieldNames.isEmpty) {
        df = spark.read.format(file_format).options(options).load(file_path)
      }
      else{
        df = spark.read.format(file_format).options(options).schema(schema).load(file_path)
      }
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
    println(s"-------------- Count of Nulls by column, Number of records ${df.count()} --------------")
    df.select(df.columns.map(col_name => count(when(col(col_name).isNull, col_name)).alias(col_name)): _*).show()
    // return
    df
  }

  override def read_postgresql_df(database: String, table_name: String, clause: String="", columns_to_read: List[String]=Nil, fetch_size: Int=10000): DataFrame={
    /**In Order to manipulate the Postgres Dataframe you need to add the Postgresql jar to all the available spark workers
     */
    println(s"------------- Reading data from PostgresQl, Database: $database, Table Name: $table_name ------------------")
    val url_postgres= jdbc_connection_string + database
    val connection_properties = new java.util.Properties()
    connection_properties.put("user", postgres_user_name)
    connection_properties.put("password", postgres_password)
    connection_properties.put("driver", postgres_driver)
    connection_properties.put("fetchsize", fetch_size.toString)

    var query =""
    if (columns_to_read.isEmpty){
      query = s"(SELECT * FROM $table_name $clause) as $table_name"
    }
    else{
      val columns_to_read_concat = columns_to_read.mkString(",")
      query = s"(SELECT $columns_to_read_concat FROM $table_name $clause) as $table_name"
    }
    df= spark.read.jdbc(url_postgres, query, connection_properties)

    // Print Nulls summary
    println(s"-------------- Count of Nulls by column, Number of records ${df.count()} --------------")
    df.select(df.columns.map(col_name => count(when(col(col_name).isNull, col_name)).alias(col_name)): _*).show()
    // return
    df
  }
}
