package org.data_training.jobs

import org.apache.spark.sql.{SparkSession}
import org.data_training.Runnable
import org.data_training.engine.{Engine, ReadDataframes, WriteDataframes}
import org.data_training.engine.Constant


class TestLogs extends Runnable with Constant {
  def run (spark : SparkSession, engine: Engine ,args: String*): Unit={
    println("-------------- Process Data And Create A PostgresQl DW ------------------")
    val readDFObj = new ReadDataframes(spark = spark)

    // Read and process orders table
    val orders_df= readDFObj.read_hive_df(database = "ecomdatadb",table_name = "orders_table",
      columns_to_read = List("order_id","customer_id","order_status","order_purchase_timestamp"))
    orders_df.show(20)
  }

  def JobsName2Log(): String = {
    "TestLogs"
  }
}
