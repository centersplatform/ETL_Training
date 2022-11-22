package org.data_training.jobs

import org.data_training.Runnable
import org.apache.spark.sql.functions.{current_timestamp, date_format, regexp_replace}
import org.apache.spark.sql.{DataFrame, SparkSession, types}
import org.data_training.Runnable
import org.data_training.engine.Engine

case class Customers() extends Runnable{

var customers_DF: DataFrame = _

  def run (spark: SparkSession, odate: String, engine: Engine,args: String*): Unit = {
  print ("############## starting Customers JOB ##############")
    val schema = types.StructType(Array(
      types.StructField("id", types.StringType, true),
      types.StructField("uniqueId", types.StringType, true),
      types.StructField("zipCode", types.StringType, true),
      types.StructField("city", types.StringType, true),
      types.StructField("state", types.StringType, true),
    ))

    customers_DF = spark.sql ("SELECT * FROM ecom1.customers_dataset ")
    customers_DF.columns.foreach(col_name => {
      customers_DF= customers_DF.withColumn(col_name, regexp_replace(customers_DF(col_name), "\"", ""))
    })
    customers_DF.show(5)
  println ("############## processing customers JOB ##############")

  val result = process ()

  println ("############## writing customers JOB ##############")
    result.show(3)
  result.coalesce(1).write.option("delimiter", ",").csv ("hdfs://192.168.182.6:8020/hive/warehouse/processEcomData/customers_dataset_final")

  println ("##############  Customers JOB Finished ###s###########")
    println(s"working dir: ${System.getProperty("user.dir")}")
  }

  def process () = {

  customers_DF.withColumn ("insertion_date", date_format (current_timestamp (), "yyyy-MM-dd") )


  }

  def JobsName2Log (): String = {
  "Customers"
  }


  }
