package org.data_training.jobs

import org.data_training.Runnable
import org.apache.spark.sql.functions.{current_timestamp, date_format}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.data_training.Runnable
import org.data_training.engine.Engine

case class Customers() extends Runnable{

var customers_DF: DataFrame = _

  def run (spark: SparkSession, odate: String, engine: Engine): Unit = {
  print ("############## starting Customers JOB ##############")

  customers_DF = spark.sql ("SELECT * FROM customers_dataset ")

  print ("############## processing customers JOB ##############")

  val result = process ()

  print ("############## writing customers JOB ##############")

  result.write.option("delimiter", ",").csv ("/tmp/spark_output/customers.csv")

  print ("##############  Customers JOB Finished ##############")
  }

  def process () = {

  customers_DF.withColumn ("insertion_date", date_format (current_timestamp (), "yyyy-MM-dd") )


  }

  def JobsName2Log (): String = {
  "Customers"
  }


  }
