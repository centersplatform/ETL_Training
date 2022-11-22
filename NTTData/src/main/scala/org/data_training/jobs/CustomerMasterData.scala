package org.data_training.jobs

import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.{SparkSession, types}
import org.apache.spark.sql.types.DateType
import org.data_training.Runnable
import org.data_training.engine.Engine

class CustomerMasterData extends Runnable{

  def run(spark: SparkSession, odate: String, engine: Engine, args: String*): Unit = {

    val schema = types.StructType(Array(
      types.StructField("id", types.StringType, true),
      types.StructField("uniqueId", types.StringType, true),
      types.StructField("zipCode", types.StringType, true),
      types.StructField("city", types.StringType, true),
      types.StructField("state", types.StringType, true),
    ))
    //Read customer_df and remove the " character
    var customer_df=spark.sql("SELECT * FROM ecom1.customers_dataset ")
    customer_df.columns.foreach(col_name => {
      customer_df = customer_df.withColumn(col_name, regexp_replace(customer_df(col_name), "\"", ""))
    })
    // Read orders_df and change type of order_estimated_delivery_date to short date

  }

  def JobsName2Log(): String = {
    "CustomerMasterData"
  }


}
