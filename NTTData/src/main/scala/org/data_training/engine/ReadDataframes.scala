package org.data_training.engine

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.regexp_replace
import org.data_training.Runnable
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col,when, count};

class ReadDataframes() {

  def apply(spark: SparkSession, table_name: String): DataFrame ={
    //Read table
    var table_DF = spark.sql ("SELECT * FROM ecom1."+ table_name)
    // Remove the "
        table_DF.columns.foreach(col_name => {
       table_DF = table_DF.withColumn(col_name, regexp_replace(table_DF(col_name), "\"", ""))
    })
    // Count records and nulls
    table_DF.select(table_DF.columns.map(col_name=>count(when(col(col_name).isNull,col_name)).alias(col_name)):_*).show()
    table_DF


  }



}
