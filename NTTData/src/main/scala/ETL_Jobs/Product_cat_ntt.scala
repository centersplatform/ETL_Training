import Engine.Engine
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{current_timestamp, date_format, lit}
import org.apache.spark.sql

import scala.reflect.io.File.separator

class Product_cat_ntt {

  val s = new Engine()

   def run() ={
    val df = s.spark.sql("SELECT * FROM product_category_nt ")
  }

    def process(df: DataFrame) ={
      df.withColumn("date_insertion", date_format(current_timestamp(), "yyyy-MM-dd"))

      df.write.csv("/tmp/spark_output/data.csv")

    }
}