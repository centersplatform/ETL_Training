import ETL_Jobs.Engine
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{current_timestamp, date_format, lit}
import org.apache.spark.sql

class Product_cat_ntt {

  val s = new Engine()


  val df = s.spark.sql("SELECT * FROM product_category_nt ")


  df.withColumn("date_insertion", date_format(current_timestamp(), "yyyy-MM-dd"))

  df.write.csv("/tmp/spark_output/data.csv")

}