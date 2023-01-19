package org.data_training.jobs

import org.apache.spark.sql.functions.{col, regexp_replace, date_format, to_timestamp, sum, count, round, broadcast}
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.types.{DateType, DayTimeIntervalType}
import org.data_training.Runnable
import org.data_training.engine.{Engine, ReadDataframes, WriteDataframes}
import org.apache.spark.sql.DataFrame
import org.data_training.engine.Constant
import org.apache.spark.sql.Row


class CustomerMasterData extends Runnable with Constant {
  def run (spark : SparkSession, engine: Engine ,args: String*): Unit={
    println("-------------- Process Data And Create A PostgresQl DW ------------------")
    val n="25512.225".indexOf(".")
    val writeDFObj = new WriteDataframes(spark = spark)
    val readDFObj = new ReadDataframes(spark = spark)

    // Read and process orders table
    val orders_df= readDFObj.read_hive_df(database = "ecomdatadb",table_name = "orders_table",
      columns_to_read = List("order_id","customer_id","order_status","order_purchase_timestamp"))
    val processed_orders_df= orders_df.withColumn("order_purchase_date", date_format(to_timestamp(col("order_purchase_timestamp")),"yyyy-MM-dd")).
      withColumn("order_purchase_time",date_format(to_timestamp(col("order_purchase_timestamp")),"hh:mm:ss a")).
      drop(col("order_purchase_timestamp")).
      withColumn("order_purchase_date", col("order_purchase_date").cast("date")).
      withColumnRenamed("customer_id","customer_unique_id")

    processed_orders_df.printSchema()

    // Read and process orders items table
    val order_items_df= readDFObj.read_hive_df(database = "ecomdatadb", table_name = "order_items_table")
    val processed_order_items_df= order_items_df.groupBy(col("order_id"),	col("product_id"),	col("seller_id"), col("shipping_limit_date")).
      agg(round(sum("price"),2).as("total_price"),
          round(sum("freight_value"),2).as("total_freight_value"),
          count("order_item_id").as("qty")).
      withColumn("shipping_limit_short_date", date_format(col("shipping_limit_date"), "yyyy-MM-dd")).
      withColumn("shipping_limit_time", date_format(col("shipping_limit_date"), "hh:mm:ss a")).
      drop(col("shipping_limit_date")).
      withColumnRenamed("shipping_limit_short_date","shipping_limit_date").
      withColumn("shipping_limit_date", col("shipping_limit_date").cast("date"))

    processed_order_items_df.printSchema()
    processed_order_items_df.filter(col("order_id")==="00143d0f86d6fbd9f9b38ab440ac16f5").show()

    // Read and Process orders payment table
    val order_payments_df= readDFObj.read_hive_df(database = "ecomdatadb", table_name = "order_payments_table",
      columns_to_read = List("order_id", "payment_type", "payment_installments", "payment_value"))
    val processed_order_payments_df= order_payments_df.withColumn("payment_installments",col("payment_installments").cast("long")).
      withColumn("payment_value", col("payment_value").cast("double"))

    processed_order_payments_df.printSchema()

    // Read and Process order's reviews table
    val order_reviews_df = readDFObj.read_hive_df(database = "ecomdatadb", table_name = "order_reviews_table",
      columns_to_read = List("review_id", "order_id", "review_score"))
    val processed_order_reviews_df= order_reviews_df.withColumn("review_score", col("review_score").cast("int"))

    processed_order_reviews_df.printSchema()

    // Order Master DF
    val master_orders_df= processed_orders_df.join(processed_order_items_df,Seq("order_id"),"left").
      join(processed_order_payments_df, Seq("order_id"),"left").
      join(processed_order_reviews_df, Seq("order_id"), "left")
    master_orders_df.printSchema()

    println(s"------------- Number of records in the master orders DF is : ${master_orders_df.count()} -----------------")

    // Read and Process products table
    val products_df= readDFObj.read_hive_df(database = "ecomdatadb", table_name = "products_table")
    val processed_products_df= products_df./*na.fill(0.0f, Seq("product_name_lenght","product_description_lenght",
      "product_photos_qty","product_weight_g","product_length_cm","product_height_cm","product_width_cm")).*/
      withColumn("product_name_lenght",col("product_name_lenght").cast("float")).
      withColumn("product_description_lenght",col("product_description_lenght").cast("float")).
      withColumn("product_photos_qty",col("product_photos_qty").cast("float")).
      withColumn("product_weight_g",col("product_weight_g").cast("float")).
      withColumn("product_length_cm",col("product_length_cm").cast("float")).
      withColumn("product_height_cm",col("product_height_cm").cast("float")).
      withColumn("product_width_cm",col("product_width_cm").cast("float"))

    processed_products_df.printSchema()

    // Read and Process product category name translation table
    val product_category_name_df= broadcast(readDFObj.read_hive_df(database = "ecomdatadb", table_name = "product_category_name_translation_table"))
    product_category_name_df.printSchema()

    // Products Master DF
    val master_products_df= processed_products_df.join(product_category_name_df,Seq("product_category_name"),"left")
    master_products_df.printSchema()

    // Read and Process geolocation
    /*val geolocation_df= readDFObj.read_hive_df(database = "ecomdatadb", table_name = "geolocation_table", columns_to_read = List("geolocation_zip_code_prefix", "geolocation_lat", "geolocation_lng"))
    val processed_geolocation_df = geolocation_df.withColumn("geolocation_zip_code_prefix", col("geolocation_zip_code_prefix").cast("int")).
      withColumn("geolocation_lat", col("geolocation_lat").cast("double")).
      withColumn("geolocation_lng", col("geolocation_lng").cast("double"))
    processed_geolocation_df.printSchema()*/

    // Read and Process customers table
    val customers_df= readDFObj.read_hive_df(database = "ecomdatadb", table_name = "customers_table")
    val processed_customers_df= customers_df.withColumn("customer_zip_code_prefix", col("customer_zip_code_prefix").cast("int"))
    processed_customers_df.printSchema()

    // Master customers DF
    /*val master_customers_df= processed_customers_df.join(processed_geolocation_df, processed_customers_df("customer_zip_code_prefix")===processed_geolocation_df("geolocation_zip_code_prefix"), "left").
      withColumnRenamed("geolocation_lng","customer_geolocation_lng").
      withColumnRenamed("geolocation_lat","customer_geolocation_lat").
      drop("geolocation_zip_code_prefix")*/

    // Read and Process sellers table
    val sellers_df = readDFObj.read_hive_df(database = "ecomdatadb", table_name = "sellers_table")
    val processed_sellers_df = sellers_df.withColumn("seller_zip_code_prefix", col("seller_zip_code_prefix").cast("int"))
    processed_sellers_df.printSchema()

    // Master customers DF
    /*val master_sellers_df = processed_sellers_df.join(processed_geolocation_df, processed_sellers_df("seller_zip_code_prefix") === processed_geolocation_df("geolocation_zip_code_prefix"), "left").
      withColumnRenamed("geolocation_lng", "seller_geolocation_lng").
      withColumnRenamed("geolocation_lat", "seller_geolocation_lat").
      drop("geolocation_zip_code_prefix")*/


    // Master DF
    println(s"-------------- Creating Master DF with the following schema ------------------")
    val master_df= master_orders_df.join(master_products_df, Seq("product_id"), "left").
      join(processed_customers_df, Seq("customer_unique_id"),"left").
      join(processed_sellers_df, Seq("seller_id"), "left")

    master_df.printSchema()
    println(s"------------- Number of records in the master DF is : ${master_df.count()} -----------------")

    println(s"-------------- List all the databases in PostgresQL ------------------")
    val jdbc_url = jdbc_connection_string+"postgres"
    val connection_properties = new java.util.Properties()
    connection_properties.put("user", postgres_user_name)
    connection_properties.put("password", postgres_password)
    connection_properties.put("driver", postgres_driver)

    val list_postgresql_databases= "(SELECT datname FROM pg_database WHERE datistemplate = false) AS databases"
    var databasesDF = spark.read.jdbc(jdbc_url, list_postgresql_databases, connection_properties)
    databasesDF.show()

    var isPostgresDbCreated= databasesDF.filter(col("datname")===postgres_db_datawarehouse).count()==1
    println(s"-------------- Is the PostgresQl Database Name: $postgres_db_datawarehouse was created: $isPostgresDbCreated ------------------")
    if (!isPostgresDbCreated) {
      println(s"-------------- Create PostgresQl Database Name: $postgres_db_datawarehouse ------------------")
      val create_postgresqlDB_dw = s"CREATE DATABASE $postgres_db_datawarehouse"
      val connection = java.sql.DriverManager.getConnection(jdbc_url, connection_properties)
      val statement = connection.createStatement()
      try {
        statement.executeUpdate(create_postgresqlDB_dw)
      }
      finally {
        connection.close()
      }
      databasesDF = spark.read.jdbc(jdbc_url, list_postgresql_databases, connection_properties)
      isPostgresDbCreated= databasesDF.filter(col("datname")).count()==1
      println(s"-------------- Is the PostgresQl Database Name: $postgres_db_datawarehouse was created: $isPostgresDbCreated ------------------")
    }

    println(s"-------------- Create PostgresQl Table Name: $postgres_table_datawarehouse, Inside Database Name: $postgres_db_datawarehouse ------------------")
    val list_postgresql_tables= "(SELECT table_name FROM information_schema.tables WHERE table_schema='public') AS tables"
    var tablesDwDF = spark.read.jdbc(jdbc_connection_string+postgres_db_datawarehouse, list_postgresql_tables, connection_properties)

    var isPostgresTableCreated = tablesDwDF.filter(col("table_name") === postgres_table_datawarehouse).count() == 1
    println(s"-------------- Is the PostgresQl Table Name: $postgres_table_datawarehouse was created: $isPostgresTableCreated ------------------")
    if (!isPostgresTableCreated) {
      println(s"-------------- Create PostgresQl Table Name: $postgres_table_datawarehouse ------------------")
      val new_schema_postgres_table_df = master_df.sqlContext.createDataFrame(spark.sparkContext.emptyRDD[Row], master_df.schema)
      //val column_types = new_schema_postgres_table_df.schema.fields.map(field => s"${field.name} ${field.dataType.typeName}").mkString(", ")
      new_schema_postgres_table_df.write
        .mode("overwrite")
        //.option("createTableColumnTypes", column_types)
        .jdbc(jdbc_connection_string + postgres_db_datawarehouse, postgres_table_datawarehouse, connection_properties)

      tablesDwDF = spark.read.jdbc(jdbc_connection_string+postgres_db_datawarehouse, list_postgresql_tables, connection_properties)
      isPostgresTableCreated = tablesDwDF.filter(col("table_name") === postgres_table_datawarehouse).count() == 1
      println(s"-------------- Is the PostgresQl Table Name: $postgres_table_datawarehouse was created: $isPostgresTableCreated ------------------")
    }
    tablesDwDF.show()

    println(s"-------------- Write master dataframe Data to PostgresQL, Database: $postgres_db_datawarehouse, Table: $postgres_table_datawarehouse ------------------")
    writeDFObj.write_df_to_postgresql(df = master_df, database = postgres_db_datawarehouse, table_name = postgres_table_datawarehouse, batch_size = 10000, num_partition = 12, max_pool_size = 4)

    println(s"-------------- Read Data from PostgresQL, Database: $postgres_db_datawarehouse, Table: $postgres_table_datawarehouse ------------------")
    val df: DataFrame= readDFObj.read_postgresql_df(database = postgres_db_datawarehouse, table_name = postgres_table_datawarehouse)
    df.show(10)
  }

  def JobsName2Log(): String = {
    "CustomerMasterData"
  }
}
