package Engine

import org.apache.spark.sql.SparkSession

class Engine {

  val spark = SparkSession.builder()
    .master("spark://spark-master-0.spark-headless.spark.svc.cluster.local:7077")
    .appName("Product name")
    .config("spark.sql.warehouse.dir", "hdfs://192.168.235.188:8020/hive/warehouse/eCom.db")
    .enableHiveSupport()
    .getOrCreate();

}
