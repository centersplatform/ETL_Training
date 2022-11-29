package org.data_training.jobs
import org.data_training.engine.Engine
import org.apache.spark.sql.SparkSession
import org.data_training.Runnable
import java.io.File


class MoveHDFSToHive extends Runnable {
  def run (spark : SparkSession, engine: Engine ,args: String*): Unit={
    println("----------- Looping Through HDFS Directory ----------------")
    new File("hdfs://192.168.182.6:8020/ecomData").listFiles.filter(_.getName.endsWith(".csv")).foreach(file_name=> println(file_name))
  }

  def JobsName2Log(): String = {
    "MoveHDFSToHive"
  }
}
