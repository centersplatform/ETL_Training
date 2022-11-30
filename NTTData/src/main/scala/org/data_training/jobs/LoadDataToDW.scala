package org.data_training.jobs
import org.data_training.engine.{Constant, Engine, ReadDataframes, WriteDataframes}
import org.apache.spark.sql.SparkSession
import org.data_training.Runnable
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import org.apache.hadoop.conf.Configuration
import upickle._
import os._


class LoadDataToDW extends Runnable with Constant {
  def run (spark : SparkSession, engine: Engine ,args: String*): Unit={
    println(s"-------------- Loading HDFS Directory ----------------")
    val path_name= args(1)
    val config_f= os.read(path_name)
    val config_data = ujson.read(config_f, false)
    println(config_data.value)
    val paths_value=config_data("HDFS_FILE_PATHS")
    println(s"---- Print files paths: $paths_value")
    /*
    val list_of_files = FileSystem.get(new URI(hdfs_host_server), new Configuration()).
                        listStatus(new Path(path_name))

    val readDFObj = new ReadDataframes(spark = spark)
    val writeDFObj = new WriteDataframes(spark = spark)

    list_of_files.foreach(file_path =>{
      var file_name= file_path.getPath().getName()
      if (file_name.endsWith(extension) && file_path.isFile()){
        println(s"------------ Reading $file_name -------------")
        var df= readDFObj.read_hdfs_df(file_path = file_path.getPath().toString(),file_format = extension)
        df.show(5)
      }
    })*/
  }

  def JobsName2Log(): String = {
    "LoadDataToDW"
  }
}
