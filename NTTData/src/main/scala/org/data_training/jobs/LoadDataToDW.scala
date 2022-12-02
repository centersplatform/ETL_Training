package org.data_training.jobs

import org.data_training.engine.{Constant, Engine, ReadDataframes, WriteDataframes}
import org.apache.spark.sql.SparkSession
import org.data_training.Runnable
import org.apache.hadoop.fs.{FileSystem, Path}
import java.net.URI
import org.apache.hadoop.conf.Configuration
import org.yaml.snakeyaml.Yaml
import java.util.Map
import collection.JavaConverters.mapAsScalaMapConverter
import collection.JavaConverters._


class LoadDataToDW extends Runnable with Constant {
  def run (spark : SparkSession, engine: Engine ,args: String*): Unit={
    println(s"-------------- Loading HDFS Files ----------------")
    var fs = FileSystem.get(new URI(hdfs_host_server), new Configuration())
    val yaml_fileStream =fs.open( new Path(load_hdfs_to_dw_settings))

    val yaml= new Yaml()
    val conf_data= yaml.load(yaml_fileStream).asInstanceOf[java.util.Map[String, Any]]
    val list_hdfs_to_load= conf_data.get("HDFS_FILE_PATHS").asInstanceOf[java.util.ArrayList[String]]
    println(s"----------- Print Settings: $conf_data -------------")
    println(s"----------- Print files paths: $list_hdfs_to_load -------------")
    val hdfs_options= conf_data.get("OPTIONS").asInstanceOf[java.util.Map[String,String]].asScala.toMap
    println(s"----------- Print Options: $hdfs_options ,type: ${hdfs_options.getClass}--------------")
    val columns_name= conf_data.get("COLUMNS_NAME").asInstanceOf[java.util.ArrayList[String]].asScala
    println(s"----------- Print Field's name $columns_name --------------")
    val header_option= hdfs_options.getOrElse("header", "true").toBoolean
    println(s"----------- Print Header Option $header_option,type: ${header_option.getClass} -------------")

    val readDFObj = new ReadDataframes(spark = spark)
    val writeDFObj = new WriteDataframes(spark = spark)
    val extension=conf_data.get("FILE_FORMAT").asInstanceOf[String]

    list_hdfs_to_load.forEach(file_path =>{
      var full_path = hdfs_host_server+file_path
      if (full_path.endsWith(extension)){
        println(s"----------- Reading $file_path -------------")
        var df= readDFObj.read_hdfs_df(file_path = full_path,file_format = extension, options = hdfs_options)
        if (!header_option){
          println("--------- Changing columns name ----------")
          df=df.toDF(columns_name.map(col_name=>col_name):_*)
        }
        df.show(5)
      }
    })
  }

  def JobsName2Log(): String = {
    "LoadDataToDW"
  }
}
