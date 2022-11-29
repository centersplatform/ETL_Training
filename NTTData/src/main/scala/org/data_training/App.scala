package org.data_training

import org.data_training.engine.Engine
import scala.collection.mutable.ListBuffer
import org.data_training.engine.Constant


/**
 * @author ${user.name}
 */
object App extends Constant{
  //main function
  def main(args : Array[String]) {
    // initialization of  SparkSession
    val engine =  new Engine()
    val spark = engine.init_spark()

    val JobsNames = args(0)
    val JobsToBeExecuted = getJobsFromInput(JobsNames)
    
    println(s"Classes to be executed : $JobsNames")

    JobsToBeExecuted.foreach{ runnableJOB =>
      try{
        //runnableJOB.JobsName2Log() = JobsNames
        runnableJOB.run(spark, engine)
      }catch{
        case exception : Exception => print(" Job "+ runnableJOB.JobsName2Log()+ " failed. /n" + exception)
      }
    }
  }

   def getJobsFromInput(class_to_execute: String): List[Runnable] ={
     val ETL_JobsList = ListBuffer[Runnable]()
     val list_of_classes_to_execute:List[String]=class_to_execute.split(",").map(_.trim).toList
     list_of_classes_to_execute.foreach(item => {
       if (JobsList.exists(jobs_name=>jobs_name==item)) {
        val runClass = Class.forName("org.data_training.jobs." + item).newInstance().asInstanceOf[Runnable]
        ETL_JobsList += runClass
        }
      })
     ETL_JobsList.toList
  }
}
