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
    JobsToBeExecuted.foreach{ runnableJOB =>
      try{
        //runnableJOB.JobsName2Log() = JobsNames
        runnableJOB.run(spark,"",engine)
      }catch{
        case exception : Exception => print(" Job "+ runnableJOB.JobsName2Log()+ " failed. /n" + exception)
      }
    }

    print("hello")

  }

   def getJobsFromInput(inputargs: String): List[Runnable] ={
     val ETL_JobsList = ListBuffer[Runnable]()
    JobsList.foreach(item => {

      val runClass = Class.forName("org.data_training.jobs."+item).newInstance().asInstanceOf[Runnable]
      ETL_JobsList += runClass

    })
     ETL_JobsList.toList

  }

}
