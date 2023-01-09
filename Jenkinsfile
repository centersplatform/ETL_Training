def gv 

pipeline {
  agent any
  tools {
    maven 'maven-3.8.1'
  }
  stages {
    stage('init'){
      steps{
        echo "Loading the groovy script..."
        script{
          gv= load "ecom-script.groovy"
        }
      }
    }
    stage('Build') {
      steps {
        echo 'Building the project...'
        script{
          gv.buildFunc() 
        }
      }
    }
    
    stage('Copy jat to Spark') {
      steps {
        echo 'Copying the jar file from local fs to Spark pod'
        script{
          gv.cpJarToSpark()
        }

      }
    }
    stage('Spark-submit') {
      steps {
        echo 'submitting the Spark job'
        script{
           sshagent(credentials:['ssh-test']){       
        sh "ssh  -o StrictHostKeyChecking=no  admin@38.242.220.77  kubectl exec -it spark-master-0 -n spark  -- spark-submit --master spark://spark-master-svc:7077 --class org.data_training.App tmp/NTTData-1.0-SNAPSHOT.jar LoadDataToDW --executor-memory 10g --driver-memory 10g"
    }
        }
      }
    }
  }
}
