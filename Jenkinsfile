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
       /* script{
          gv.sparkSubmit()
        }*/
      }
    }
  }
}
