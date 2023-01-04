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
    stage('Test') {
      steps {
        echo 'Running tests...'
        // run tests here
      }
    }
    stage('Deploy') {
      steps {
        echo 'Deploying the application...'
        // deploy the application here
      }
    }
  }
}
