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
        scripts{
          gv= load "ecom-script.groovy"
        }
      }
    }
    stage('Build') {
      steps {
        echo 'Building the project...'
        scripts{
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
