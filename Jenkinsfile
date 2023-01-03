pipeline {
  agent any
  tools {
    maven 'maven-3.8.1'
  }
  stages {
    stage('Build') {
      steps {
        echo 'Building the project...'
        sh "cd NTTData && mvn package"
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
