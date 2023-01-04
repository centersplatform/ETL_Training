def buildFunc(){
  sh "cd NTTData && mvn clean package"
}

return this
