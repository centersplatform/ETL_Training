def buildFunc(){
  sh "cd NTTData && mvn clean package"
}

def cpJarToSpark(){
  sshagent(credentials:['ssh-test']){       
        sh "ssh  -o StrictHostKeyChecking=no  admin@38.242.220.77  kubectl cp /srv/nfs/kubedata/jenkins-jenkins-pvc-a6da9cc2-3bef-4500-a284-8178a2ccf9ea/workspace/ecom-dev-pipeline_master/NTTData/target/NTTData-1.0-SNAPSHOT.jar spark-master-0:tmp -n spark"
    } 
}
def submitJob(){
  sshagent(credentials:['ssh-test']){       
        sh "ssh  -o StrictHostKeyChecking=no  admin@38.242.220.77  kubectl get pods -n spark"
    }
}


return this
