pipeline {
    agent any
    stages {
        stage('upload to ecr and build image - CI') {
            steps {
                sh """ aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 992382545251.dkr.ecr.us-east-1.amazonaws.com
                docker build -t app .
                 docker tag app:latest 992382545251.dkr.ecr.us-east-1.amazonaws.com/roy-docker:app
                 docker push 992382545251.dkr.ecr.us-east-1.amazonaws.com/roy-docker:app """
            }
        stage('deploy to container and recive health check') {
            steps {
                sh """ CONTAINER_ID=\$(docker run -d app)
                 docker exec -it $CONTAINER_ID curl localhost:5000/health """
                
        }
    }
}
    }
}
