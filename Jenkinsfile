pipeline {
    agent any
    stages {
        stage('upload to ecr and build image') {
            steps {
                sh """ aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 992382545251.dkr.ecr.us-east-1.amazonaws.com
                 docker build -t app .
                 docker tag app:latest 992382545251.dkr.ecr.us-east-1.amazonaws.com/roy-docker:app
                 docker push 992382545251.dkr.ecr.us-east-1.amazonaws.com/roy-docker:app
                 docker run --name app -d app
                 docker exec -it app curl localhost/5000/health && python3 -m unittest discover -s tests -v  """
        }
    }
    }
}
