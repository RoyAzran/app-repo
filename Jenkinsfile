pipeline {
    agent any
    stages {
        stage('ci') {
            when {
              expression { (branch == 'testing') 
            }
            steps {
                echo 'deploying...'
                sh """
                    aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 992382545251.dkr.ecr.us-east-1.amazonaws.com
                    docker build -t app .
                    docker tag app:latest 992382545251.dkr.ecr.us-east-1.amazonaws.com/roy-docker:app
                    docker push 992382545251.dkr.ecr.us-east-1.amazonaws.com/roy-docker:app
                    docker run --name app --rm app python3 -m unittest discover -s tests -v
                    
                """
            }
        }
        stage('cd') {
            when { 
                branch 'master'
            }
            steps {
                sh """
                    docker run --name app -d app
                    docker exec app bash -c "python3 api.py & sleep 2 && curl localhost:5000/health "
                """
            }
        }
    }
}
}
