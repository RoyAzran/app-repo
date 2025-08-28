pipeline {
    agent any
    stages {
        stage('ci') {
            when {
                anyOf {
                branch 'testing'
                expression { env.CHANGE_BRANCH == 'testing' }
                }
            }
            steps {
                echo 'deploying...'
                sh """
                    ssh -i key ec2-user@18.212.20.28
                    aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 992382545251.dkr.ecr.us-east-1.amazonaws.com
                    docker build -t app .
                    docker tag build-test:latest 992382545251.dkr.ecr.us-east-1.amazonaws.com/roy-docker:build-test
                    docker push 992382545251.dkr.ecr.us-east-1.amazonaws.com/roy-docker:build-test
                    docker run --rm app python3 -m unittest discover -s tests -v
                """
            }
        }
        stage('cd') {
            when { 
                anyOf {
                    branch 'main'
                    expression { env.CHANGE_BRANCH == 'main' }
            }
            }
            steps {
                sh """
            
                    ls
                    ssh -i key ec2-user@18.212.20.28
                    aws ecr get-login-password --region us-east-1 | docker login --username AWS --password-stdin 992382545251.dkr.ecr.us-east-1.amazonaws.com
                    docker build -t app .
                    docker tag app:production-ready 992382545251.dkr.ecr.us-east-1.amazonaws.com/roy-docker:app
                    docker push 992382545251.dkr.ecr.us-east-1.amazonaws.com/roy-docker:app
                    docker rm -f app || true
                    docker run --name app -d app
                    docker exec app bash -c "python3 api.py & sleep 2 && curl localhost:5000/health"
                """
           
            
        }
}
}
}
