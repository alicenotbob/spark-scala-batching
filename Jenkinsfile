pipeline {
    agent any

    stages {
        stage('init') {
            steps {
                script {
                    def sbtHome = tool 'sbt-1.3.4'
                    env.sbt= "${sbtHome}/bin/sbt"
                }
            }
        }
        stage('Build') {
            steps {
                echo "Compiling..."
                sh "${sbt} compile"
            }
        }
        stage('Test') {
            steps {
                echo "Running tests..."
                sh "${sbt} test"
            }
        }
        stage('Package') {
            steps {
                echo "Packaging..."
                sh "${sbt} package"
            }
        }
    }
}