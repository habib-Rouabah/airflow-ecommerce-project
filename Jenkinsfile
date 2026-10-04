pipeline {
    agent any

    environment {
        VENV = "${WORKSPACE}/.venv"
        AIRFLOW_DAGS_DIR = "/opt/airflow/dags"
        MONGO_URI = "mongodb://mongodb:27017/"
        MONGO_DB = "ecommerce_analytics"
        MONGO_COLLECTION = "sales_metrics"
    }

    options {
        timestamps()
        disableConcurrentBuilds()
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Install dependencies') {
            steps {
                sh '''
                    python3 -m venv "$VENV"
                    . "$VENV/bin/activate"
                    pip install --upgrade pip
                    pip install -r requirements.txt
                '''
            }
        }

        stage('Run tests') {
            steps {
                sh '''
                    . "$VENV/bin/activate"
                    pytest -v --junitxml=test-results.xml
                '''
            }
            post {
                always {
                    junit allowEmptyResults: true, testResults: 'test-results.xml'
                }
            }
        }

        stage('Validate DAG') {
            steps {
                sh '''
                    . "$VENV/bin/activate"
                    python -m py_compile dags/*.py
                '''
            }
        }

        stage('Deploy DAG') {
            steps {
                sh '''
                    mkdir -p "$AIRFLOW_DAGS_DIR"
                    cp dags/*.py "$AIRFLOW_DAGS_DIR"/
                '''
            }
        }

        stage('Trigger DAG') {
            steps {
                sh 'airflow dags trigger ecommerce_sales_pipeline || true'
            }
        }

        stage('Verify MongoDB') {
            steps {
                sh '''
                    . "$VENV/bin/activate"
                    python scripts/check_mongodb.py
                '''
            }
        }
    }

    post {
        success { echo 'Pipeline terminé avec succès.' }
        failure { echo 'Pipeline en échec : consulter les logs des stages.' }
        always  { cleanWs() }
    }
}
