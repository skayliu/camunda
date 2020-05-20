#!/usr/bin/env groovy

// general properties for CI execution
def static NODE_POOL() { return "agents-n1-standard-32-netssd-preempt" }
def static GCLOUD_DOCKER_IMAGE() { return "google/cloud-sdk:alpine" }

static String kubectlAgent(env) {
  return """
apiVersion: v1
kind: Pod
metadata:
  labels:
    agent: zeebe-tasklist-ci-build
spec:
  nodeSelector:
    cloud.google.com/gke-nodepool: ${NODE_POOL()}
  tolerations:
    - key: "${NODE_POOL()}"
      operator: "Exists"
      effect: "NoSchedule"
  serviceAccountName: ci-zeebe-tasklist-camunda-cloud
  containers:
  - name: gcloud
    image: ${GCLOUD_DOCKER_IMAGE()}
    imagePullPolicy: Always
    command: ["cat"]
    tty: true
    resources:
      limits:
        cpu: 500m
        memory: 500Mi
      requests:
        cpu: 500m
        memory: 500Mi
"""
}

pipeline {

  agent {
    kubernetes {
      cloud 'zeebe-tasklist-ci'
      label "zeebe-tasklist-ci-build_${env.JOB_BASE_NAME.replaceAll("%2F", "-").take(20)}-${env.BUILD_ID}"
      defaultContainer 'jnlp'
      yaml kubectlAgent(env)
    }
  }

  options {
    buildDiscarder(logRotator(numToKeepStr: '10'))
    timestamps()
    timeout(time: 15, unit: 'MINUTES')
  }

  stages {
    stage('Prepare') {
      steps {
        dir('gcloud-infrastructure') {
          git url: 'git@github.com:camunda-internal/gcloud-infrastructure',
            branch: "${params.INFRASTRUCTURE_BRANCH}",
            credentialsId: 'camunda-jenkins-github-ssh',
            poll: false
        }
        dir('zeebe-tasklist') {
          git url: 'git@github.com:zeebe-io/zeebe-tasklist',
            branch: "${params.ZEEBE_TASKLIST_BRANCH}",
            credentialsId: 'camunda-jenkins-github-ssh',
            poll: false
        }

        container('gcloud') {
          sh("""
            gcloud components install kubectl --quiet
          """)
        }
      }
    }
    stage('Deploy to K8s') {
      steps {
        container('gcloud') {
          dir('gcloud-infrastructure') {
            sh("""
              ./cmd/k8s/deploy-template-to-branch \
              ${WORKSPACE}/gcloud-infrastructure/camunda-ci/deployments/zeebe-tasklist-branch \
              ${WORKSPACE}/zeebe-tasklist/.ci/branch-deployment \
              ${params.BRANCH} \
              zeebe-tasklist
            """)
          }
        }
      }
      post {
        always {
          archiveArtifacts artifacts: 'gcloud-infrastructure/rendered-templates/**/*'
        }
      }
    }
  }

  post {
    failure {
      script {
        def notification = load "${pwd()}/.ci/pipelines/build_notification.groovy"
        notification.buildNotification(currentBuild.result)
      }
    }
  }
}
