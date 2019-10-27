#!/bin/sh
# chmod +x the_file_name

ENVIRONMENT=$1
FUNCTION="streaming_"${ENVIRONMENT}
PROJECT="your-project"
BUCKET="project_staging_files"

# set the gcloud project
gcloud config set project ${PROJECT}

gcloud functions deploy ${FUNCTION} \
    --region=europe-west2 \
    --stage-bucket=gs://your-project-functions-staging/ \
    --runtime python37 \
    --trigger-resource ${BUCKET} \
    --trigger-event google.storage.object.finalize \
    --update-labels=service=functions-bigquery-${ENVIRONMENT} \
    --set-env-vars ENV=${ENVIRONMENT}