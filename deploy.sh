#!/usr/bin/env bash
set -x
export $(cat .env | xargs)
poetry export -f requirements.txt --output requirements.txt --without-hashes
gcloud functions deploy gcs-trigger-function \
  --gen2 \
  --memory=128MiB \
  --trigger-event-filters="type=google.cloud.storage.object.v1.finalized" \
  --trigger-event-filters="bucket=my-new-project-bucket-1234" \
  --entry-point=main \
  --runtime python39 \
  --region us-central1
