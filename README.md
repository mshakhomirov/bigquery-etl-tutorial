# Use your Google account credential (replace the file):
~~~~bash
export GOOGLE_APPLICATION_CREDENTIALS="./client-50b5355d654a.json"
~~~~

# Create a dataset and a table:
~~~~bash
bq mk staging
~~~~

~~~~bash
bq mk staging.table_1 ./schemas/schema.json
~~~~

# Load data in:

bq load --source_format=NEWLINE_DELIMITED_JSON \
 staging.table_1 test_files/data_new_line_delimited_json.json
bq load --source_format=CSV -F ';' mydataset.mytable4 test_files/data_json_outer_array.json

bq query 'select first_name, last_name, dob from staging.table_1'

bq query 'select * from staging.table_7'

# create bucket
gcloud config set project your-project
gsutil mb -c regional -l europe-west2 gs://project_staging_files

# Copy files
gsutil cp ./test_files/* gs://project_staging_files/

gsutil cp ./test_files/data_new_line_delimited_json.json gs://project_staging_files/table-1_data_new_line_delimited_json.json
gsutil cp ./test_files/data_json_outer_array.json gs://project_staging_files/table-2_data_json_outer_array.json
gsutil cp ./test_files/data_json_outer_array.json gs://project_staging_files/table-3_data_new_line_delimited_src.json
gsutil cp ./test_files/data_one_line.json gs://project_staging_files/table-4_data_object_string.json
gsutil cp ./test_files/data_json_outer_array.json gs://project_staging_files/table-5_data_new_line_delimited_json.json
gsutil cp ./test_files/data_json_outer_array.json gs://project_staging_files/table-6_data_new_line_delimited_json.json
gsutil cp ./test_files/data_new_line_delimited_json.json gs://project_staging_files/table-7_data_new_line_delimited_json.json


gsutil cp ./test_files/data_error.json gs://project_staging_files/table-7_data_new_line_delimited_json.json

gsutil mb -c regional -l europe-west2 gs://project_staging_files_success
gsutil mb -c regional -l europe-west2 gs://project_staging_files_error
gsutil mb -c regional -l europe-west2 gs://your-project-functions-staging

# Create topics
STREAMING_ERROR_TOPIC=streaming_error_topic_staging
gcloud pubsub topics create ${STREAMING_ERROR_TOPIC}
STREAMING_SUCCESS_TOPIC=streaming_success_topic_staging
gcloud pubsub topics create ${STREAMING_SUCCESS_TOPIC}

# Check topics
$ gcloud pubsub topics list


# Create move functions:

# streaming_error_staging function to copy files in case there was an error during BigQuery import:
gcloud functions deploy streaming_error_staging --region=europe-west2 \
    --source=./functions/move_file \
    --entry-point=move_file --runtime=python37 \
    --stage-bucket=gs://your-project-functions-staging/ \
    --trigger-topic=streaming_error_topic_staging \
    --set-env-vars SOURCE_BUCKET=project_staging_files,DESTINATION_BUCKET=project_staging_files_error

# Check function status
gcloud functions describe streaming_error_staging --region=europe-west2 \
    --format="table[box](entryPoint, status, eventTrigger.eventType)"


# streaming_success_staging function to copy files
gcloud functions deploy streaming_success_staging --region=europe-west2 \
    --source=./functions/move_file \
    --entry-point=move_file --runtime=python37 \
    --stage-bucket=gs://your-project-functions-staging/ \
    --trigger-topic=streaming_success_topic_staging \
    --set-env-vars SOURCE_BUCKET=project_staging_files,DESTINATION_BUCKET=project_staging_files_success

gcloud functions describe streaming_success_staging --region=europe-west2 \
    --format="table[box](entryPoint, status, eventTrigger.eventType)"

# notes
