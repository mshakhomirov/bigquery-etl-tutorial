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

# create bucket
gcloud config set project your-project
gsutil mb -c regional -l europe-west2 gs://project_staging_files
gsutil cp ./test_files/* gs://project_staging_files/

gsutil cp ./test_files/data_new_line_delimited_json.json gs://project_staging_files/table-1_data_new_line_delimited_json.json
gsutil cp ./test_files/data_json_outer_array.json gs://project_staging_files/table-2_data_json_outer_array.json
gsutil cp ./test_files/data_json_outer_array.json gs://project_staging_files/table-3_data_new_line_delimited_src.json
gsutil cp ./test_files/data_one_line.json gs://project_staging_files/table-4_data_object_string.json
gsutil cp ./test_files/data_json_outer_array.json gs://project_staging_files/table-5_data_new_line_delimited_json.json
gsutil cp ./test_files/data_json_outer_array.json gs://project_staging_files/table-6_data_new_line_delimited_json.json



# notes

    # df['id'] = df.id.astype(str)
    # df['first_name'] = df.first_name.astype(str)
    # df['last_name'] = df.last_name.astype(str)
    # df['addresses'] = df.addresses.astype(str)