#!/usr/bin/env bash
# chmod +x the_file_name

base=${PWD##*/}
zp=$base".zip"
echo $zp

rm -f $zp

zip -r $zp * -x deploy.sh
if ! aws lambda create-function  \
    --function-name gcs-transfer \
    --description "Sync files from Datalake to Google Cloud Storage" \
    --handler index.handler \
    --runtime nodejs8.10 \
    --role arn:aws:iam::1234567890:role/your_role_dataload_lambda \
    --zip-file fileb://$zp;

    then

    echo ""
    echo "Function already exists, updating instead..."

    aws lambda update-function-code  \
    --function-name gcs-transfer \
    --zip-file fileb://$zp;
fi