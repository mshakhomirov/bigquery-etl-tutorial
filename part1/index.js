'use strict';
const AWS = require('aws-sdk');
const async = require('async');
const { Storage } = require('@google-cloud/storage');
AWS.config.update({region: "eu-west-1"});
let s3 = new AWS.S3();
const moment = require('moment');

let config = require('./config.json');


exports.handler = async (event, context) => {

    let tables = config.Tables;

    for (const table of tables) {
        
        if (event.Records[0].s3.object.key.indexOf(table.name) > 0) {
            console.log('Loading into a table ',table.name);
            console.log('from: ',event.Records[0].s3.object.key);
            try {
                let data = await processEvent(event.Records);
                context.succeed(data);
            } catch (e) {
                console.log(e);
                context.done(e);
            }
        }
        
    };

};

let processEvent = async (Records) => {
    let srcBucket = Records[0].s3.bucket.name;
    let srcKey = decodeURIComponent(Records[0].s3.object.key.replace(/\+/g, " "));
    let data = await s3.getObject({Bucket: srcBucket, Key: srcKey}).promise();

    let googleCreds = await getGoogleCredentials();
    
    try {
        await copyToGoogleStorage(data.Body, srcKey, googleCreds);
    } catch (error) {
        console.log(error);
      };

    return 'Success'  ;

};

let getGoogleCredentials = async () => {
    let creds = await s3.getObject({Bucket: config.cred_bucket, Key: config.cred_s3_obj}).promise();
    var chunk = creds.Body.toString();
    let googleKey = JSON.parse(chunk).private_key.split("\\n");
    return googleKey;
}
;

let copyToGoogleStorage = async (data,destination, googleCreds) => {
    //Create GCS object
    const storage = new Storage({
        projectId: config.gcp_project_id, 
        credentials: { 
            client_email: config.gcp_client_email, 
            private_key: googleCreds[0]
        }
    });

    let Bucket = {};
    try {
        Bucket = await storage.bucket(config.googleBucket);
        const options = {
            metadata:{
              contentType: data.ContentType
            }
        };
        await Bucket.file(destination).save(data, options);
        console.log('File written successfully:', destination);
      } catch (error) {
        console.log(error);
      }
}
;