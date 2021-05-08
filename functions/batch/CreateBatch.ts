import { S3, SQS } from 'aws-sdk';
import { S3Handler } from 'aws-lambda';

const s3 = new S3();
const sqs = new SQS();
export const handler: S3Handler = async (event) => {

    const bucket = event.Records[0].s3.bucket.name;
    const key = event.Records[0].s3.object.key

    var csvRowsLength = await s3.getObject({ Bucket: bucket, Key: key })
        .promise()
        .then(response => response.Body.toString().trim())
        .then(csvString => csvString.split('\n'))
        .then(rows => rows.length);

    console.log(`No of rows: ${csvRowsLength}`);

    var messagePromises = [];
    const batchSize = 10000;
    // Starting from index 1 to ignore headers
    for (let index = 1; index < csvRowsLength; index = index + batchSize) {
        messagePromises.push(sqs.sendMessage({
            MessageBody: JSON.stringify({ start: index, end: index + batchSize, bucket, key }),
            QueueUrl: process.env.SQS_URL
        }).promise());
    }

    const resolved = await Promise.all(messagePromises);
    console.log(`Messages sent : ${resolved.length}`);
}
